//! Native input adapters; wire decoding and settlement stay in the shared runner.
use crate::{Error, Result};
use futures::{StreamExt, stream::BoxStream};

/// Bounded JSONL stdin, independently scheduled from replay/follow. Must be
/// called within a Tokio runtime with I/O enabled.
pub fn stdin_jsonl() -> Result<BoxStream<'static, Result<String>>> {
    #[cfg(unix)]
    let input = stdin::open().map_err(|error| Error::Storage(error.to_string()))?;
    #[cfg(not(unix))]
    let input = tokio::io::stdin();
    Ok(Box::pin(
        tokio_util::codec::FramedRead::new(
            input,
            tokio_util::codec::LinesCodec::new_with_max_length(65_536),
        )
        .map(|line| line.map_err(|error| Error::Invalid(error.to_string()))),
    ))
}

/// Unix stdin without a blocking-pool hop. Pipes, sockets and terminals wait
/// on the reactor. On Linux they are reopened as a private open file
/// description that can be nonblocking without affecting any other process,
/// even if this one is killed. Elsewhere the shared description's flags are
/// never changed: readiness is confirmed before each read, so a blocking read
/// still returns at once. Regular files and block devices cannot signal
/// readiness, so a dedicated reader keeps their reads off the reactor.
#[cfg(unix)]
mod stdin {
    use std::fs::File;
    use std::io::{self, Read};
    use std::os::fd::AsFd;
    use std::os::unix::fs::FileTypeExt;
    use std::pin::Pin;
    use std::task::{Context, Poll, ready};

    use rustix::event::{PollFd, PollFlags, Timespec, poll};
    use tokio::io::unix::AsyncFd;
    use tokio::io::{AsyncRead, ReadBuf};

    use super::reader::Reader;

    pub(super) enum Stdin {
        /// A private description opened nonblocking.
        Private(AsyncFd<File>),
        /// The shared description, flags untouched.
        Shared(AsyncFd<File>),
        Reader(Reader),
    }

    pub(super) fn open() -> io::Result<Stdin> {
        from_file(File::from(io::stdin().as_fd().try_clone_to_owned()?))
    }

    pub(super) fn from_file(file: File) -> io::Result<Stdin> {
        let kind = file.metadata()?.file_type();
        if kind.is_file() || kind.is_block_device() {
            return Reader::spawn(file).map(Stdin::Reader);
        }
        #[cfg(target_os = "linux")]
        if let Ok(private) = rustix::fs::open(
            format!("/proc/self/fd/{}", std::os::fd::AsRawFd::as_raw_fd(&file)),
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        ) && let Ok(fd) = AsyncFd::try_new(File::from(private))
        {
            return Ok(Stdin::Private(fd));
        }
        match AsyncFd::try_new(file) {
            Ok(fd) => Ok(Stdin::Shared(fd)),
            // Not pollable (a device the reactor rejects): read it off-reactor.
            Err(error) => Reader::spawn(error.into_parts().0).map(Stdin::Reader),
        }
    }

    /// Whether a read of the shared, blocking description returns at once:
    /// data is queued, or end of input or an error is pending.
    fn readable(file: &File) -> io::Result<bool> {
        let mut fds = [PollFd::new(file, PollFlags::IN)];
        poll(
            &mut fds,
            Some(&Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            }),
        )?;
        Ok(!fds[0].revents().is_empty())
    }

    impl AsyncRead for Stdin {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let (fd, shared) = match self.get_mut() {
                Self::Reader(reader) => return reader.poll_read(cx, buf),
                Self::Private(fd) => (fd, false),
                Self::Shared(fd) => (fd, true),
            };
            loop {
                let mut ready = ready!(fd.poll_read_ready(cx))?;
                if shared && !readable(fd.get_ref())? {
                    // Stale edge: the data it announced was already read.
                    ready.clear_ready();
                    continue;
                }
                match ready.try_io(|fd| {
                    let mut file = fd.get_ref();
                    file.read(buf.initialize_unfilled())
                }) {
                    Ok(Ok(count)) => {
                        buf.advance(count);
                        return Poll::Ready(Ok(()));
                    }
                    Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                    Ok(Err(error)) => return Poll::Ready(Err(error)),
                    Err(_would_block) => {}
                }
            }
        }
    }
}

/// A dedicated reader for input the reactor cannot wait on. One persistent
/// thread fills recycled batches and feeds a bounded channel, so a slow or
/// stalled read never blocks the runtime.
#[cfg(unix)]
mod reader {
    use std::fs::File;
    use std::io::{self, Read};
    use std::sync::mpsc;
    use std::task::{Context, Poll, ready};

    use tokio::io::ReadBuf;

    const BATCH: usize = 64 * 1024;

    pub(super) struct Reader {
        batches: tokio::sync::mpsc::Receiver<io::Result<Vec<u8>>>,
        spare: mpsc::Sender<Vec<u8>>,
        current: Vec<u8>,
        offset: usize,
    }

    impl Reader {
        pub(super) fn spawn(mut file: File) -> io::Result<Self> {
            let (send, batches) = tokio::sync::mpsc::channel(2);
            let (spare, recycled) = mpsc::channel::<Vec<u8>>();
            // Detached: a dropped stream fails the next send, and file reads
            // always complete, so the thread ends after at most one more read.
            std::thread::Builder::new()
                .name("harness-stdin".into())
                .spawn(move || {
                    loop {
                        let mut batch = recycled.try_recv().unwrap_or_default();
                        batch.resize(BATCH, 0);
                        let result = file.read(&mut batch).map(|count| {
                            batch.truncate(count);
                            batch
                        });
                        let last = !matches!(&result, Ok(batch) if !batch.is_empty());
                        if send.blocking_send(result).is_err() || last {
                            return;
                        }
                    }
                })?;
            Ok(Self {
                batches,
                spare,
                current: Vec::new(),
                offset: 0,
            })
        }

        pub(super) fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.offset == self.current.len() {
                let batch = match ready!(self.batches.poll_recv(cx)) {
                    Some(batch) => batch?,
                    None => return Poll::Ready(Ok(())),
                };
                let spent = std::mem::replace(&mut self.current, batch);
                self.offset = 0;
                if spent.capacity() != 0 {
                    let _ = self.spare.send(spent);
                }
            }
            let count = buf.remaining().min(self.current.len() - self.offset);
            buf.put_slice(&self.current[self.offset..self.offset + count]);
            self.offset += count;
            Poll::Ready(Ok(()))
        }
    }
}

/// Waits for UI input or a validated delivery. Publication/commit is completed
/// outside select, so an input event cannot cancel a half-published delivery.
#[cfg(feature = "tui")]
pub async fn next_tui_event<S: super::ClientStateStore>(
    client: &mut super::HarnessClient<S>,
    input: &mut (impl futures::Stream<Item = Result<crate::tui::key_event::InputEvent>> + Unpin),
    completions: &mut crate::tui_files::CompletionFiles,
    prompt: &mut crate::tui::PromptInput,
    commands: &mut super::CommandSession<acyclic_stream::LocalStream>,
    documents: &mut acyclic_stream::BoxProviderStream<'static, Result<super::ViewUpdate>>,
) -> Result<Option<crate::tui::key_event::InputEvent>> {
    let delivery = tokio::select! {
        event = input.next() => return event.transpose()?.map(Some).ok_or_else(|| Error::Storage("terminal input closed".into())),
        delivery = client.read_delivery() => delivery?,
        update = documents.next() => {
            client.apply_update(update.ok_or_else(|| Error::Conflict("document follow ended; reopen snapshots".into()))??)?;
            return Ok(None);
        },
        change = commands.observe_change() => { change?; return Ok(None); },
        change = completions.read_change() => {
            completions.apply_change(change?, prompt)?;
            return Ok(None);
        },
    };
    let delivery = delivery.ok_or_else(|| {
        Error::Conflict("live replay ended; reconnect from the committed cursor".into())
    })?;
    client.publish_delivery(delivery, |_| Ok(())).await?;
    Ok(None)
}

/// Multiplex terminal input with authorized file publications.
#[cfg(all(feature = "tui", feature = "filesystem-local"))]
pub async fn next_file_tui_event(
    client: &mut super::TaskFileClient,
    input: &mut (impl futures::Stream<Item = Result<crate::tui::key_event::InputEvent>> + Unpin),
    completions: &mut crate::tui_files::CompletionFiles,
    prompt: &mut crate::tui::PromptInput,
    commands: &mut super::CommandSession<acyclic_stream::LocalStream>,
    documents: &mut acyclic_stream::BoxProviderStream<'static, Result<super::ViewUpdate>>,
) -> Result<Option<crate::tui::key_event::InputEvent>> {
    tokio::select! {
        event = input.next() => event.transpose()?.map(Some).ok_or_else(|| Error::Storage("terminal input closed".into())),
        change = client.next_change() => { change?; Ok(None) },
        update = documents.next() => {
            client.apply_update(update.ok_or_else(|| Error::Conflict("document follow ended; reopen snapshots".into()))??)?;
            Ok(None)
        },
        change = commands.observe_change() => { change?; Ok(None) },
        change = completions.read_change() => {
            completions.apply_change(change?, prompt)?;
            Ok(None)
        },
    }
}

/// Follow controlling-terminal input through the decoder retained by
/// negotiation. Input waits on the reactor (a console waiter on Windows);
/// dropping the stream releases the terminal immediately.
#[cfg(feature = "tui")]
pub fn terminal_events(
    tty: crate::tui::term::Tty,
) -> Result<acyclic_stream::BoxProviderStream<'static, Result<crate::tui::key_event::InputEvent>>> {
    let events = tty
        .events()
        .map_err(|error| Error::Storage(error.to_string()))?;
    Ok(Box::pin(events.map(|event| {
        event.map_err(|error| Error::Storage(error.to_string()))
    })))
}

#[cfg(all(test, unix))]
mod tests {
    use super::stdin::{Stdin, from_file};
    use futures::StreamExt;
    use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
    use std::fs::File;
    use std::io::{self, Write};
    use std::time::Duration;
    use tokio_util::codec::{FramedRead, LinesCodec};

    fn runtime() -> io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    }

    fn pipe() -> io::Result<(File, File)> {
        let (read, write) = io::pipe()?;
        Ok((
            File::from(std::os::fd::OwnedFd::from(read)),
            File::from(std::os::fd::OwnedFd::from(write)),
        ))
    }

    #[test]
    fn pipe_input_never_changes_the_shared_description() -> io::Result<()> {
        let (read, mut write) = pipe()?;
        let observer = read.try_clone()?;
        let original = fcntl_getfl(&observer)?;
        assert!(!original.contains(OFlags::NONBLOCK));
        let runtime = runtime()?;
        let lines = runtime.block_on(async {
            let input = from_file(read)?;
            assert_eq!(
                matches!(input, Stdin::Private(_)),
                cfg!(target_os = "linux")
            );
            assert!(!matches!(input, Stdin::Reader(_)));
            // Nothing a killed process could leave behind: the description
            // shared with the parent keeps its flags while input is followed.
            assert_eq!(fcntl_getfl(&observer)?, original);
            write.write_all(b"one\ntwo\n")?;
            drop(write);
            let lines: Vec<_> = FramedRead::new(input, LinesCodec::new()).collect().await;
            io::Result::Ok(lines)
        })?;
        let lines: Vec<String> = lines
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(io::Error::other)?;
        assert_eq!(lines, ["one", "two"]);
        assert_eq!(fcntl_getfl(&observer)?, original);
        Ok(())
    }

    #[test]
    fn negative_control_detects_flags_set_through_a_duplicate() -> io::Result<()> {
        let (read, _write) = pipe()?;
        let original = fcntl_getfl(&read)?;
        let duplicate = read.try_clone()?;
        fcntl_setfl(&duplicate, original | OFlags::NONBLOCK)?;
        assert_ne!(
            fcntl_getfl(&read)?,
            original,
            "a duplicate shares the description"
        );
        Ok(())
    }

    #[test]
    fn regular_files_are_read_off_the_reactor() -> io::Result<()> {
        let mut file = tempfile::tempfile()?;
        file.write_all(b"alpha\nbeta\n")?;
        std::io::Seek::rewind(&mut file)?;
        let runtime = runtime()?;
        let lines = runtime.block_on(async {
            let input = from_file(file)?;
            assert!(matches!(input, Stdin::Reader(_)));
            let lines: Vec<_> = FramedRead::new(input, LinesCodec::new()).collect().await;
            io::Result::Ok(lines)
        })?;
        let lines: Vec<String> = lines
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(io::Error::other)?;
        assert_eq!(lines, ["alpha", "beta"]);
        Ok(())
    }

    #[test]
    fn a_stalled_dedicated_read_leaves_the_runtime_running() -> io::Result<()> {
        // A read that cannot complete stands in for a stalled filesystem.
        let (read, mut write) = pipe()?;
        let runtime = runtime()?;
        runtime.block_on(async {
            let reader = super::reader::Reader::spawn(read)?;
            let mut lines = FramedRead::new(Stdin::Reader(reader), LinesCodec::new());
            assert!(
                tokio::time::timeout(Duration::from_millis(5), lines.next())
                    .await
                    .is_err()
            );
            write.write_all(b"late\n")?;
            let line = lines.next().await.ok_or(io::ErrorKind::UnexpectedEof)?;
            assert_eq!(line.map_err(io::Error::other)?, "late");
            io::Result::Ok(())
        })
    }

    #[test]
    fn negative_control_detects_a_read_that_blocks_its_thread() -> io::Result<()> {
        // The removed path read regular files inline; on a stalled source that
        // read holds its thread, which the timer here observes.
        let (mut read, mut write) = pipe()?;
        let blocked = std::thread::spawn(move || io::Read::read(&mut read, &mut [0; 8]));
        let runtime = runtime()?;
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(5)).await });
        assert!(!blocked.is_finished());
        write.write_all(b"x")?;
        assert_eq!(blocked.join().map_err(|_| io::ErrorKind::Other)??, 1);
        Ok(())
    }
}
