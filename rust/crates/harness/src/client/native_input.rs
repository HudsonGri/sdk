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

/// Unix stdin. Only a description this process owns and opened nonblocking
/// waits on the reactor: no other reader can turn a readiness edge into a
/// blocking read, and its flags affect no other process even if this one is
/// killed. That description exists only where stdin can be reopened (Linux's
/// /proc/self/fd). Every other description is shared or blocking, so a
/// dedicated reader thread reads it off the reactor, flags untouched.
#[cfg(unix)]
mod stdin {
    use std::fs::File;
    use std::io::{self, Read};
    use std::os::fd::{AsFd, AsRawFd, RawFd};
    use std::pin::Pin;
    use std::task::{Context, Poll, ready};

    use tokio::io::unix::AsyncFd;
    use tokio::io::{AsyncRead, ReadBuf};

    use super::reader::Reader;

    /// A nonblocking open file description held by this process alone. The
    /// only constructor reopens the input, so the reactor path cannot be
    /// handed a shared or blocking description.
    pub(super) struct Private(File);

    impl Private {
        #[cfg(target_os = "linux")]
        fn reopen(file: &File) -> Option<Self> {
            use rustix::fs::{Mode, OFlags};
            let kind = file.metadata().ok()?.file_type();
            // Regular files and block devices are always "ready" yet may
            // block on storage: they belong to the reader as well.
            if kind.is_file() || std::os::unix::fs::FileTypeExt::is_block_device(&kind) {
                return None;
            }
            rustix::fs::open(
                format!("/proc/self/fd/{}", file.as_raw_fd()),
                OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .ok()
            .map(|fd| Self(File::from(fd)))
        }

        #[cfg(not(target_os = "linux"))]
        fn reopen(_: &File) -> Option<Self> {
            None
        }
    }

    impl AsRawFd for Private {
        fn as_raw_fd(&self) -> RawFd {
            self.0.as_raw_fd()
        }
    }

    pub(super) enum Stdin {
        Reactor(AsyncFd<Private>),
        Reader(Reader),
    }

    pub(super) fn open() -> io::Result<Stdin> {
        from_file(File::from(io::stdin().as_fd().try_clone_to_owned()?))
    }

    pub(super) fn from_file(file: File) -> io::Result<Stdin> {
        if let Some(private) = Private::reopen(&file)
            && let Ok(fd) = AsyncFd::try_new(private)
        {
            return Ok(Stdin::Reactor(fd));
        }
        Reader::spawn(file).map(Stdin::Reader)
    }

    impl AsyncRead for Stdin {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let fd = match self.get_mut() {
                Self::Reader(reader) => return reader.poll_read(cx, buf),
                Self::Reactor(fd) => fd,
            };
            loop {
                let mut ready = ready!(fd.poll_read_ready(cx))?;
                // Nonblocking: bytes taken by another reader since the edge
                // yield WouldBlock, which clears readiness and waits again.
                match ready.try_io(|fd| (&fd.get_ref().0).read(buf.initialize_unfilled())) {
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
            // Detached: a dropped stream fails the next send, so the thread
            // ends after its current read completes (at the next input or end
            // of input for a pipe or terminal).
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
                matches!(input, Stdin::Reactor(_)),
                cfg!(target_os = "linux")
            );
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

    /// Another reader takes the announced byte between the readiness edge and
    /// this stream's read. The reactor path must keep waiting without
    /// blocking the runtime. (The thread path cannot be raced into blocking
    /// the runtime: only its own thread ever waits in a read.)
    #[cfg(target_os = "linux")]
    #[test]
    fn input_stolen_after_readiness_leaves_the_runtime_running() -> io::Result<()> {
        let (read, mut write) = pipe()?;
        let mut thief = read.try_clone()?;
        let runtime = runtime()?;
        runtime.block_on(async {
            let mut input = from_file(read)?;
            let Stdin::Reactor(fd) = &input else {
                return Err(io::ErrorKind::Unsupported.into());
            };
            write.write_all(b"x")?;
            // Hold the edge, then let the thief consume its byte.
            fd.readable().await?.retain_ready();
            assert_eq!(io::Read::read(&mut thief, &mut [0; 1])?, 1);
            let stalled = tokio::time::timeout(
                Duration::from_millis(5),
                tokio::io::AsyncReadExt::read(&mut input, &mut [0; 1]),
            )
            .await;
            assert!(stalled.is_err(), "the stolen byte was read");
            write.write_all(b"y\n")?;
            let mut lines = FramedRead::new(input, LinesCodec::new());
            let line = lines.next().await.ok_or(io::ErrorKind::UnexpectedEof)?;
            assert_eq!(line.map_err(io::Error::other)?, "y");
            io::Result::Ok(())
        })
    }

    #[test]
    fn negative_control_detects_a_blocking_read_after_stolen_readiness() -> io::Result<()> {
        // The removed shared path: readiness seen, byte stolen, then a read of
        // the blocking description holds its thread.
        let (read, mut write) = pipe()?;
        let mut thief = read.try_clone()?;
        write.write_all(b"x")?;
        let mut fds = [rustix::event::PollFd::new(
            &read,
            rustix::event::PollFlags::IN,
        )];
        rustix::event::poll(&mut fds, None)?;
        assert_eq!(io::Read::read(&mut thief, &mut [0; 1])?, 1);
        let mut shared = read;
        let blocked = std::thread::spawn(move || io::Read::read(&mut shared, &mut [0; 1]));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!blocked.is_finished(), "the stale read returned");
        write.write_all(b"y")?;
        assert_eq!(blocked.join().map_err(|_| io::ErrorKind::Other)??, 1);
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
