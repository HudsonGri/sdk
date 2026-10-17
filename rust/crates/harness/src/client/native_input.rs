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

/// Unix stdin without a blocking-thread hop: pipes, sockets and terminals
/// wait on the reactor; descriptors that are always ready (regular files,
/// block devices, /dev/null) are read directly.
#[cfg(unix)]
mod stdin {
    use std::fs::File;
    use std::io::{self, IsTerminal, Read};
    use std::os::fd::AsFd;
    use std::os::unix::fs::FileTypeExt;
    use std::pin::Pin;
    use std::task::{Context, Poll, ready};

    use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
    use tokio::io::unix::AsyncFd;
    use tokio::io::{AsyncRead, ReadBuf};

    pub(super) enum Stdin {
        Ready(File),
        /// Original flags of the shared open file description, restored on drop.
        Polled(AsyncFd<File>, OFlags),
    }

    pub(super) fn open() -> io::Result<Stdin> {
        let file = File::from(io::stdin().as_fd().try_clone_to_owned()?);
        let kind = file.metadata()?.file_type();
        if kind.is_file()
            || kind.is_block_device()
            || (kind.is_char_device() && !file.is_terminal())
        {
            return Ok(Stdin::Ready(file));
        }
        let flags = fcntl_getfl(&file)?;
        fcntl_setfl(&file, flags | OFlags::NONBLOCK)?;
        match AsyncFd::try_new(file) {
            Ok(fd) => Ok(Stdin::Polled(fd, flags)),
            Err(error) => {
                let (file, error) = error.into_parts();
                let _ = fcntl_setfl(&file, flags);
                Err(error)
            }
        }
    }

    impl Drop for Stdin {
        fn drop(&mut self) {
            if let Self::Polled(fd, flags) = self {
                let _ = fcntl_setfl(fd.get_ref(), *flags);
            }
        }
    }

    impl AsyncRead for Stdin {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let fd = match self.get_mut() {
                Self::Ready(file) => {
                    let count = file.read(buf.initialize_unfilled())?;
                    buf.advance(count);
                    return Poll::Ready(Ok(()));
                }
                Self::Polled(fd, _) => fd,
            };
            loop {
                let mut ready = ready!(fd.poll_read_ready(cx))?;
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
