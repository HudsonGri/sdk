//! Native async terminal input. Each platform has exactly one byte source;
//! every platform shares this decoder loop, so there is no blocking read path.
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_core::Stream;
use tokio::time::{Instant, Sleep, sleep_until};

use super::Decoder;
use super::decoder::ESCAPE_TIMEOUT;
use crate::key_event::InputEvent;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix::Source;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::Source;

/// Classified terminal input. Dropping the stream releases the terminal
/// immediately: no thread is left waiting for another key.
pub struct Events {
    source: Source,
    decoder: Decoder,
    ready: VecDeque<InputEvent>,
    escape: Option<Pin<Box<Sleep>>>,
    escape_armed: bool,
    ended: bool,
    /// Keys typed during negotiation are already classified; they come first.
    negotiated: bool,
}

impl Events {
    fn new(source: Source, decoder: Decoder) -> Self {
        Self {
            source,
            decoder,
            ready: VecDeque::new(),
            negotiated: true,
            escape: None,
            escape_armed: false,
            ended: false,
        }
    }

    /// Wait out a pending lone Escape. The timer is allocated once per stream
    /// and re-armed only by a quiet source, matching the decoder's framing.
    fn poll_escape(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let deadline = Instant::now() + ESCAPE_TIMEOUT;
        let timer = self
            .escape
            .get_or_insert_with(|| Box::pin(sleep_until(deadline)));
        if !self.escape_armed {
            timer.as_mut().reset(deadline);
            self.escape_armed = true;
        }
        ready!(timer.as_mut().poll(cx));
        self.escape_armed = false;
        Poll::Ready(())
    }
}

impl Stream for Events {
    type Item = io::Result<InputEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(event) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(event)));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            let mut bytes = [0; 1024];
            let decoded = if std::mem::take(&mut this.negotiated) {
                Ok(())
            } else {
                match this.source.poll_read(cx, &mut bytes) {
                    Poll::Ready(Ok(0)) => {
                        this.ended = true;
                        this.decoder.finish_input()
                    }
                    Poll::Ready(Ok(count)) => {
                        this.escape_armed = false;
                        this.decoder
                            .feed(bytes.get(..count).ok_or(io::ErrorKind::InvalidData)?)
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                    Poll::Pending if this.decoder.escape_pending() => {
                        ready!(this.poll_escape(cx));
                        this.decoder.finish_input()
                    }
                    Poll::Pending => {
                        this.escape_armed = false;
                        return Poll::Pending;
                    }
                }
            };
            // A rejected unit is reported first; the rest of its chunk is
            // already classified and follows in order.
            let mut intercepted = Ok(());
            for event in this.decoder.drain_events() {
                match this.source.intercept(&event) {
                    Ok(false) => this.ready.push_back(event),
                    Ok(true) => {}
                    Err(error) => intercepted = intercepted.and(Err(error)),
                }
            }
            if let Err(error) = decoded.and(intercepted) {
                return Poll::Ready(Some(Err(error)));
            }
        }
    }
}
