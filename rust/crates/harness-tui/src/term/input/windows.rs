//! Windows input: console input cannot complete through IOCP, so each stream
//! owns one persistent waiter. It waits on the console input handle and the
//! console's close wake together and feeds a bounded channel.
use std::io;
use std::os::windows::io::AsRawHandle;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::thread::{self, JoinHandle};

use acyclic_native_runtime::terminal as ffi;
use tokio::sync::mpsc;

use super::super::Tty;
use super::Events;
use crate::key_event::InputEvent;

type Batch = io::Result<Vec<u8>>;

/// The persistent waiter and its bounded channel. A batch is at most 1024 bytes.
struct Waiter {
    batches: Option<mpsc::Receiver<Batch>>,
    thread: Option<JoinHandle<()>>,
}

impl Waiter {
    /// `read` must return once the wake event it waits on is signalled; a
    /// failed read ends the waiter after delivering its error.
    fn spawn(mut read: impl FnMut() -> Batch + Send + 'static) -> io::Result<Self> {
        let (sender, batches) = mpsc::channel(2);
        let thread = thread::Builder::new()
            .name("terminal-input".into())
            .spawn(move || {
                loop {
                    let batch = read();
                    let failed = batch.is_err();
                    // A dropped receiver fails the send immediately.
                    if sender.blocking_send(batch).is_err() || failed {
                        return;
                    }
                }
            })?;
        Ok(Self {
            batches: Some(batches),
            thread: Some(thread),
        })
    }

    fn poll_read(&mut self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>> {
        let Some(batches) = &mut self.batches else {
            return Poll::Ready(Ok(0));
        };
        match ready!(batches.poll_recv(cx)) {
            Some(Ok(batch)) => {
                let target = bytes
                    .get_mut(..batch.len())
                    .ok_or(io::ErrorKind::InvalidData)?;
                target.copy_from_slice(&batch);
                Poll::Ready(Ok(batch.len()))
            }
            Some(Err(error)) => Poll::Ready(Err(error)),
            None => Poll::Ready(Ok(0)),
        }
    }

    /// Join after the wake is signalled: the waiter leaves its wait, and a
    /// pending send fails because the receiver is dropped first.
    fn join(&mut self) {
        drop(self.batches.take());
        if let Some(thread) = self.thread.take() {
            // A key-down record makes ReadFile return promptly; cancellation
            // covers that short call too.
            ffi::cancel_thread(&thread);
            let _ = thread.join();
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        self.join();
    }
}

pub(super) struct Source {
    waiter: Waiter,
    tty: Tty,
}

impl Tty {
    /// Follow input through the decoder retained by negotiation. Must be
    /// called within a Tokio runtime with time enabled.
    pub fn events(mut self) -> io::Result<Events> {
        let reader = self.reader.try_clone()?;
        let console = Arc::clone(&self.console);
        let waiter = Waiter::spawn(move || {
            console.check_open()?;
            ffi::console_read(
                reader.as_raw_handle() as usize,
                1024,
                None,
                Some(&console.closed),
            )
        })?;
        let decoder = std::mem::take(&mut self.decoder);
        Ok(Events::new(Source { waiter, tty: self }, decoder))
    }
}

impl Source {
    pub(super) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.waiter.poll_read(cx, bytes)
    }

    pub(super) fn intercept(&mut self, _event: &InputEvent) -> io::Result<bool> {
        Ok(false)
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        // Closing signals the wake the waiter includes; then it can be joined.
        let _ = self.tty.restore();
        self.waiter.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::pin;
    use std::task::Waker;
    use std::time::{Duration, Instant};

    fn pending(future: impl Future) {
        let mut cx = Context::from_waker(Waker::noop());
        assert!(pin!(future).poll(&mut cx).is_pending());
    }

    #[test]
    fn dropping_a_waiting_stream_leaves_no_thread_blocked() -> io::Result<()> {
        let input = Arc::new(ffi::ConsoleWake::new()?);
        let close = Arc::new(ffi::ConsoleWake::new()?);
        let (handle, wake) = (Arc::clone(&input), Arc::clone(&close));
        let mut waiter = Waiter::spawn(move || {
            ffi::console_read(handle.as_raw_handle() as usize, 1024, None, Some(&wake))
        })?;
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            pending(std::future::poll_fn(|cx| {
                waiter.poll_read(cx, &mut [0; 1024])
            }));
        });
        close.signal()?;
        drop(waiter);
        // The close handshake: the waiter released its clones before join returned.
        assert_eq!(Arc::strong_count(&close), 1);
        assert_eq!(Arc::strong_count(&input), 1);
        Ok(())
    }

    #[test]
    fn negative_control_detects_spawn_blocking_reader_left_blocked() -> io::Result<()> {
        let input = Arc::new(ffi::ConsoleWake::new()?);
        let close = Arc::new(ffi::ConsoleWake::new()?);
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let handle = Arc::clone(&input);
        // The removed path: one blocking task per batch with a 24-hour deadline.
        let read = runtime.spawn_blocking(move || {
            ffi::console_read(
                handle.as_raw_handle() as usize,
                1024,
                Some(Instant::now() + Duration::from_secs(86_400)),
                None,
            )
        });
        runtime.block_on(async { pending(read) });
        close.signal()?;
        assert_eq!(
            Arc::strong_count(&input),
            2,
            "blocked reader still holds input"
        );
        input.signal()?; // Release the blocked thread so the runtime can stop.
        runtime.shutdown_timeout(Duration::from_secs(5));
        assert_eq!(Arc::strong_count(&input), 1);
        Ok(())
    }

    struct Wakes(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn idle_waiter_has_zero_wakeups_and_reports_handoff_latency() -> io::Result<()> {
        let (keys, typed) = std::sync::mpsc::channel::<Vec<u8>>();
        let typed = std::sync::Mutex::new(typed);
        let mut waiter = Waiter::spawn(move || {
            let typed = typed.lock().map_err(|_| io::ErrorKind::Other)?;
            typed.recv().map_err(|_| io::ErrorKind::NotConnected.into())
        })?;
        // Rebound after the waiter: on unwind the sender drops first, so the
        // waiter's read ends and its join cannot hang.
        let keys = keys;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let wakes = Arc::new(Wakes(std::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&waker);
        assert!(waiter.poll_read(&mut cx, &mut [0; 1024]).is_pending());
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(200)).await });
        assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        // Measurement only: waiter wake to task readiness (the console wait
        // itself is the kernel's; this is the handoff this module adds).
        let mut samples = Vec::new();
        for _ in 0..200 {
            let start = Instant::now();
            keys.send(b"x".to_vec())
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            let count = runtime.block_on(std::future::poll_fn(|cx| {
                waiter.poll_read(cx, &mut [0; 1024])
            }))?;
            assert_eq!(count, 1);
            samples.push(start.elapsed());
        }
        samples.sort();
        eprintln!(
            "windows input handoff p50 {:?} p99 {:?} max {:?}; idle 200 ms: 0 wakeups",
            samples[100], samples[198], samples[199]
        );
        Ok(())
    }
}
