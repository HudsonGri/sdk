//! A dedicated reader for input the runtime cannot wait on. One persistent
//! thread fills recycled batches and hands them over a bounded channel, so a
//! slow or stalled read never blocks the runtime.
//!
//! Shutdown never depends on a lock someone else holds. Dropping the stream
//! closes the channel, which releases a thread waiting for queue space, and
//! signals a level-triggered cancel object the thread's every wait includes:
//! a pipe hung up on Unix, an event on Windows. A read admitted before the
//! cancel is released by `CancelSynchronousIo` on Windows; on Unix a read
//! starts only once poll reports input, and the thread is detached, holding
//! nothing that could delay process exit.
use std::collections::VecDeque;
use std::io;
use std::task::{Context, Poll, Waker, ready};

use tokio::io::ReadBuf;

use crate::sync::{Arc, SyncCondvar, SyncGuard, SyncMutex};

const BATCH: usize = 64 * 1024;
/// Filled batches waiting for the stream.
const QUEUED: usize = 2;

type Batch = io::Result<Vec<u8>>;

/// Input the dedicated reader waits on.
pub(super) trait Source: Send + 'static {
    /// Fill `bytes`; `None` once the stream has cancelled the wait.
    fn read_input(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>>;
}

/// The bounded handoff from the reader thread to the stream.
pub(super) struct Channel {
    state: SyncMutex<State>,
    /// Signalled when a batch is taken or the channel closes.
    space: SyncCondvar,
    batch: usize,
}

struct State {
    ready: VecDeque<Batch>,
    spare: Vec<Vec<u8>>,
    waker: Option<Waker>,
    /// The stream is gone; the reader stops at its next channel call.
    closed: bool,
    /// The reader is gone; the stream ends after the queued batches.
    finished: bool,
}

impl Channel {
    pub(super) fn new(batch: usize) -> Self {
        Self {
            state: SyncMutex::new(State {
                ready: VecDeque::with_capacity(QUEUED),
                spare: Vec::with_capacity(QUEUED + 1),
                waker: None,
                closed: false,
                finished: false,
            }),
            space: SyncCondvar::new(),
            batch,
        }
    }

    fn lock(&self) -> SyncGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reader: storage for the next read once the queue has room, or `None`
    /// once the stream is gone.
    pub(super) fn storage(&self) -> Option<Vec<u8>> {
        let mut state = self.lock();
        while state.ready.len() == QUEUED && !state.closed {
            state = self
                .space
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if state.closed {
            return None;
        }
        let mut batch = state.spare.pop().unwrap_or_default();
        drop(state);
        batch.resize(self.batch, 0);
        Some(batch)
    }

    /// Reader: hand over a batch; false once the stream is gone.
    pub(super) fn send(&self, batch: Batch) -> bool {
        let mut state = self.lock();
        if state.closed {
            return false;
        }
        state.ready.push_back(batch);
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    /// Reader: no further batches.
    pub(super) fn finish(&self) {
        let mut state = self.lock();
        state.finished = true;
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Stream: the next batch, or `None` once the reader finished and the
    /// queue is empty. Checks and registers under one lock.
    pub(super) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<Batch>> {
        let mut state = self.lock();
        if let Some(batch) = state.ready.pop_front() {
            drop(state);
            self.space.notify_one();
            return Poll::Ready(Some(batch));
        }
        if state.finished {
            return Poll::Ready(None);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Stream: return spent storage for reuse.
    pub(super) fn recycle(&self, batch: Vec<u8>) {
        let mut state = self.lock();
        if state.spare.len() < QUEUED + 1 {
            state.spare.push(batch);
        }
    }

    /// Stream: stop the reader at its next channel call, releasing a wait
    /// for space.
    pub(super) fn close(&self) {
        self.lock().closed = true;
        self.space.notify_all();
    }

    /// The removed shutdown's mistake, for the model's negative control: the
    /// flag is set but a reader waiting for space is never told.
    #[cfg(all(test, acyclic_harness_loom))]
    pub(super) fn close_unnotified(&self) {
        self.lock().closed = true;
    }
}

/// The reader thread's whole life: read into circulating storage until the
/// input ends, a read fails, or the stream cancels or goes away.
pub(super) fn pump(source: &mut impl Source, channel: &Channel) {
    while let Some(mut batch) = channel.storage() {
        let result = match source.read_input(&mut batch) {
            Ok(None) => break,
            Ok(Some(count)) => {
                batch.truncate(count);
                Ok(batch)
            }
            Err(error) => Err(error),
        };
        let last = !matches!(&result, Ok(batch) if !batch.is_empty());
        if !channel.send(result) || last {
            break;
        }
    }
    channel.finish();
}

/// The stream side: the current batch and what stops the thread.
pub(super) struct Reader {
    channel: Arc<Channel>,
    current: Vec<u8>,
    offset: usize,
    stop: Stop,
}

impl Reader {
    fn start(
        mut source: impl Source,
        stop: impl FnOnce(std::thread::JoinHandle<()>) -> Stop,
    ) -> io::Result<Self> {
        let channel = Arc::new(Channel::new(BATCH));
        let reader = Arc::clone(&channel);
        let thread = std::thread::Builder::new()
            .name("harness-stdin".into())
            .spawn(move || pump(&mut source, &reader))?;
        Ok(Self {
            channel,
            current: Vec::new(),
            offset: 0,
            stop: stop(thread),
        })
    }

    pub(super) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.offset == self.current.len() {
            let batch = match ready!(self.channel.poll_recv(cx)) {
                Some(batch) => batch?,
                None => return Poll::Ready(Ok(())),
            };
            let spent = std::mem::replace(&mut self.current, batch);
            self.offset = 0;
            if spent.capacity() != 0 {
                self.channel.recycle(spent);
            }
        }
        let count = buf.remaining().min(self.current.len() - self.offset);
        buf.put_slice(&self.current[self.offset..self.offset + count]);
        self.offset += count;
        Poll::Ready(Ok(()))
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.channel.close();
        self.stop.stop();
    }
}

#[cfg(unix)]
pub(super) use unix::read_until_cancelled;

#[cfg(unix)]
mod unix {
    use std::fs::File;
    use std::io::{self, Read};

    use super::{Reader, Source, Stop};

    /// Stops the detached thread by hanging up the cancel pipe it polls.
    pub(in super::super) struct Stopper {
        _cancel: io::PipeWriter,
    }

    impl Stopper {
        pub(super) fn stop(&mut self) {}
    }

    struct Polled {
        file: File,
        cancelled: io::PipeReader,
    }

    impl Source for Polled {
        fn read_input(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
            read_until_cancelled(&mut self.file, &self.cancelled, bytes)
        }
    }

    impl Reader {
        pub(in super::super) fn spawn(file: File) -> io::Result<Self> {
            let (cancelled, cancel) = io::pipe()?;
            // Detached: the dropped writer ends its wait.
            Reader::start(Polled { file, cancelled }, |_detached| {
                Stop(Stopper { _cancel: cancel })
            })
        }
    }

    /// Read once the input is ready, or `None` once `cancel` is hung up or
    /// written. The wait is in poll, so a read starts only with input
    /// present and a cancel seen in the same wake wins. An inherited
    /// description may be nonblocking (a socket, say): WouldBlock waits
    /// again instead of ending input, without changing the shared flags.
    pub(in super::super) fn read_until_cancelled(
        file: &mut File,
        cancel: &io::PipeReader,
        bytes: &mut [u8],
    ) -> io::Result<Option<usize>> {
        use rustix::event::{PollFd, PollFlags, poll};
        loop {
            let mut fds = [
                PollFd::new(&*file, PollFlags::IN),
                PollFd::new(cancel, PollFlags::IN),
            ];
            match poll(&mut fds, None) {
                Err(rustix::io::Errno::INTR) => continue,
                Err(error) => return Err(error.into()),
                Ok(_) => {}
            }
            if !fds[1].revents().is_empty() {
                return Ok(None);
            }
            // A device poll cannot watch (macOS reports some as invalid)
            // answers reads at once; waiting on it again would spin.
            let unwatchable = fds[0].revents().contains(PollFlags::NVAL);
            match file.read(bytes) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock && !unwatchable => {}
                read => return read.map(Some),
            }
        }
    }
}

#[cfg(windows)]
pub(super) use windows::end_of_input;

#[cfg(windows)]
mod windows {
    use std::fs::File;
    use std::io::{self, Read};
    use std::sync::Arc;
    use std::thread::JoinHandle;

    use acyclic_native_runtime::terminal::{
        ConsoleWake, cancel_thread, console_read_text, console_wait,
    };

    use super::{BATCH, Reader, Source, Stop};

    /// ERROR_BROKEN_PIPE: the writer closed, which is the end of input.
    const BROKEN_PIPE: i32 = 109;
    /// ERROR_OPERATION_ABORTED: `CancelSynchronousIo` ended the read.
    const OPERATION_ABORTED: i32 = 995;

    /// A closed pipe's writer is the end of input, not an error.
    pub(in super::super) fn end_of_input(read: io::Result<usize>) -> io::Result<usize> {
        match read {
            Err(error) if error.raw_os_error() == Some(BROKEN_PIPE) => Ok(0),
            read => read,
        }
    }

    /// A read the stream cancelled ends the thread; any other result stands.
    fn unless_cancelled(
        read: io::Result<usize>,
        cancel: &ConsoleWake,
    ) -> io::Result<Option<usize>> {
        match read {
            Err(error)
                if error.raw_os_error() == Some(OPERATION_ABORTED) && cancel.is_signalled() =>
            {
                Ok(None)
            }
            read => end_of_input(read).map(Some),
        }
    }

    /// Signals the cancel event, then cancels a read admitted before it
    /// until the thread has observed one, and joins.
    pub(in super::super) struct Stopper {
        cancel: Arc<ConsoleWake>,
        thread: Option<JoinHandle<()>>,
    }

    impl Stopper {
        pub(super) fn stop(&mut self) {
            // A failed signal leaves the read cancellation below, which
            // alone ends a read; a wait for console input would then end at
            // the next input, so the error cannot hang drop forever.
            let _ = self.cancel.signal();
            if let Some(thread) = self.thread.take() {
                while !thread.is_finished() {
                    cancel_thread(&thread);
                }
                let _ = thread.join();
            }
        }
    }

    /// A pipe, file or device read synchronously. Admission checks the
    /// cancel event, and a read in flight is released by cancellation.
    struct Blocking {
        file: File,
        cancel: Arc<ConsoleWake>,
    }

    impl Source for Blocking {
        fn read_input(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
            if self.cancel.is_signalled() {
                return Ok(None);
            }
            unless_cancelled(self.file.read(bytes), &self.cancel)
        }
    }

    /// The console, read from this reader's own handle as UTF-16 and
    /// encoded as UTF-8 here, so no process-wide stdin lock is involved.
    struct Console {
        file: File,
        cancel: Arc<ConsoleWake>,
        units: Vec<u16>,
        text: super::Utf16,
    }

    impl Source for Console {
        fn read_input(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
            loop {
                if self.text.ended {
                    return Ok(Some(0));
                }
                if !console_wait(&self.file, &self.cancel)? {
                    return Ok(None);
                }
                // Each unit encodes to at most three bytes, and a pending
                // high surrogate completes to four with one more unit.
                let room = (bytes.len().saturating_sub(1) / 3).clamp(1, self.units.len());
                let count = match unless_cancelled(
                    console_read_text(&self.file, &mut self.units[..room]),
                    &self.cancel,
                )? {
                    Some(count) => count,
                    None => return Ok(None),
                };
                let written = self.text.decode(&self.units[..count], bytes);
                if written != 0 || self.text.ended {
                    return Ok(Some(written));
                }
            }
        }
    }

    impl Reader {
        fn windows(source: impl FnOnce(Arc<ConsoleWake>) -> Box<dyn Source>) -> io::Result<Self> {
            let cancel = Arc::new(ConsoleWake::new()?);
            let source = source(Arc::clone(&cancel));
            Reader::start(source, |thread| {
                Stop(Stopper {
                    cancel,
                    thread: Some(thread),
                })
            })
        }

        pub(in super::super) fn blocking(file: File) -> io::Result<Self> {
            Self::windows(|cancel| Box::new(Blocking { file, cancel }))
        }

        pub(in super::super) fn console(file: File) -> io::Result<Self> {
            Self::windows(|cancel| {
                Box::new(Console {
                    file,
                    cancel,
                    units: vec![0; BATCH / 3],
                    text: super::Utf16::default(),
                })
            })
        }
    }

    impl Source for Box<dyn Source> {
        fn read_input(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
            (**self).read_input(bytes)
        }
    }
}

/// Stops the reader thread on drop; the platform decides how.
pub(super) struct Stop(#[cfg(unix)] unix::Stopper, #[cfg(windows)] windows::Stopper);

impl Stop {
    fn stop(&mut self) {
        self.0.stop();
    }
}

/// Console UTF-16 to UTF-8, with a high surrogate carried across reads.
/// Unpaired surrogates become U+FFFD; a Ctrl+Z unit ends the input.
#[derive(Default)]
#[cfg(any(windows, test))]
pub(super) struct Utf16 {
    high: Option<u16>,
    pub(super) ended: bool,
}

#[cfg(any(windows, test))]
impl Utf16 {
    /// Encode `units` into `out`, which must hold three bytes per unit plus
    /// one; returns the bytes written.
    pub(super) fn decode(&mut self, units: &[u16], out: &mut [u8]) -> usize {
        let mut written = 0;
        let mut put = |c: char, out: &mut [u8]| {
            written += c.encode_utf8(&mut out[written..]).len();
        };
        for &unit in units {
            if unit == 0x1a {
                if self.high.take().is_some() {
                    put(char::REPLACEMENT_CHARACTER, out);
                }
                self.ended = true;
                break;
            }
            match (self.high.take(), unit) {
                (Some(high), 0xdc00..=0xdfff) => {
                    let scalar =
                        0x10000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(unit) - 0xdc00);
                    put(
                        char::from_u32(scalar).unwrap_or(char::REPLACEMENT_CHARACTER),
                        out,
                    );
                }
                (high, 0xd800..=0xdbff) => {
                    if high.is_some() {
                        put(char::REPLACEMENT_CHARACTER, out);
                    }
                    self.high = Some(unit);
                }
                (high, unit) => {
                    if high.is_some() {
                        put(char::REPLACEMENT_CHARACTER, out);
                    }
                    put(
                        char::from_u32(u32::from(unit)).unwrap_or(char::REPLACEMENT_CHARACTER),
                        out,
                    );
                }
            }
        }
        written
    }
}

#[cfg(test)]
#[path = "reader_tests.rs"]
mod tests;
