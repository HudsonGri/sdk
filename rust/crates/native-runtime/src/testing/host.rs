//! A real host worker yields only at an original blocking future's Pending.
use super::*;
#[cfg(acyclic_native_runtime_loom)]
use loom::sync::Condvar;
#[cfg(not(acyclic_native_runtime_loom))]
use std::sync::Condvar;
use std::task::{Context as PollContext, Wake};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Starting,
    Running,
    Parked,
    Finished,
}

struct State {
    phase: Phase,
    ready: bool,
    io_ready: bool,
    driver: Option<Waker>,
    #[cfg(test)]
    forget_ready: bool,
    #[cfg(test)]
    forget_io: bool,
}

// Sole handoff lock. Never nested with executor, schedule, channel or provider
// locks; future polls, callbacks, wakes and original I/O happen outside it.
pub(super) struct Host {
    state: Mutex<State>,
    changed: Condvar,
}

impl Host {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                phase: Phase::Starting,
                ready: false,
                io_ready: false,
                driver: None,
                #[cfg(test)]
                forget_ready: false,
                #[cfg(test)]
                forget_io: false,
            }),
            changed: Condvar::new(),
        })
    }

    pub(super) fn run<T>(self: &Arc<Self>, work: impl FnOnce() -> T) -> T {
        lock(&self.state).phase = Phase::Running;
        let _finished = Finished(self.clone());
        let context = Context::simulation().map(|mut context| {
            context.host = Some(self.clone());
            context
        });
        BLOCKING_CONTEXT.sync_scope(context, work)
    }

    fn block<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        let waker = Waker::from(self.clone());
        let mut cx = PollContext::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            // Synchronous physical work retains sources only. Its explicit
            // async callback must also retain task ownership: submissions can
            // yield this worker's permit at Pending and run in seeded order.
            let context = Context::simulation();
            if let Poll::Ready(output) =
                CURRENT.sync_scope(context, || future.as_mut().poll(&mut cx))
            {
                return output;
            }
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.phase = Phase::Parked;
            self.changed.notify_one();
            while state.phase == Phase::Parked {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
    }

    // Wait for the physical worker to finish or reach an original async wait.
    // Root/owned futures can then satisfy that wait without any recursive poll.
    pub(super) async fn drive<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let io_waker = Waker::from(Arc::new(IoWake(self.clone())));
        std::future::poll_fn(|cx| {
            'poll_io: loop {
                if let Poll::Ready(output) = future
                    .as_mut()
                    .poll(&mut PollContext::from_waker(&io_waker))
                {
                    return Poll::Ready(output);
                }
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.driver = Some(cx.waker().clone());
                loop {
                    if state.io_ready {
                        state.io_ready = false;
                        drop(state);
                        continue 'poll_io;
                    }
                    match state.phase {
                        Phase::Finished => {
                            drop(state);
                            return Poll::Ready(executor::complete_io(future.as_mut()));
                        }
                        Phase::Parked if state.ready => {
                            state.ready = false;
                            state.phase = Phase::Running;
                            self.changed.notify_one();
                        }
                        Phase::Parked => return Poll::Pending,
                        _ => (),
                    }
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        })
        .await
    }
}

// Queue capacity and physical completion wakes stay inside the I/O step;
// device latency must not enqueue an extra simulated task poll.
struct IoWake(Arc<Host>);
impl Wake for IoWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        {
            let mut state = lock(&self.0.state);
            #[cfg(test)]
            if state.forget_io {
                return;
            }
            state.io_ready = true;
        }
        self.0.changed.notify_one();
    }
}

struct Finished(Arc<Host>);
impl Drop for Finished {
    fn drop(&mut self) {
        lock(&self.0.state).phase = Phase::Finished;
        self.0.changed.notify_one();
    }
}

impl Wake for Host {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let driver = {
            let mut state = lock(&self.state);
            #[cfg(test)]
            if state.forget_ready {
                return;
            }
            state.ready = true;
            state.driver.clone()
        };
        if let Some(driver) = driver {
            driver.wake();
        }
    }
}

/// Waits on the original channel/control future on its real blocking worker.
/// A simulated worker yields its permit at Pending and resumes by seed order.
pub fn blocking_on<F: Future>(future: F) -> F::Output {
    match Context::simulation().and_then(|context| context.host) {
        Some(host) => host.block(future),
        None => executor::complete_io(future),
    }
}

#[cfg(all(test, acyclic_native_runtime_loom))]
mod models;
#[cfg(all(test, not(acyclic_native_runtime_loom)))]
mod tests;
