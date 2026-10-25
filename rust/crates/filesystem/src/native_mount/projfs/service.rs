//! The projector service of a shared ProjFS instance: one thread runs the
//! projector passes of every view of the instance, so a view costs no thread
//! of its own. A view is scheduled when its projector has work, runs one pass
//! at a time, and is rescheduled while work remains; held entries retry on
//! one shared timer. Detaching a view waits for its running pass, after which
//! the service holds nothing of it.

use super::*;
use std::collections::VecDeque;
use std::sync::Weak;

pub(super) struct ServiceHook {
    service: Weak<Shared>,
    id: u64,
}

impl ServiceHook {
    pub(super) fn schedule(&self) {
        if let Some(service) = self.service.upgrade() {
            service.schedule(self.id);
        }
    }
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    views: HashMap<u64, View>,
    queue: Queue,
    running: Option<u64>,
    next: u64,
    stopping: bool,
}

struct View {
    runtime: Arc<Runtime>,
    context: SendContext,
}

/// Which view runs next. A held view retries [`HELD_RETRY`] after it was
/// held, and a due retry joins the ready queue before the next pass is
/// chosen, so other views' ready work never starves it.
#[derive(Default)]
struct Queue {
    ready: VecDeque<u64>,
    /// The views in `ready`.
    queued: HashSet<u64>,
    delayed: HashSet<u64>,
    retry_at: Option<std::time::Instant>,
}

impl Queue {
    /// Queues a view to run; false when it already is.
    fn schedule(&mut self, id: u64) -> bool {
        if !self.queued.insert(id) {
            return false;
        }
        self.delayed.remove(&id);
        self.ready.push_back(id);
        true
    }

    /// Holds a view that has only held entries left, unless new work queued it.
    fn delay(&mut self, id: u64, now: std::time::Instant) {
        if !self.queued.contains(&id) {
            self.delayed.insert(id);
            self.retry_at.get_or_insert(now + HELD_RETRY);
        }
    }

    fn remove(&mut self, id: u64) {
        if self.queued.remove(&id) {
            self.ready.retain(|queued| *queued != id);
        }
        self.delayed.remove(&id);
    }

    /// The next view to run, or how long to wait for one: `None` waits for
    /// a schedule.
    fn next(&mut self, now: std::time::Instant) -> Result<u64, Option<std::time::Duration>> {
        if self.delayed.is_empty() {
            self.retry_at = None;
        } else if self.retry_at.is_some_and(|due| now >= due) {
            self.retry_at = None;
            for id in std::mem::take(&mut self.delayed) {
                if self.queued.insert(id) {
                    self.ready.push_back(id);
                }
            }
        }
        if let Some(id) = self.ready.pop_front() {
            self.queued.remove(&id);
            return Ok(id);
        }
        Err(self.retry_at.map(|due| due.saturating_duration_since(now)))
    }
}

impl Shared {
    fn schedule(&self, id: u64) {
        let mut state = lock_recover(&self.state);
        if state.views.contains_key(&id) && state.queue.schedule(id) {
            self.changed.notify_all();
        }
    }

    fn run(&self) {
        let mut state = lock_recover(&self.state);
        loop {
            if state.stopping {
                return;
            }
            let id = match state.queue.next(std::time::Instant::now()) {
                Ok(id) => id,
                Err(None) => {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    continue;
                }
                Err(Some(timeout)) => {
                    state = self
                        .changed
                        .wait_timeout(state, timeout)
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .0;
                    continue;
                }
            };
            let Some(view) = state.views.get(&id) else {
                continue;
            };
            let runtime = Arc::clone(&view.runtime);
            let context = view.context.0;
            state.running = Some(id);
            drop(state);
            let readiness = runtime.projector.service_pass(&runtime, context);
            drop(runtime);
            state = lock_recover(&self.state);
            state.running = None;
            self.changed.notify_all();
            if !state.views.contains_key(&id) {
                continue;
            }
            match readiness {
                Readiness::Ready => {
                    state.queue.schedule(id);
                }
                Readiness::Delayed => state.queue.delay(id, std::time::Instant::now()),
                Readiness::Idle | Readiness::Stopped => {}
            }
        }
    }
}

/// One shared instance's projector service. Dropping it stops and joins
/// its thread.
pub(super) struct ProjectorService {
    shared: Arc<Shared>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ProjectorService {
    pub(super) fn start() -> Result<Self, NativeMountError> {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        });
        let runner = Arc::clone(&shared);
        let worker = std::thread::Builder::new()
            .name("acyclic-projfs-projector".to_owned())
            .spawn(move || runner.run())
            .map_err(NativeMountError::Io)?;
        Ok(Self {
            shared,
            worker: Some(worker),
        })
    }

    /// Makes the service the runner of `runtime`'s projector and schedules
    /// its first pass. Returns the id that detaches it.
    pub(super) fn attach(
        &self,
        runtime: &Arc<Runtime>,
        context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    ) -> Result<u64, NativeMountError> {
        let id = {
            let mut state = lock_recover(&self.shared.state);
            state.next += 1;
            let id = state.next;
            state.views.insert(
                id,
                View {
                    runtime: Arc::clone(runtime),
                    context: SendContext(context),
                },
            );
            id
        };
        runtime
            .projector
            .service
            .set(ServiceHook {
                service: Arc::downgrade(&self.shared),
                id,
            })
            .map_err(|_| NativeMountError::ControlPending)?;
        self.shared.schedule(id);
        Ok(id)
    }

    /// Removes a view, waiting for its running pass; afterwards the service
    /// holds no reference to it and never runs it again.
    pub(super) fn detach(&self, id: u64) {
        let mut state = lock_recover(&self.shared.state);
        state.views.remove(&id);
        state.queue.remove(id);
        while state.running == Some(id) {
            state = self
                .shared
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl Drop for ProjectorService {
    fn drop(&mut self) {
        lock_recover(&self.shared.state).stopping = true;
        self.shared.changed.notify_all();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            tracing::warn!("ProjFS projector service panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_view_retries_while_other_views_stay_ready() {
        let start = std::time::Instant::now();
        let mut queue = Queue::default();
        queue.schedule(1);
        assert_eq!(queue.next(start), Ok(1));
        queue.delay(1, start);
        // View 2 always has fresh work: it is rescheduled after every pass.
        let mut now = start;
        let mut retried = false;
        for _ in 0..1000 {
            queue.schedule(2);
            now += HELD_RETRY / 4;
            match queue.next(now) {
                Ok(1) => {
                    retried = true;
                    break;
                }
                Ok(2) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        assert!(retried, "a busy sibling starved the held view");
        assert!(
            now - start <= HELD_RETRY * 2,
            "retried late: {:?}",
            now - start
        );
    }

    #[test]
    fn an_idle_queue_waits_for_the_held_retry_or_a_schedule() {
        let start = std::time::Instant::now();
        let mut queue = Queue::default();
        assert_eq!(queue.next(start), Err(None));
        queue.delay(1, start);
        assert_eq!(queue.next(start), Err(Some(HELD_RETRY)));
        // New work runs the view at once and cancels its retry.
        assert!(queue.schedule(1));
        assert!(!queue.schedule(1));
        assert_eq!(queue.next(start), Ok(1));
        assert_eq!(queue.next(start), Err(None));
        queue.delay(1, start);
        queue.remove(1);
        assert_eq!(queue.next(start + HELD_RETRY), Err(None));
    }
}
