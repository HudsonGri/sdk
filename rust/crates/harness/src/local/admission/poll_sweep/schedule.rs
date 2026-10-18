//! Push barriers around the real native submissions; no timer or poll-index cuts.
use acyclic_native_runtime::testing::{Boundary, Schedule as IoSchedule};
use std::{
    cell::Cell,
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Default)]
struct State {
    cancelled: bool,
    active: usize,
    trace: Vec<String>,
    points: Vec<&'static str>,
    publication: Arc<str>,
    cancelled_at: Option<String>,
    admission_submission: Option<usize>,
}

pub(super) struct Schedule {
    state: Mutex<State>,
    io: IoSchedule,
}

impl Schedule {
    pub fn new() -> (Arc<Self>, UnboundedReceiver<Boundary>) {
        let (io, events) = IoSchedule::new();
        (
            Arc::new(Self {
                state: Mutex::default(),
                io,
            }),
            events,
        )
    }
    // Sole test-state lock; no callback, send, or await while held.
    pub fn start(&self) {
        *self.state.lock().unwrap() = State::default();
    }
    pub async fn point(&self, name: &'static str) {
        acyclic_native_runtime::testing::boundary(name).await;
    }
    pub fn describe(&self, publication: impl FnOnce() -> String) {
        let publication = publication();
        self.state.lock().unwrap().publication = publication.clone().into();
        acyclic_native_runtime::testing::describe(|| publication);
    }
    pub fn failure(&self) -> String {
        let state = self.state.lock().unwrap();
        state
            .cancelled_at
            .clone()
            .unwrap_or_else(|| state.publication.to_string())
    }
    pub fn dispatch(self: &Arc<Self>, label: &'static str) -> Active {
        let mut state = self.state.lock().unwrap();
        assert!(
            !state.cancelled,
            "external dispatch after cancellation: {label}"
        );
        state.active += 1;
        state.trace.push(label.into());
        Active(self.clone())
    }
    pub fn drained(&self) -> Vec<String> {
        assert_eq!(
            self.state.lock().unwrap().active,
            0,
            "external effects remain active"
        );
        let trace = self.io.trace();
        let cancel = trace.iter().position(|record| record == "CANCEL").unwrap();
        assert!(
            trace[cancel + 1..]
                .iter()
                .all(|record| record.ends_with(":completion")),
            "new dispatch after cancellation: {trace:?}"
        );
        trace
    }
    pub fn recover(&self) {
        self.state.lock().unwrap().cancelled = false;
    }
    pub fn admission_submission(&self) -> Option<usize> {
        self.state.lock().unwrap().admission_submission
    }
    pub fn ready_reads(&self) -> usize {
        self.io.ready_reads()
    }
    pub fn points(&self) -> Vec<&'static str> {
        self.state.lock().unwrap().points.clone()
    }
}

pub(super) struct Active(Arc<Schedule>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().active -= 1;
    }
}

pub(super) enum Exit<T> {
    Cancelled,
    Completed(T),
}

/// The boundaries at which durable state can change: submissions and external
/// effects. Completion and blocking-pool boundaries of independent owners may
/// interleave differently between replays (#949) without changing durable state.
pub(super) const DURABLE: [&str; 6] = [
    "model:accepted",
    "model:dispatch",
    "native:submitted",
    "task:submitted",
    "tool:accepted",
    "tool:dispatch",
];

pub(super) fn durable(point: &str) -> bool {
    DURABLE.contains(&point)
}

pub(super) fn durable_count(points: &[&'static str]) -> usize {
    points.iter().filter(|point| durable(point)).count()
}

/// A cancellation at the `occurrence`-th boundary named `name`. Every boundary
/// name occurs the same number of times in every replay, so the cut always
/// fires, at the same operation, however independent owners interleave.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Cut {
    pub name: &'static str,
    pub occurrence: usize,
    /// Durable boundaries through this one in the baseline; exact for durable cuts.
    pub durable: usize,
}

/// Where a submitted operation ends or blocks: cancelling there leaves its
/// result observable while the caller has not acted on it.
pub(super) const OPERATION: [&str; 4] = [
    "native:completed",
    "task:completed",
    "stream:read:blocked",
    "stream:read:completed",
];

/// Operation-aware cuts: every durable boundary and every operation boundary,
/// by name and occurrence. Planned names occur equally often in every replay,
/// so the plan is the same for every baseline interleaving.
pub(super) fn plan(baseline: &[&'static str]) -> Vec<Cut> {
    let mut seen = BTreeMap::<&str, usize>::new();
    let mut ordinal = 0;
    let mut cuts = Vec::new();
    for &name in baseline {
        let occurrence = seen.entry(name).or_default();
        *occurrence += 1;
        ordinal += usize::from(durable(name));
        if durable(name) || OPERATION.contains(&name) {
            cuts.push(Cut {
                name,
                occurrence: *occurrence,
                durable: ordinal,
            });
        }
    }
    cuts
}

/// A replay cancelled after `ordinal` durable boundaries must reproduce exactly
/// that durable prefix of the baseline; independent boundaries may reorder.
pub(super) fn check_replay(
    points: &[&'static str],
    baseline: &[&'static str],
    ordinal: usize,
) -> Result<(), String> {
    let replayed = points.iter().filter(|point| durable(point));
    let expected = baseline.iter().filter(|point| durable(point));
    if replayed.clone().count() == ordinal && replayed.eq(expected.take(ordinal)) {
        Ok(())
    } else {
        Err(format!(
            "replay diverged before cancellation: {:?} is not the first {ordinal} of {:?}",
            points
                .iter()
                .filter(|point| durable(point))
                .collect::<Vec<_>>(),
            baseline
                .iter()
                .filter(|point| durable(point))
                .collect::<Vec<_>>(),
        ))
    }
}

thread_local! {
    // Negative control only: keep polling the cancelled caller.
    static RETAIN_CALLER: Cell<bool> = const { Cell::new(false) };
}

/// Runs `body` with a driver that does not release the caller at the cut.
pub(super) fn retaining_caller<T>(body: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            RETAIN_CALLER.set(false);
        }
    }
    RETAIN_CALLER.set(true);
    let _reset = Reset;
    body()
}

/// Drives `future`, cancelling at `cut`. Returns the exit and the number of
/// boundaries and durable boundaries hit.
pub(super) async fn drive<F: Future>(
    future: F,
    cut: Option<Cut>,
    schedule: &Schedule,
    events: &mut UnboundedReceiver<Boundary>,
) -> (Exit<F::Output>, usize, usize) {
    let mut future = Box::pin(schedule.io.scope(tokio::task::unconstrained(future)));
    let mut hit = 0;
    let mut ordinal = 0;
    let mut seen = BTreeMap::<&str, usize>::new();
    loop {
        tokio::select! {
            output = &mut future => {
                drop(future);
                drain(schedule, events).await;
                return (Exit::Completed(output), hit, ordinal);
            },
            event = events.recv() => {
                let event = event.expect("provider lost its push driver");
                hit += 1;
                ordinal += usize::from(durable(event.name));
                let occurrence = seen.entry(event.name).or_default();
                *occurrence += 1;
                {
                    let mut state = schedule.state.lock().unwrap();
                    if event.name == "task:submitted"
                        && (event.detail.contains("append harness/v3/coordinator/events ")
                            || (event.detail.starts_with("commit ")
                                && event.detail.contains("\"harness/v3/coordinator/events\"")))
                    {
                        state.admission_submission.get_or_insert(ordinal);
                    }
                    state.points.push(event.name);
                    state.publication = event.detail.clone();
                }
                if cut.is_some_and(|cut| cut.name == event.name && cut.occurrence == *occurrence) {
                    {
                        let mut state = schedule.state.lock().unwrap();
                        state.cancelled = true;
                        state.cancelled_at = Some(format!("{}: {}", event.name, event.detail));
                    }
                    schedule.io.cancel();
                    if RETAIN_CALLER.get() {
                        event.release();
                        loop {
                            tokio::select! {
                                _ = &mut future => break,
                                event = events.recv() => event.unwrap().release(),
                            }
                        }
                        drop(future);
                        drain(schedule, events).await;
                        return (Exit::Cancelled, hit, ordinal);
                    }
                    // A JoinHandle's drop detaches its already submitted work.
                    // Cancel the caller first, then release and drain those owners.
                    drop(future);
                    event.release();
                    drain(schedule, events).await;
                    return (Exit::Cancelled, hit, ordinal);
                }
                event.release();
            }
        }
    }
}

async fn drain(schedule: &Schedule, events: &mut UnboundedReceiver<Boundary>) {
    loop {
        tokio::select! {
            _ = schedule.io.drained() => break,
            event = events.recv() => event.unwrap().release(),
        }
    }
}

pub(super) fn check_cut<T>(exit: &Exit<T>, cut: Cut, ordinal: usize) -> Result<(), &'static str> {
    if matches!(exit, Exit::Cancelled) && (!durable(cut.name) || cut.durable == ordinal) {
        Ok(())
    } else {
        Err("cancellation point was skipped")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TURN: [&str; 10] = [
        "task:submitted",
        "blocking:submitted",
        "task:completed",
        "native:submitted",
        "blocking:completed",
        "model:dispatch",
        "native:completed",
        "model:accepted",
        "task:submitted",
        "task:completed",
    ];

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        // Any interleaving of independent boundaries fires every planned cut at
        // its operation with a replayable durable prefix.
        #[test]
        fn independent_reordering_keeps_every_operation_cut(
            swaps in proptest::collection::vec(0usize..256, 1..32),
        ) {
            let baseline = TURN.repeat(4);
            let mut replay = baseline.clone();
            for swap in swaps {
                let index = swap % (replay.len() - 1);
                if !(durable(replay[index]) && durable(replay[index + 1])) {
                    replay.swap(index, index + 1);
                }
            }
            for cut in plan(&baseline) {
                let at = replay
                    .iter()
                    .enumerate()
                    .filter(|(_, point)| **point == cut.name)
                    .nth(cut.occurrence - 1)
                    .unwrap()
                    .0;
                let ordinal = durable_count(&replay[..=at]);
                proptest::prop_assert_eq!(check_replay(&replay[..=at], &baseline, ordinal), Ok(()));
                if durable(cut.name) {
                    proptest::prop_assert_eq!(ordinal, cut.durable);
                }
            }
        }
    }

    #[test]
    fn plan_cuts_every_durable_and_operation_boundary() {
        let cut = |name, occurrence, durable| Cut {
            name,
            occurrence,
            durable,
        };
        let baseline = [
            "task:submitted",
            "task:completed",
            "blocking:submitted",
            "task:completed",
            "blocking:completed",
            "native:submitted",
            "native:completed",
        ];
        assert_eq!(
            plan(&baseline),
            [
                cut("task:submitted", 1, 1),
                cut("task:completed", 1, 1),
                cut("task:completed", 2, 1),
                cut("native:submitted", 1, 2),
                cut("native:completed", 1, 2),
            ]
        );
        // The same operations are cut however independent boundaries interleave.
        let mut replay = TURN.to_vec();
        replay.swap(2, 3);
        let names = |cuts: Vec<Cut>| {
            let mut cuts: Vec<_> = cuts
                .into_iter()
                .map(|cut| (cut.name, cut.occurrence))
                .collect();
            cuts.sort_unstable();
            cuts
        };
        assert_eq!(names(plan(&replay)), names(plan(&TURN)));
    }

    #[test]
    fn reordered_completion_negative_control() {
        let baseline = TURN.to_vec();
        let mut replay = baseline.clone();
        // A completion overtakes an unrelated submission, as after #949.
        replay.swap(2, 3);
        let cut = durable_count(&baseline[..4]);
        // The former all-boundary planner cut a different event and diverged.
        assert_ne!(replay[..4], baseline[..4]);
        assert_eq!(check_replay(&replay[..4], &baseline, cut), Ok(()));
        // Durable divergence is still rejected: reordered and missing effects.
        let mut swapped = baseline.clone();
        swapped.swap(3, 5);
        let cut = durable_count(&baseline[..6]);
        assert!(check_replay(&swapped[..6], &baseline, cut).is_err());
        let mut missing = baseline.clone();
        missing.remove(3);
        assert!(check_replay(&missing[..5], &baseline, cut).is_err());
    }
}
