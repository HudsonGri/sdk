use super::tests::{amount, counters, status};
use super::*;
use proptest::prelude::*;

/// Runs `operation`, returning its leaf locks and registry reconciliations.
fn probed<T>(
    root: &crate::NativeMaterializationWork,
    operation: impl FnOnce() -> T,
) -> (T, usize, usize) {
    let before = crate::demand::RECONCILES.with(std::cell::Cell::get);
    let (result, locks) = root.state.count_locks(operation);
    let reconciles = crate::demand::RECONCILES.with(std::cell::Cell::get) - before;
    (result, locks, reconciles)
}

/// The replaced native sequences: the independent baseline.
fn old_admit(
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    let native = crate::materializer::native_materialization_work_error;
    work.state(|state| {
        state.admit(WorkCounters::UNCHARGED).map_err(native)?;
        cancellation.check()?;
        state.admit(delta).map_err(native).map_err(Into::into)
    })
}

fn old_charge(
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    let native = crate::materializer::native_materialization_work_error;
    work.state(|state| {
        state.admit(WorkCounters::UNCHARGED).map_err(native)?;
        cancellation.check()?;
        state.charge(delta).map_err(native).map_err(Into::into)
    })
}

fn old_record(
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    work.record_admitted(delta)?;
    work.admit(WorkCounters::UNCHARGED)?;
    cancellation.check().map_err(Into::into)
}

fn new_call(
    operation: u8,
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    let mut accounting = KernelAccounting::native(work, cancellation);
    match operation % 3 {
        0 => accounting.admit(delta),
        1 => accounting.charge(delta),
        _ => accounting.record_completed(delta),
    }
}

fn old_call(
    operation: u8,
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    match operation % 3 {
        0 => old_admit(work, cancellation, delta),
        1 => old_charge(work, cancellation, delta),
        _ => old_record(work, cancellation, delta),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    /// Reconciling once per leaf gives the replaced sequences' exact results,
    /// receipts and live storage at every step, through cancellation, poison
    /// and every limit.
    #[test]
    fn one_reconciliation_matches_the_replaced_sequences(
        limits in prop::array::uniform24(amount(96)),
        steps in prop::collection::vec((0_u8..3, prop::array::uniform24(amount(8))), 0..16),
        resource in prop::array::uniform3(amount(64)),
        scoped in any::<bool>(), invocation in any::<bool>(),
        stop_at in 0_usize..16, failure in 0_u8..4,
    ) {
        let root_cancel = [crate::CancellationToken::new(), crate::CancellationToken::new()];
        let caller_cancel = [crate::CancellationToken::new(), crate::CancellationToken::new()];
        let mut budget = WorkBudget::UNBOUNDED;
        budget.items_examined = limits[19].saturating_add(64);
        budget.bytes_copied = limits[13];
        budget.bytes_hashed = limits[12];
        budget.object_probes = limits[4];
        let roots = [0, 1].map(|i| crate::NativeMaterializationWork::new(budget, &root_cancel[i]).unwrap());
        let work = roots.each_ref().map(|root| {
            let mut work = root.clone();
            if scoped {
                work = work.native_volume_work(WorkBudget {
                    bytes_copied: resource[0], items_examined: resource[1].saturating_add(64),
                    bytes_hashed: resource[2], ..WorkBudget::UNBOUNDED
                }).unwrap();
            }
            if invocation { work = work.invocation_view().unwrap().0; }
            work
        });
        for (step, (operation, delta)) in steps.into_iter().enumerate() {
            if step == stop_at {
                for i in [0, 1] {
                    match failure {
                        1 => caller_cancel[i].cancel(),
                        2 => root_cancel[i].cancel(),
                        3 => work[i].state(|state| state.poison_native_original_work()),
                        _ => (),
                    }
                }
            }
            let delta = counters(delta);
            let old = old_call(operation, &work[0], &caller_cancel[0], delta);
            let new = new_call(operation, &work[1], &caller_cancel[1], delta);
            prop_assert_eq!(status(new), status(old));
            prop_assert_eq!(roots[1].receipt(), roots[0].receipt());
            prop_assert_eq!(roots[1].live_allocation_bytes(), roots[0].live_allocation_bytes());
        }
    }
}

/// Each native kernel call takes the leaf once and reconciles the registry
/// once; the replaced sequences took two of either.
#[test]
fn native_kernel_calls_reconcile_once_under_one_lock() {
    let cancellation = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let delta = WorkCounters {
        bytes_copied: 3,
        items_examined: 2,
        ..WorkCounters::UNCHARGED
    };
    for operation in 0..3 {
        let (result, locks, reconciles) =
            probed(&root, || new_call(operation, &root, &cancellation, delta));
        result.unwrap();
        assert_eq!((operation, locks, reconciles), (operation, 1, 1));
        // Negative control: the replaced sequence is detected.
        let (result, locks, reconciles) =
            probed(&root, || old_call(operation, &root, &cancellation, delta));
        result.unwrap();
        assert_eq!(
            (operation, locks + reconciles),
            (operation, if operation == 2 { 4 } else { 3 })
        );
    }
}
