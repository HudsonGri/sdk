use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;
use std::cell::{Cell, RefCell};

type Event = (u8, WorkCounters, u64, u64);
thread_local! {
    static EVENTS: RefCell<Vec<Event>> = const { RefCell::new(Vec::new()) };
    static REDUNDANT: Cell<bool> = const { Cell::new(false) };
    static ALLOCATE: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn observed(tag: u8, delta: WorkCounters, bytes: u64, operations: u64) {
    EVENTS.with_borrow_mut(|events| {
        if events.capacity() != 0 {
            events.push((tag, delta, bytes, operations));
        }
    });
}

pub(super) fn before(accounting: &KernelAccounting<'_>) -> Result<(), crate::FsError> {
    if REDUNDANT.with(Cell::get) {
        accounting.check()?;
    }
    if ALLOCATE.with(Cell::get) {
        drop(std::hint::black_box(Box::new([0_u8; 17])));
    }
    Ok(())
}

enum Output {
    Remaining(WorkBudget),
    Recorded,
    Claimed(KernelOwnedAllocation),
}

fn operation(
    old: bool,
    kind: u8,
    work: &crate::NativeMaterializationWork,
    caller: &crate::CancellationToken,
    delta: WorkCounters,
    bytes: u64,
) -> Result<Output, crate::FsError> {
    if old {
        // Frozen pre-change wrappers; no candidate accounting method is used.
        if kind == 1 {
            work.record_admitted(delta)?;
            work.admit(WorkCounters::UNCHARGED)?;
            caller.check()?;
            return Ok(Output::Recorded);
        }
        work.admit(WorkCounters::UNCHARGED)?;
        caller.check()?;
        return match kind {
            0 => work.remaining().map(Output::Remaining).map_err(Into::into),
            _ => work
                .claim(bytes, 1, WorkCounters::UNCHARGED)
                .map(KernelOwnedAllocation::NativeOriginal)
                .map(Output::Claimed)
                .map_err(Into::into),
        };
    }
    let mut accounting = KernelAccounting::native(work, caller);
    match kind {
        0 => accounting.remaining().map(Output::Remaining),
        1 => accounting
            .record_completed(delta)
            .map(|()| Output::Recorded),
        _ => accounting.claim_owned(bytes, 1).map(Output::Claimed),
    }
}

fn scoped(
    root: &crate::NativeMaterializationWork,
    mode: u8,
    limit: u64,
) -> crate::NativeMaterializationWork {
    // Invocation registration is funded before introducing bounded resources.
    let mut work = if mode == 3 {
        root.invocation_view().unwrap().0
    } else {
        root.clone()
    };
    for level in 0..mode.min(2) {
        work = work
            .native_volume_work(WorkBudget {
                bytes_copied: limit.saturating_sub(u64::from(level)),
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
    }
    work
}

fn run(
    old: bool,
    kind: u8,
    mode: u8,
    limit: u64,
    delta: WorkCounters,
    bytes: u64,
    failure: u8,
    recheck: bool,
    redundant: bool,
    allocate: bool,
) -> (
    Result<Vec<u8>, String>,
    WorkCounters,
    u64,
    AllocationInfo,
    (usize, usize),
) {
    let root_cancel = crate::CancellationToken::new();
    let caller = crate::CancellationToken::new();
    // An independently initialized probe discovers setup custody, without
    // changing the tested ledger's budget after construction.
    let probe = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &root_cancel).unwrap();
    let probe_work = scoped(&probe, mode, limit);
    let setup = probe.state(|state| {
        let mut reserved = state.budget.remaining(state.original_budget()).unwrap();
        reserved.peak_allocation_bytes = 0;
        state.work.checked_add(reserved).unwrap()
    });
    drop(probe_work);
    drop(probe);
    let root = crate::NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: limit,
            items_examined: setup.items_examined.saturating_add(limit),
            peak_allocation_bytes: setup.peak_allocation_bytes.saturating_add(limit),
            ..WorkBudget::UNBOUNDED
        },
        &root_cancel,
    )
    .unwrap();
    let work = scoped(&root, mode, limit);
    let work = if recheck {
        work.recheck_admitted()
    } else {
        work
    };
    let accounting = KernelAccounting::native(&work, &caller);
    accounting.check().unwrap();
    match failure {
        1 => caller.cancel(),
        2 => root_cancel.cancel(),
        3 => root.state(|state| state.poison_native_original_work()),
        4 => {
            let _ = work.record_admitted(WorkCounters {
                bytes_copied: limit.saturating_add(1),
                ..WorkCounters::UNCHARGED
            });
            caller.cancel();
        }
        _ => (),
    }
    let before = root.receipt();
    let initial_live = root.live_allocation_bytes();
    EVENTS.with_borrow_mut(|events| *events = Vec::with_capacity(64));
    REDUNDANT.with(|flag| flag.set(redundant));
    ALLOCATE.with(|flag| flag.set(allocate));
    let mut measured = None;
    let heap = measure(|| {
        measured = Some(crate::materializer::count_resource_locks(|| {
            root.state
                .count_locks(|| operation(old, kind, &work, &caller, delta, bytes))
        }))
    });
    REDUNDANT.with(|flag| flag.set(false));
    ALLOCATE.with(|flag| flag.set(false));
    let ((result, leaf), resources) = measured.unwrap();
    let live = root.live_allocation_bytes();
    if result.is_ok() {
        let expected = match kind {
            0 => before,
            1 => before
                .checked_add(WorkCounters {
                    items_examined: if recheck { 0 } else { delta.items_examined },
                    ..delta
                })
                .unwrap(),
            _ => before
                .checked_add(WorkCounters {
                    allocation_operations: 1,
                    peak_allocation_bytes: initial_live + bytes,
                    ..WorkCounters::UNCHARGED
                })
                .unwrap(),
        };
        assert_eq!(root.receipt(), expected, "exact production work");
    }
    let result = result
        .map(|output| match output {
            Output::Remaining(remaining) => serde_json::to_vec(&remaining).unwrap(),
            Output::Recorded => Vec::new(),
            Output::Claimed(allocation) => {
                let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
                    panic!("native custody")
                };
                assert_eq!(allocation.bytes(), bytes);
                assert!(allocation.work().shares_allocation_context(&work));
                assert_eq!(live, initial_live + bytes);
                let output =
                    serde_json::to_vec(&(allocation.bytes(), allocation.work().receipt())).unwrap();
                drop(allocation);
                output
            }
        })
        .map_err(|error| format!("{error:?}"));
    assert_eq!(root.live_allocation_bytes(), initial_live);
    for event in EVENTS.with_borrow_mut(std::mem::take) {
        acyclic_test_support::differential::record(event);
    }
    (result, root.receipt(), live, heap, (leaf, resources))
}

fn compare(
    kind: u8,
    mode: u8,
    limit: u64,
    delta: WorkCounters,
    bytes: u64,
    failure: u8,
    recheck: bool,
    redundant: bool,
    allocate: bool,
) -> Result<(), &'static str> {
    let [(old, old_trace), (new, new_trace)] =
        acyclic_test_support::differential::pair::<_, _, Event>(
            || (),
            |(), baseline| {
                run(
                    baseline,
                    kind,
                    mode,
                    limit,
                    delta,
                    bytes,
                    failure,
                    recheck,
                    !baseline && redundant,
                    !baseline && allocate,
                )
            },
        );
    if old.0 != new.0 || old.1 != new.1 || old.2 != new.2 || old_trace != new_trace {
        return Err("full output/error/receipt/storage/operation equality");
    }
    if old.3 != AllocationInfo::default() || new.3 != AllocationInfo::default() {
        return Err("exact zero heap work");
    }
    if new.0.is_ok()
        && (old.4.0 != 2
            || new.4.0 != 1
            || old.4.1 != 2 * usize::from(mode.min(2))
            || new.4.1 != 2 * usize::from(mode.min(2)))
    {
        return Err("exact original-leaf and resource lock counts");
    }
    Ok(())
}

#[test]
fn boundaries_match_independent_old_wrappers_and_detect_redundant_work() {
    for kind in 0..3 {
        for mode in 0..4 {
            compare(
                kind,
                mode,
                64,
                WorkCounters::UNCHARGED,
                8,
                0,
                false,
                false,
                false,
            )
            .unwrap();
            assert_eq!(
                compare(
                    kind,
                    mode,
                    64,
                    WorkCounters::UNCHARGED,
                    8,
                    0,
                    false,
                    true,
                    false
                ),
                Err("exact original-leaf and resource lock counts")
            );
            assert_eq!(
                compare(
                    kind,
                    mode,
                    64,
                    WorkCounters::UNCHARGED,
                    8,
                    0,
                    false,
                    false,
                    true
                ),
                Err("exact zero heap work")
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn boundaries_preserve_full_results_and_denied_prefixes(kind in 0_u8..3, mode in 0_u8..4, limit in 0_u64..32, copies in 0_u64..40, items in 0_u64..40, bytes in 0_u64..40, failure in 0_u8..5, recheck in any::<bool>()) {
        compare(kind, mode, limit, WorkCounters { bytes_copied: copies, items_examined: items, ..WorkCounters::UNCHARGED }, bytes, failure, recheck, false, false).map_err(proptest::test_runner::TestCaseError::fail)?;
    }
}

fn timed(old: bool, kind: u8, mode: u8, iterations: u32) -> f64 {
    // Separate initialization for every side and round. No shared ledgers,
    // cancellation tokens, resource registrations or pending completion work.
    let caller = crate::CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &caller).unwrap();
    let work = scoped(&root, mode, u64::MAX);
    let delta = WorkCounters {
        bytes_copied: 1,
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    };
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let output = operation(old, kind, &work, &caller, delta, 8).unwrap();
        match output {
            Output::Remaining(remaining) => {
                std::hint::black_box(remaining);
            }
            Output::Recorded => (),
            Output::Claimed(allocation) => drop(std::hint::black_box(allocation)),
        }
    }
    let elapsed = start.elapsed().as_nanos() as f64 / f64::from(iterations);
    std::hint::black_box(root.receipt());
    elapsed
}

#[test]
#[ignore = "paired production-path timing report"]
fn paired_accounting_boundaries() {
    for kind in 0..3 {
        for mode in 0..4 {
            // Warm both implementations without retaining their fixture state.
            timed(true, kind, mode, 128);
            timed(false, kind, mode, 128);
            let mut old = Vec::new();
            let mut new = Vec::new();
            let mut gains = Vec::new();
            for round in 0..31 {
                let (before, after) = if round % 2 == 0 {
                    (
                        timed(true, kind, mode, 4096),
                        timed(false, kind, mode, 4096),
                    )
                } else {
                    let after = timed(false, kind, mode, 4096);
                    (timed(true, kind, mode, 4096), after)
                };
                old.push(before);
                new.push(after);
                gains.push(100.0 * (before - after) / before);
            }
            old.sort_by(f64::total_cmp);
            new.sort_by(f64::total_cmp);
            gains.sort_by(f64::total_cmp);
            eprintln!(
                "BOUNDARY_PAIR kind={kind} mode={mode} old_ns={:.1} new_ns={:.1} paired_percent={:.2} range={:.2}..{:.2}",
                old[15], new[15], gains[15], gains[0], gains[30]
            );
        }
    }
}
