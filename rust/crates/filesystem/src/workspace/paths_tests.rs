use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;

// Frozen main restore sequence (cancel check, admit, allocate, copy, record),
// initialized on a separate ledger. Single-threaded it is the reference for
// both callers; the old label differed only by a redundant second admit.
fn old(
    prefix: &str,
    text: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: Option<&CancellationToken>,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    if let Some(cancellation) = cancellation {
        cancellation.check()?;
    }
    let length = prefix.len() + text.len();
    let copied = copy_work(length as u64);
    accounting.admit(copied)?;
    let mut path = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
    path.value.extend_from_slice(prefix.as_bytes());
    path.value.extend_from_slice(text.as_bytes());
    let path = crate::kernel::Owned::new(
        String::from_utf8(path.value).map_err(WorkspaceError::engine)?,
        path.allocation,
        path.bytes,
    );
    if let Err(error) = accounting.record_completed(copied) {
        path.retire(accounting)?;
        return Err(error.into());
    }
    Ok(path)
}

// A label copies a prebuilt name; a restore copies "/" plus the text.
fn candidate(
    name: Option<&WorkspaceName>,
    text: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    match name {
        Some(name) => copy_workspace_label(name, accounting),
        None => restore_absolute_path(text, accounting, cancellation),
    }
}

fn name(text: &str) -> Option<WorkspaceName> {
    WorkspaceName::new(
        text,
        &crate::WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .ok()
    .map(|name| name.value)
}

fn live(accounting: &mut KernelAccounting<'_>, root: &crate::NativeMaterializationWork) -> u64 {
    match accounting.standalone_parts() {
        Some((allocations, ..)) => allocations.live_bytes(),
        None => root.live_allocation_bytes(),
    }
}

// Native setup storage, discovered on an independent unbounded chain, so the
// generated allocation allowance is what remains for the copy itself.
fn setup_peak(mode: u8, recheck: bool) -> u64 {
    if mode == 0 {
        return 0;
    }
    let cancellation = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut work = root.clone();
    for _ in 0..mode.saturating_sub(1).min(2) {
        work = work
            .native_volume_work(crate::WorkBudget::UNBOUNDED)
            .unwrap();
    }
    if mode == 4 {
        work = work.invocation_view().unwrap().0;
    }
    if recheck {
        work = work.recheck_admitted();
    }
    let peak = root.live_allocation_bytes();
    drop(work);
    peak
}

fn measured<T>(operation: impl FnOnce() -> T) -> (T, AllocationInfo) {
    let mut output = None;
    let heap = measure(|| output = Some(operation()));
    (output.unwrap(), heap)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Output, exact errors, every receipt field, live storage and heap match
    // the old path under copy/allocation limits, prior work, cancellation,
    // poison and nested policies; no copy credit stays reserved.
    #[test]
    fn reserved_copies_preserve_output_errors_receipts_storage_and_heap(
        text in "[a-z0-9]{1,24}", restore in any::<bool>(),
        copy_limit in 0_u64..32, allocation_limit in 0_u64..48,
        prior_copy in 0_u64..16, mode in 0_u8..5, recheck in any::<bool>(),
        failure in 0_u8..4,
    ) {
        let prefix = if restore { "/" } else { "" };
        let label = if restore { None } else { name(&text) };
        prop_assume!(restore || label.as_ref().is_some_and(|name| name.as_str() == text));
        let limit = crate::WorkBudget {
            bytes_copied: copy_limit,
            peak_allocation_bytes: setup_peak(mode, recheck) + allocation_limit,
            ..crate::WorkBudget::UNBOUNDED
        };
        let roots_cancel = [CancellationToken::new(), CancellationToken::new()];
        let callers = [CancellationToken::new(), CancellationToken::new()];
        let roots = [0, 1].map(|i| crate::NativeMaterializationWork::new(
            if mode == 1 { limit } else { crate::WorkBudget::UNBOUNDED }, &roots_cancel[i],
        ).unwrap());
        let work = roots.each_ref().map(|root| {
            let mut work = root.clone();
            for _ in 0..mode.saturating_sub(1).min(2) { work = work.native_volume_work(limit).unwrap(); }
            if mode == 4 { work = work.invocation_view().unwrap().0; }
            if recheck { work = work.recheck_admitted(); }
            work
        });
        let mut accounting = [0, 1].map(|i| if mode == 0 {
            KernelAccounting::standalone(limit, &callers[i])
        } else { KernelAccounting::native(&work[i], &callers[i]) });
        for i in [0, 1] {
            let _ = accounting[i].record_completed(copy_work(prior_copy));
            match failure {
                1 => callers[i].cancel(),
                2 if mode != 0 => roots_cancel[i].cancel(),
                3 if mode != 0 => work[i].state(|state| state.poison_native_original_work()),
                _ => (),
            }
        }
        let live_before = [live(&mut accounting[0], &roots[0]), live(&mut accounting[1], &roots[1])];
        let (before, old_heap) = measured(|| {
            old(prefix, &text, &mut accounting[0], restore.then_some(&callers[0]))
        });
        let (after, new_heap) = measured(|| {
            candidate(label.as_ref(), &text, &mut accounting[1], &callers[1])
        });
        prop_assert_eq!(new_heap, old_heap);
        let expected = format!("{prefix}{text}");
        if after.is_ok() {
            // One exactly sized backing allocation and nothing else.
            prop_assert_eq!(new_heap.count_total, 1);
            prop_assert_eq!(new_heap.bytes_total, expected.len() as u64);
        }
        prop_assert_eq!(accounting[1].spent(), accounting[0].spent());
        let held = [live(&mut accounting[0], &roots[0]), live(&mut accounting[1], &roots[1])];
        prop_assert_eq!(held[1] - live_before[1], held[0] - live_before[0]);
        let outcome = |result: Result<crate::kernel::Owned<String>, WorkspaceError>,
                       accounting: &mut KernelAccounting<'_>| match result {
            Ok(owned) => {
                let observed = (owned.value.clone(), owned.bytes);
                owned.retire(accounting).unwrap();
                Ok(observed)
            }
            Err(error) => Err(format!("{error:?}")),
        };
        let before = outcome(before, &mut accounting[0]);
        let after = outcome(after, &mut accounting[1]);
        if let Ok((value, bytes)) = &after {
            prop_assert_eq!(value, &expected);
            prop_assert_eq!(*bytes, expected.len() as u64);
            prop_assert_eq!(accounting[1].spent().bytes_copied, prior_copy + expected.len() as u64);
        }
        prop_assert_eq!(after, before);
        prop_assert_eq!(live(&mut accounting[1], &roots[1]), live_before[1]);
        let credit = work[1].state(|state| (state.budget.bytes_copied, state.original_budget().bytes_copied));
        prop_assert_eq!(credit.0, credit.1);
    }
}

// The copy limit still wins over an exhausted allocation limit: the preflight
// runs before storage is claimed, so neither limit's work is recorded.
#[test]
fn copy_limit_precedes_allocation_and_one_byte_less_has_no_effect() {
    let cancellation = CancellationToken::new();
    for restore in [false, true] {
        for (copies, allocation, denied) in [
            (4, 4, None),
            (3, 0, Some("bytes_copied")),
            (4, 3, Some("peak_allocation_bytes")),
        ] {
            let setup = setup_peak(1, false);
            let limit = crate::WorkBudget {
                bytes_copied: copies,
                peak_allocation_bytes: setup + allocation,
                ..crate::WorkBudget::UNBOUNDED
            };
            let root = crate::NativeMaterializationWork::new(limit, &cancellation).unwrap();
            let mut accounting = KernelAccounting::native(&root, &cancellation);
            let text = if restore { "abc" } else { "abcd" };
            let label = (!restore).then(|| name(text).unwrap());
            let result = candidate(label.as_ref(), text, &mut accounting, &cancellation);
            match denied {
                None => {
                    let owned = result.unwrap();
                    assert_eq!(owned.value.len(), 4);
                    assert_eq!(root.receipt().bytes_copied, 4);
                    assert_eq!(root.live_allocation_bytes(), setup + 4);
                    owned.retire(&mut accounting).unwrap();
                }
                Some(field) => {
                    let error = format!("{:?}", result.err().unwrap());
                    assert!(error.contains(field), "{error}");
                    assert_eq!(root.receipt().bytes_copied, 0);
                }
            }
            assert_eq!(root.live_allocation_bytes(), setup);
            let credit = root
                .state(|state| state.budget.bytes_copied == state.original_budget().bytes_copied);
            assert!(credit);
        }
    }
}
