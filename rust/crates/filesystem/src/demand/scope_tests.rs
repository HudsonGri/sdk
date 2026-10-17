use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

std::thread_local! {
    static OMIT_RESTORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(super) fn omit_restore() -> bool {
    OMIT_RESTORE.with(std::cell::Cell::get)
}

proptest::proptest! {
    // Property: arbitrary nested scopes restore the caller's value on both
    // return and unwind, without undoing work completed inside the scope.
    #[test]
    fn nested_scopes_restore_policy_and_preserve_completed_work(
        original in proptest::bool::ANY,
        frames in proptest::collection::vec((proptest::bool::ANY, proptest::num::u8::ANY), 1..33),
        unwind in proptest::bool::ANY,
    ) {
        check::<true>(original, &frames, unwind);
    }
}

fn assert_policy<const ASSERT: bool>(actual: bool, expected: bool) {
    if ASSERT {
        assert_eq!(actual, expected, "scope policy restoration");
    }
}

fn check<const ASSERT: bool>(original: bool, frames: &[(bool, u8)], unwind: bool) {
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    work.recheck_items = original;
    let result = catch_unwind(AssertUnwindSafe(|| {
        nested::<ASSERT>(&mut work, frames, unwind)
    }));
    assert_policy::<ASSERT>(work.recheck_items, original);
    assert_eq!(result.is_err(), unwind);
    if let Err(panic) = result {
        assert!(
            panic.is::<()>(),
            "only the deliberate callback panic may escape"
        );
    }
    assert_eq!(
        work.work.bytes_copied,
        frames
            .iter()
            .map(|(_, bytes)| u64::from(*bytes))
            .sum::<u64>()
    );
}

fn nested<const ASSERT: bool>(work: &mut DemandWork, frames: &[(bool, u8)], unwind: bool) {
    let Some((&(policy, bytes), remaining)) = frames.split_first() else {
        if unwind {
            std::panic::panic_any(());
        }
        return;
    };
    let original = work.recheck_items;
    let result = catch_unwind(AssertUnwindSafe(|| {
        work.scope(
            |work| &mut work.recheck_items,
            policy,
            |work| {
                assert_policy::<ASSERT>(work.recheck_items, policy);
                work.work.bytes_copied += u64::from(bytes);
                nested::<ASSERT>(work, remaining, unwind);
                assert_policy::<ASSERT>(work.recheck_items, policy);
            },
        )
    }));
    assert_policy::<ASSERT>(work.recheck_items, original);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[test]
fn missing_production_restoration_requires_the_policy_assertion() {
    for unwind in [false, true] {
        for original in [false, true] {
            OMIT_RESTORE.with(|flag| flag.set(true));
            let result = catch_unwind(|| check::<true>(original, &[(!original, 7)], unwind));
            OMIT_RESTORE.with(|flag| flag.set(false));
            let panic = result.expect_err("omitted production restoration must fail");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("");
            assert!(message.contains("scope policy restoration"), "{message}");
            OMIT_RESTORE.with(|flag| flag.set(true));
            let without_assertion =
                catch_unwind(|| check::<false>(original, &[(!original, 7)], unwind));
            OMIT_RESTORE.with(|flag| flag.set(false));
            assert!(
                without_assertion.is_ok(),
                "the same mutant passes only if the policy assertion is deleted"
            );
        }
    }
}

/// What one leaf call sees of its caller's view.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    invocation: Option<usize>,
    resource: Option<usize>,
    custody: Option<usize>,
    recheck_items: bool,
}

fn seen(work: &DemandWork) -> Seen {
    Seen {
        invocation: work
            .native_invocation
            .as_ref()
            .map(|view| view.as_ptr() as usize),
        resource: work
            .native_resource
            .as_ref()
            .map(|view| view.as_ptr() as usize),
        custody: work
            .capture_custody
            .as_ref()
            .map(|view| view.as_ptr() as usize),
        recheck_items: work.recheck_items,
    }
}

/// The per-field nested scopes one view scope replaces: the independent
/// baseline for what a leaf call sees and what it restores.
fn nested_baseline(
    work: &crate::NativeMaterializationWork,
    original: bool,
    unwind: bool,
) -> (Seen, Seen) {
    let inside = std::cell::RefCell::new(None);
    let result = catch_unwind(AssertUnwindSafe(|| {
        work.state.with_original_leaf(|state| {
            state.scope(
                |work| &mut work.native_invocation,
                work.invocation.as_ref().map(std::sync::Arc::downgrade),
                |state| {
                    state.scope(
                        |work| &mut work.native_resource,
                        work.resource.as_ref().map(std::sync::Arc::downgrade),
                        |state| {
                            state.scope(
                                |work| &mut work.capture_custody,
                                if original {
                                    work.capture_origin.clone()
                                } else {
                                    None
                                },
                                |state| {
                                    state.scope(
                                        |state| &mut state.recheck_items,
                                        work.recheck_items,
                                        |state| {
                                            *inside.borrow_mut() = Some(seen(state));
                                            if unwind {
                                                std::panic::panic_any(());
                                            }
                                        },
                                    )
                                },
                            )
                        },
                    )
                },
            )
        })
    }));
    assert_eq!(result.is_err(), unwind);
    let after = work.state.with_original_leaf(|state| seen(state));
    (inside.into_inner().unwrap(), after)
}

fn view_scope(
    work: &crate::NativeMaterializationWork,
    original: bool,
    unwind: bool,
) -> (Seen, Seen) {
    let inside = std::cell::RefCell::new(None);
    let observe = |state: &mut DemandWork| {
        *inside.borrow_mut() = Some(seen(state));
        if unwind {
            std::panic::panic_any(());
        }
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        if original {
            work.with_original_leaf(observe);
        } else {
            work.state(observe);
        }
    }));
    assert_eq!(result.is_err(), unwind);
    let after = work.state.with_original_leaf(|state| seen(state));
    (inside.into_inner().unwrap(), after)
}

/// One view scope shows each caller exactly the view the nested per-field
/// scopes showed, and restores the leaf on return and unwind.
#[test]
fn one_view_scope_equals_the_nested_field_scopes() {
    let cancellation = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let resource = root
        .native_volume_work(crate::WorkBudget::UNBOUNDED)
        .unwrap();
    let (invoked, _token) = resource.invocation_view().unwrap();
    let custody = CaptureTransientCustody::new_native(&invoked).unwrap();
    let captured = invoked.with_capture_custody(&custody).unwrap();
    let rechecked = captured.recheck_admitted();
    let idle = root.state.with_original_leaf(|state| seen(state));
    for work in [&root, &resource, &invoked, &captured, &rechecked] {
        for original in [false, true] {
            for unwind in [false, true] {
                let expected = nested_baseline(work, original, unwind);
                assert_eq!(expected.1, idle);
                assert_eq!(view_scope(work, original, unwind), expected);
            }
        }
    }
    // Negative control: without restoration the caller's view outlives the call.
    OMIT_RESTORE.with(|flag| flag.set(true));
    let leaked = view_scope(&captured, true, false);
    OMIT_RESTORE.with(|flag| flag.set(false));
    assert_ne!(leaked.1, idle);
    captured.state.with_original_leaf(|state| {
        state.native_invocation = None;
        state.native_resource = None;
        state.capture_custody = None;
    });
}
