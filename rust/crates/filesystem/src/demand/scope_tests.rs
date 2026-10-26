use super::accounting::NativeScope;
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
    work.native_scope.recheck_items = original;
    let result = catch_unwind(AssertUnwindSafe(|| {
        nested::<ASSERT>(&mut work, frames, unwind)
    }));
    assert_policy::<ASSERT>(work.native_scope.recheck_items, original);
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
    let original = work.native_scope.recheck_items;
    let result = catch_unwind(AssertUnwindSafe(|| {
        work.scope(
            |work| &mut work.native_scope.recheck_items,
            policy,
            |work| {
                assert_policy::<ASSERT>(work.native_scope.recheck_items, policy);
                work.work.bytes_copied += u64::from(bytes);
                nested::<ASSERT>(work, remaining, unwind);
                assert_policy::<ASSERT>(work.native_scope.recheck_items, policy);
            },
        )
    }));
    assert_policy::<ASSERT>(work.native_scope.recheck_items, original);
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

fn native_scope(mask: u8) -> NativeScope {
    NativeScope {
        native_resource: (mask & 1 != 0).then(std::sync::Weak::new),
        native_invocation: (mask & 2 != 0).then(std::sync::Weak::new),
        capture_custody: (mask & 4 != 0).then(std::sync::Weak::new),
        recheck_items: mask & 8 != 0,
    }
}
fn scope_mask(scope: &NativeScope) -> u8 {
    u8::from(scope.native_resource.is_some())
        | (u8::from(scope.native_invocation.is_some()) << 1)
        | (u8::from(scope.capture_custody.is_some()) << 2)
        | (u8::from(scope.recheck_items) << 3)
}

#[test]
fn complete_native_scope_restores_on_return_and_unwind() {
    for original in 0..16 {
        for inner in 0..16 {
            for unwind in [false, true] {
                let mut work = DemandWork::new(
                    crate::performance::BOUNDARY_BUDGET,
                    &CancellationToken::new(),
                );
                work.native_scope = native_scope(original);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    work.scope(
                        |work| &mut work.native_scope,
                        native_scope(inner),
                        |work| {
                            assert_eq!(scope_mask(&work.native_scope), inner);
                            work.work.bytes_copied += 7;
                            if unwind {
                                std::panic::panic_any(());
                            }
                        },
                    );
                }));
                assert_eq!(result.is_err(), unwind);
                assert_eq!(scope_mask(&work.native_scope), original);
                assert_eq!(work.work.bytes_copied, 7);
            }
        }
    }
}
