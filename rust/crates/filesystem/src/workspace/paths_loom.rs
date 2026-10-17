use super::*;
use crate::kernel::allocation::effects::OMIT_RESERVATION;
use crate::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;

// Both production callers copy exactly two bytes: "xy" and "/" + "x".
const COPIED: u64 = 2;
loom::thread_local! {
    static ACTUAL: RefCell<Option<Arc<AtomicU64>>> = RefCell::new(None);
}

pub(super) fn completed(copied: u64) {
    ACTUAL.with(|actual| {
        if let Some(actual) = &*actual.borrow() {
            actual.fetch_add(copied, Ordering::Relaxed);
            loom::thread::yield_now();
        }
    });
}

// Runs one real caller. Successful owners are retired after the join, so
// every allocation byte surviving beyond setup is a leak from a failed copy.
fn once<const OLD: bool>(
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
    label: Option<&WorkspaceName>,
    actual: &Arc<AtomicU64>,
) -> Option<crate::kernel::Owned<String>> {
    OMIT_RESERVATION.with(|flag| flag.set(OLD));
    ACTUAL.with(|slot| *slot.borrow_mut() = Some(actual.clone()));
    let mut accounting = KernelAccounting::native(work, cancellation);
    let copied = match label {
        Some(name) => copy_workspace_label(name, &mut accounting),
        None => restore_absolute_path("x", &mut accounting, cancellation),
    };
    ACTUAL.with(|slot| *slot.borrow_mut() = None);
    OMIT_RESERVATION.with(|flag| flag.set(false));
    let owned = copied.ok()?;
    assert_eq!(owned.value, if label.is_some() { "xy" } else { "/x" });
    Some(owned)
}

fn model<const OLD: bool, const CHECK_EFFECTS: bool>(
    copies: u64,
    callers: [bool; 2],
    depth: u8,
    recheck: bool,
    cancel: u8,
) {
    loom::model(move || {
        let root_cancel = CancellationToken::new();
        let caller_cancel = CancellationToken::new();
        let limit = crate::WorkBudget {
            bytes_copied: COPIED * copies,
            ..crate::WorkBudget::UNBOUNDED
        };
        let root = crate::NativeMaterializationWork::new(
            if depth == 0 {
                limit
            } else {
                crate::WorkBudget::UNBOUNDED
            },
            &root_cancel,
        )
        .unwrap();
        let mut work = root.clone();
        for _ in 0..depth {
            work = work.native_volume_work(limit).unwrap();
        }
        if recheck {
            work = work.recheck_admitted();
        }
        let setup = root.live_allocation_bytes();
        let name = WorkspaceName::new(
            "xy",
            &crate::WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .ok()
        .map(|name| name.value);
        let labels = callers.map(|restore| (!restore).then(|| name.clone().unwrap()));
        let actual = Arc::new(AtomicU64::new(0));
        let worker_work = work.clone();
        let worker_cancel = caller_cancel.clone();
        let worker_actual = actual.clone();
        let [worker_label, label] = labels;
        let worker = loom::thread::spawn(move || {
            once::<OLD>(
                &worker_work,
                &worker_cancel,
                worker_label.as_ref(),
                &worker_actual,
            )
        });
        match cancel {
            1 => caller_cancel.cancel(),
            2 => root_cancel.cancel(),
            _ => (),
        }
        let owned = once::<OLD>(&work, &caller_cancel, label.as_ref(), &actual);
        let owners = [worker.join().unwrap(), owned];
        let retiring = CancellationToken::new();
        let mut accounting = KernelAccounting::native(&work, &retiring);
        for owned in owners.into_iter().flatten() {
            owned.retire(&mut accounting).unwrap();
        }
        let copied = actual.load(Ordering::Relaxed);
        if CHECK_EFFECTS {
            assert!(
                copied <= limit.bytes_copied,
                "workspace text copies must be admitted before they execute"
            );
        }
        assert_eq!(root.receipt().bytes_copied, copied);
        let unreserved =
            root.state(|state| state.budget.bytes_copied == state.original_budget().bytes_copied);
        assert!(unreserved, "copy credit must not remain reserved");
        assert_eq!(
            root.live_allocation_bytes(),
            setup,
            "failed copies retire storage"
        );
        let remaining = work.state(|state| state.remaining());
        match limit.bytes_copied.checked_sub(copied) {
            Some(copy) => assert_eq!(remaining.unwrap().bytes_copied, copy),
            None => assert!(remaining.is_err()),
        }
    });
}

// Label and restore share one helper; one mixed pair covers both callers.
#[test]
fn concurrent_workspace_text_copies_cannot_share_remaining_credit() {
    for copies in 0..3 {
        model::<false, true>(copies, [false, true], 0, false, 0);
    }
    model::<false, true>(1, [true, true], 1, true, 0);
    model::<false, true>(1, [true, false], 0, false, 1);
    model::<false, true>(1, [false, false], 0, false, 2);
}

fn message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("")
}

#[test]
fn old_check_only_copy_admission_fails_the_real_effect_assertion_and_needs_it() {
    let panic = std::panic::catch_unwind(|| model::<true, true>(1, [false, true], 0, false, 0))
        .expect_err("check-only admission must let both copies spend one allowance");
    assert!(
        message(&*panic).contains("workspace text copies must be admitted before they execute"),
        "{}",
        message(&*panic)
    );
    // Delete only the effect-limit assertion: the old path still records
    // every completed byte and retires storage, so the variant passes.
    model::<true, false>(1, [false, true], 0, false, 0);
}
