use super::*;
use crate::sync::atomic::{AtomicUsize, Ordering};
use std::cell::Cell;
use std::sync::Arc;

loom::thread_local! {
    static OMIT_RESERVATION: Cell<bool> = Cell::new(false);
}
pub(super) fn broken_reservation() -> bool {
    OMIT_RESERVATION.with(Cell::get)
}

fn copy_once<const BROKEN: bool>(
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    allocation: KernelOwnedAllocation,
    actual: &AtomicUsize,
) {
    OMIT_RESERVATION.with(|flag| flag.set(BROKEN));
    let mut accounting = KernelAccounting::native(work, cancellation);
    let mut observed = OriginalReadAccounting {
        accounting: &mut accounting,
        allocation,
        owned_bytes: 0,
        pending: WorkCounters::UNCHARGED,
    };
    if observed
        .observe(super::super::super::codec::ReadAllocationStep::BeforeCopy(
            8,
        ))
        .is_ok()
    {
        let mut output = [0; 8];
        output.copy_from_slice(std::hint::black_box(b"contents"));
        assert_eq!(&std::hint::black_box(output), b"contents");
        actual.fetch_add(8, Ordering::Relaxed);
        let _ = observed.observe(super::super::super::codec::ReadAllocationStep::Copied(8));
    }
    let (allocation, bytes) = observed.into_parts();
    assert_eq!(bytes, 0);
    drop(allocation);
}

fn copy_model_impl<const BROKEN: bool, const CHECK_LIMIT: bool>(
    maximum: u64,
    cancel: bool,
    resource: bool,
) {
    loom::model(move || {
        let cancellation = crate::CancellationToken::new();
        let limit = WorkBudget {
            bytes_copied: maximum,
            ..WorkBudget::UNBOUNDED
        };
        let original = crate::NativeMaterializationWork::new(
            if resource {
                WorkBudget::UNBOUNDED
            } else {
                limit
            },
            &cancellation,
        )
        .unwrap();
        let work = if resource {
            original.native_volume_work(limit).unwrap()
        } else {
            original.clone()
        };
        let worker_allocation = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let local_allocation = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let baseline = original.live_allocation_bytes();
        // Standard Arc transports storage kept alive through join. Original
        // leaf/resource locks, cancellation and the witness use crate::sync.
        let actual = Arc::new(AtomicUsize::new(0));
        let worker_work = work.clone();
        let worker_cancel = cancellation.clone();
        let worker_actual = actual.clone();
        let worker = loom::thread::spawn(move || {
            copy_once::<BROKEN>(
                &worker_work,
                &worker_cancel,
                KernelOwnedAllocation::NativeOriginal(worker_allocation),
                &worker_actual,
            )
        });
        if cancel {
            cancellation.cancel();
            drop(local_allocation);
        } else {
            copy_once::<BROKEN>(
                &work,
                &cancellation,
                KernelOwnedAllocation::NativeOriginal(local_allocation),
                &actual,
            );
        }
        worker.join().unwrap();
        let completed = actual.load(Ordering::Relaxed) as u64;
        if CHECK_LIMIT {
            assert!(completed <= maximum, "a rejected copy must not execute");
        }
        assert_eq!(
            original.receipt().bytes_copied,
            completed,
            "every completed copy stays charged"
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
    });
}
fn copy_model<const BROKEN: bool>(maximum: u64, cancel: bool) {
    copy_model_impl::<BROKEN, true>(maximum, cancel, true);
}

#[test]
fn competing_copies_reserve_shared_allowance_before_the_effect() {
    copy_model::<false>(0, false);
    copy_model::<false>(8, false);
    copy_model::<false>(16, false);
    copy_model::<false>(8, true);
    copy_model_impl::<false, true>(8, false, false);
}

#[test]
fn missing_reservation_and_deleting_the_limit_assertion_are_detected() {
    let rejected = std::panic::catch_unwind(|| copy_model::<true>(8, false));
    let panic = rejected.expect_err("the real limit assertion must reject the broken reservation");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("a rejected copy must not execute"),
        "{message}"
    );
    let assertion_deleted =
        std::panic::catch_unwind(|| copy_model_impl::<true, false>(8, false, true));
    assert!(
        assertion_deleted.is_ok(),
        "without the limit assertion the same broken model must pass"
    );
}
