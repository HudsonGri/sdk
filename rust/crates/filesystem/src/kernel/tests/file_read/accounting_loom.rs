use super::*;
use crate::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn copy_once<const BYPASS_ADMISSION: bool>(
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
    allocation: KernelOwnedAllocation,
    actual: &AtomicUsize,
) {
    let mut accounting = KernelAccounting::native(work, cancellation);
    // Constructor's empty guard was created before spawning, avoiding setup
    // interleavings unrelated to the production observe path under test.
    let mut observed = OriginalReadAccounting {
        accounting: &mut accounting,
        allocation,
        owned_bytes: 0,
        pending: WorkCounters::UNCHARGED,
    };
    if BYPASS_ADMISSION || observed.observe(ReadAllocationStep::BeforeCopy(8)).is_ok() {
        let mut output = [0; 8];
        output.copy_from_slice(b"contents");
        assert_eq!(&output, b"contents");
        actual.fetch_add(8, Ordering::Relaxed);
        let _ = observed.observe(ReadAllocationStep::Copied(8));
    }
    let (allocation, bytes) = observed.into_parts();
    assert_eq!(bytes, 0);
    drop(allocation);
}

fn copy_model<const BYPASS_ADMISSION: bool>(maximum: u64, cancel: bool) {
    loom::model(move || {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let work = original
            .native_volume_work(WorkBudget {
                bytes_copied: maximum,
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
        let worker_allocation = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let local_allocation = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let baseline = original.live_allocation_bytes();
        // Arc is transport only: root/resource owners remain live through join.
        // All cross-thread state under test uses production crate::sync locks
        // and atomics; the effect witness also uses the modeled atomic backend.
        let actual = Arc::new(AtomicUsize::new(0));
        let worker_work = work.clone();
        let worker_cancellation = cancellation.clone();
        let worker_actual = actual.clone();
        let worker = loom::thread::spawn(move || {
            copy_once::<BYPASS_ADMISSION>(
                &worker_work,
                &worker_cancellation,
                KernelOwnedAllocation::NativeOriginal(worker_allocation),
                &worker_actual,
            )
        });
        if cancel {
            cancellation.cancel();
            drop(local_allocation);
        } else {
            copy_once::<BYPASS_ADMISSION>(
                &work,
                &cancellation,
                KernelOwnedAllocation::NativeOriginal(local_allocation),
                &actual,
            );
        }
        worker.join().unwrap();
        let completed = actual.load(Ordering::Relaxed) as u64;
        assert!(completed <= maximum, "a rejected copy must not execute");
        assert_eq!(
            original.receipt().bytes_copied,
            completed,
            "every completed copy stays charged"
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
    });
}

#[test]
fn shared_decoder_admissions_and_completion_keep_every_effect_charged() {
    copy_model::<false>(8, false);
    copy_model::<false>(16, false);
    copy_model::<false>(0, false);
    copy_model::<false>(8, true);
}

#[test]
#[should_panic(expected = "a rejected copy must not execute")]
fn bypassed_admission_is_detected_by_the_real_effect_witness() {
    copy_model::<true>(0, false);
}
