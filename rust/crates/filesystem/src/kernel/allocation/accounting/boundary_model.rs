use super::*;
use std::cell::Cell;

loom::thread_local! {
    static DROP_UNDER_LEAF: Cell<bool> = Cell::new(false);
}
pub(crate) fn drop_under_leaf() -> bool {
    DROP_UNDER_LEAF.with(Cell::get)
}

fn scopes(root: &crate::NativeMaterializationWork, depth: u8) -> crate::NativeMaterializationWork {
    let mut work = root.clone();
    for _ in 0..depth {
        work = work
            .native_volume_work(WorkBudget {
                bytes_copied: 2,
                items_examined: 2,
                peak_allocation_bytes: 1,
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
    }
    work
}

fn model<const BROKEN: bool>(depth: u8, cancel: bool) {
    loom::model(move || {
        let root_cancel = crate::CancellationToken::new();
        let caller = crate::CancellationToken::new();
        let probe =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &root_cancel).unwrap();
        let probe_work = scopes(&probe, depth);
        let setup = probe.live_allocation_bytes();
        let setup_items = probe.receipt().items_examined;
        drop(probe_work);
        drop(probe);
        let root = crate::NativeMaterializationWork::new(
            WorkBudget {
                bytes_copied: 2,
                items_examined: setup_items + 2,
                peak_allocation_bytes: setup + 1,
                ..WorkBudget::UNBOUNDED
            },
            &root_cancel,
        )
        .unwrap();
        let work = scopes(&root, depth);
        let initial = root.receipt();
        let worker_work = work.clone();
        let worker_caller = caller.clone();
        let (ready, receiver) = loom::sync::mpsc::channel();
        let (release, released) = loom::sync::mpsc::channel();
        let worker = loom::thread::spawn(move || {
            let mut accounting = KernelAccounting::native(&worker_work, &worker_caller);
            DROP_UNDER_LEAF.with(|flag| flag.set(BROKEN));
            let owner = accounting.claim_owned(1, 1);
            DROP_UNDER_LEAF.with(|flag| flag.set(false));
            ready.send(owner.is_ok()).unwrap();
            let _ = accounting.record_completed(WorkCounters {
                bytes_copied: 1,
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            });
            released.recv().unwrap();
            // Guards leave the accounting leaf before owner destruction.
            drop(owner);
        });
        let worker_claimed = receiver.recv().unwrap();
        if cancel {
            caller.cancel();
        }
        let mut accounting = KernelAccounting::native(&work, &caller);
        let owner = accounting.claim_owned(1, 1);
        assert!(usize::from(worker_claimed) + usize::from(owner.is_ok()) <= 1);
        if let Ok(remaining) = accounting.remaining() {
            assert_eq!(
                remaining.bytes_copied, remaining.items_examined,
                "one coherent snapshot"
            );
        }
        let completed = accounting.record_completed(WorkCounters {
            bytes_copied: 1,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        });
        assert_eq!(completed.is_err(), cancel);
        drop(owner);
        release.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(root.live_allocation_bytes(), setup);
        assert_eq!(root.receipt().bytes_copied, initial.bytes_copied + 2);
        assert_eq!(root.receipt().items_examined, initial.items_examined + 2);
    });
}

#[test]
fn boundaries_keep_storage_exclusive_snapshots_coherent_and_completed_work_recorded() {
    for depth in [0, 2] {
        for cancel in [false, true] {
            model::<false>(depth, cancel);
        }
    }
}

#[test]
fn dropping_the_real_claim_owner_under_the_leaf_is_detected() {
    let panic = std::panic::catch_unwind(|| {
        loom::model(|| {
            let caller = crate::CancellationToken::new();
            let work =
                crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &caller).unwrap();
            DROP_UNDER_LEAF.with(|flag| flag.set(true));
            let _ = KernelAccounting::native(&work, &caller).claim_owned(1, 1);
        })
    })
    .expect_err("owner destruction re-enters the leaf and must be rejected");
    let text = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        text.contains("deadlock") || text.contains("lock order") || text.contains("reentrant"),
        "{text}"
    );
}
