//! The original native cancellation wakes a blocked record acquisition.
use super::*;

#[test]
fn original_cancellation_interrupts_a_record_lock_without_polling_or_retained_storage() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lock");
    let open = || {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap()
    };
    let holder = FileLock::acquire(open(), FileLockMode::Exclusive).unwrap();
    let contender = FileLock::new(open());
    let cancellation = crate::CancellationToken::new();
    let budget = super::native::finite_budget();
    let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let ledger = original.live_allocation_bytes();
    let work = original.clone();
    let worker = std::thread::spawn(move || {
        let error = wait_record_lock(
            &contender,
            LockMode::Exclusive,
            Instant::now() + RECORD_LOCK_DEADLINE,
            Some(&work),
        )
        .unwrap_err();
        assert!(!contender.is_held());
        assert!(
            matches!(
                error,
                LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::Cancelled)
            ),
            "original cancellation must survive IO translation: {error:?}"
        );
    });
    // One initial lock probe and one notification open precede registration;
    // the next read is admitted only after the original waiter is registered.
    let deadline = Instant::now() + Duration::from_secs(2);
    while original.receipt().backend_read_operations < 3 {
        assert!(Instant::now() < deadline, "record waiter did not register");
        std::thread::yield_now();
    }
    cancellation.cancel();
    worker.join().unwrap();
    assert_eq!(original.live_allocation_bytes(), ledger);
    original.receipt().verify(budget).unwrap();
    holder.unlock().unwrap();
    FileLock::try_acquire(open(), FileLockMode::Exclusive).unwrap();
}
