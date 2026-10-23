//! Real contention, cancellation and deadline proofs for the kernel path.
use super::*;
use std::fs::OpenOptions;
use std::sync::mpsc;
use std::time::Duration;

fn open(path: &std::path::Path) -> File {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .unwrap()
}

#[test]
fn quiet_contention_has_no_periodic_lock_probes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lock");
    let holder = FileLock::acquire(open(&path), FileLockMode::Exclusive).unwrap();
    let contender = FileLock::new(open(&path));
    let budget = Arc::new(crate::tests::ReadBudgetMeter::new(100, 4096));
    let meter: Arc<dyn NativeReadMeter> = budget.clone();
    let (waiting, started) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        WAITING.with(|slot| *slot.borrow_mut() = Some(waiting));
        let began = Instant::now();
        contender
            .lock_until(
                FileLockMode::Exclusive,
                began + Duration::from_secs(2),
                None,
                Some(&meter),
            )
            .unwrap();
        assert!(contender.is_held());
        contender.unlock().unwrap();
        began.elapsed()
    });
    started.recv_timeout(Duration::from_secs(1)).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    holder.unlock().unwrap();
    let elapsed = worker.join().unwrap();
    let reads = budget.read_attempts.load(Ordering::Acquire);
    eprintln!(
        "RECORD_LOCK_KERNEL held_ms=200 reads={reads} elapsed_us={}",
        elapsed.as_micros()
    );
    assert!(
        reads <= 12,
        "a quiet held lock must sleep in the kernel, without periodic probes"
    );
    assert_eq!(
        budget.memory.load(Ordering::Acquire),
        0,
        "all wait storage must retire on the original ledger"
    );
    FileLock::try_acquire(open(&path), FileLockMode::Exclusive).unwrap();
}

#[test]
fn cancellation_and_timeout_leave_no_accepted_acquisition() {
    for cancel in [false, true] {
        for mode in [FileLockMode::Shared, FileLockMode::Exclusive] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("lock");
            let holder = FileLock::acquire(open(&path), FileLockMode::Exclusive).unwrap();
            let contender = FileLock::new(open(&path));
            let signal = FileLockCancellation::new(None).unwrap();
            let wake = std::task::Waker::from(Arc::clone(&signal));
            let (waiting, started) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                WAITING.with(|slot| *slot.borrow_mut() = Some(waiting));
                let timeout = if cancel {
                    Duration::from_secs(2)
                } else {
                    Duration::from_millis(200)
                };
                let error = contender
                    .lock_until(mode, Instant::now() + timeout, Some(&signal), None)
                    .unwrap_err();
                assert!(!contender.is_held());
                error.kind()
            });
            started.recv_timeout(Duration::from_secs(1)).unwrap();
            if cancel {
                wake.wake();
            }
            assert_eq!(
                worker.join().unwrap(),
                if cancel {
                    io::ErrorKind::Interrupted
                } else {
                    io::ErrorKind::TimedOut
                }
            );
            holder.unlock().unwrap();
            let probe = FileLock::try_acquire(open(&path), FileLockMode::Exclusive)
                .expect("a completed unsuccessful wait must never acquire after return");
            assert!(probe.is_held());
        }
    }
}

#[test]
fn refused_wait_storage_keeps_the_original_lock_and_releases_no_foreign_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lock");
    let holder = FileLock::acquire(open(&path), FileLockMode::Exclusive).unwrap();
    let contender = FileLock::new(open(&path));
    let budget = Arc::new(crate::tests::ReadBudgetMeter::new(100, 4096));
    budget.deny_allocations.store(true, Ordering::Release);
    let meter: Arc<dyn NativeReadMeter> = budget.clone();
    assert!(
        contender
            .lock_until(
                FileLockMode::Exclusive,
                Instant::now() + Duration::from_secs(1),
                None,
                Some(&meter)
            )
            .is_err()
    );
    assert!(!contender.is_held());
    assert!(holder.is_held());
    assert_eq!(
        FileLock::try_acquire(open(&path), FileLockMode::Exclusive)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    holder.unlock().unwrap();
    FileLock::try_acquire(open(&path), FileLockMode::Exclusive).unwrap();
    assert_eq!(budget.memory.load(Ordering::Acquire), 0);
}
