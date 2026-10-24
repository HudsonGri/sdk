use super::*;
use std::sync::atomic::Ordering;

enum HostState<T> {
    Idle,
    Running(T),
}

#[test]
fn process_storage_is_small_and_matches_the_original_heap_credit() {
    const CEILING: usize = 64;
    let before = std::mem::size_of::<HostState<ProcessTree>>();
    let after = std::mem::size_of::<HostState<ProcessTreeStorage>>();
    println!("process host state: {before} -> {after} bytes");
    assert!(after <= CEILING);
    assert!(
        before > CEILING,
        "the actual inline owner is the negative control"
    );

    let original = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut state = HostState::Idle;
    let physical = acyclic_test_support::measure(|| {
        state = HostState::Running(ProcessTreeStorage::new(&observer).unwrap());
    });
    assert_eq!(
        u64::try_from(physical.bytes_current).unwrap(),
        original.memory.load(Ordering::Acquire)
    );
    assert_eq!(
        usize::try_from(physical.bytes_current).unwrap(),
        std::mem::size_of::<Option<ProcessTree>>()
    );
    let HostState::Running(slot) = &state else {
        panic!("missing funded slot")
    };
    assert!(slot.is_reaped());
    drop(state);
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
}

#[test]
fn denied_process_storage_never_reaches_an_allocator_or_spawn() {
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    original.deny_allocations.store(true, Ordering::Release);
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let physical = acyclic_test_support::measure(|| {
        assert!(ProcessTreeStorage::new(&observer).is_err());
    });
    assert_eq!(physical.bytes_total, 0);
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
    assert_eq!(original.used_operations.load(Ordering::Acquire), 0);
}

#[test]
fn malformed_private_storage_cannot_acknowledge_quiescence_or_spawn() {
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut storage = ProcessTreeStorage::new(&observer).unwrap();
    // Public operations cannot resize the slot. Exercise the guard against a
    // future internal change, rather than allowing an invalid owner to appear idle.
    storage.slot.data.clear();
    assert!(!storage.is_reaped());
    assert_eq!(
        storage.terminate().unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    let mut command = Command::new("must-not-start");
    assert_eq!(
        storage.spawn(&mut command, None).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(original.used_operations.load(Ordering::Acquire), 0);
}

#[test]
fn failed_spawn_keeps_the_partial_tree_and_its_storage_on_the_original_owner() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    // On Windows this fails the physical record's recording; on Unix it
    // fails the armed spawn's recording. Both return an owned partial slot.
    meter.fail_recorded_operation = Some(if cfg!(windows) { 1 } else { 2 });
    let original = Arc::new(meter);
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut slot = ProcessTreeStorage::new(&observer).unwrap();
    let retained = original.memory.load(Ordering::Acquire);
    let mut command = super::super::tests::command("exit-code", root.path());
    assert!(slot.spawn(&mut command, Some(&custody)).is_err());
    assert!(!slot.is_reaped());
    assert_eq!(original.memory.load(Ordering::Acquire), retained);
    let before_cleanup = original.used_operations.load(Ordering::Acquire);
    slot.terminate().unwrap();
    assert!(slot.is_reaped());
    assert!(original.used_operations.load(Ordering::Acquire) > before_cleanup);
    assert_eq!(
        slot.spawn(&mut command, Some(&custody)).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists,
        "retired storage cannot silently become another execution"
    );
    assert_eq!(original.memory.load(Ordering::Acquire), retained);
    drop(slot);
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
}
