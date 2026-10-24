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
    assert!(slot.is_none());
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
    assert!(
        ProcessTree::spawn_in_custody_controlled(
            &mut command,
            &custody,
            Some(observer.as_ref()),
            &mut slot,
        )
        .is_err()
    );
    let tree = slot
        .as_mut()
        .expect("the partial tree remains in its funded slot");
    assert!(!tree.is_reaped());
    assert_eq!(original.memory.load(Ordering::Acquire), retained);
    let recovery = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    tree.terminate(Some(&recovery)).unwrap();
    assert!(tree.is_reaped());
    assert_eq!(original.memory.load(Ordering::Acquire), retained);
    drop(slot);
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
}
