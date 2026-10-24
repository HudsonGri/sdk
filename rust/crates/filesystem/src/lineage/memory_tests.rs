use super::*;
use crate::facade::FsWork as _;
use futures::FutureExt as _;
use proptest::prelude::*;

fn record(id: u8, length: usize, revision: u64) -> WorkspaceLineageRecord {
    WorkspaceLineageRecord {
        version: LINEAGE_VERSION,
        revision,
        ready: true,
        workspace_id: WorkspaceId::from_bytes([id; 16]),
        workspace_name: "x".repeat(length),
        parent_workspace_id: None,
        parent_workspace_name: None,
        creation_operation: None,
        fork_generation: GenerationId::new(crate::Digest::from_bytes([1; 32])),
        initial_generation: GenerationId::new(crate::Digest::from_bytes([1; 32])),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn every_node_and_string_matches_real_heap_and_its_original_owner(
        steps in prop::collection::vec((0u8..16, 1usize..96, any::<bool>()), 1..96),
    ) {
        let cancellation = CancellationToken::new();
        let a = crate::NativeMaterializationWork::new(
            crate::performance::work_budget(100_000, 1 << 20), &cancellation).unwrap();
        let b = crate::NativeMaterializationWork::new(
            crate::performance::work_budget(100_000, 1 << 20), &cancellation).unwrap();
        let baseline = [a.live_allocation_bytes(), b.live_allocation_bytes()];
        let mut expected = std::collections::BTreeMap::<u8, (u64, bool, u64)>::new();
        let store = MemoryWorkspaceLineageStore::new();
        for (id, length, second) in steps {
            let revision = expected.get(&id).map_or(0, |entry| entry.0);
            let value = record(id, length, revision + 1);
            let work = if second { &b } else { &a };
            let mut accounting = work.accounting(&cancellation);
            let before = [a.live_allocation_bytes(), b.live_allocation_bytes()];
            let mut result = None;
            let physical = acyclic_test_support::measure(|| {
                result = Some(store.compare_accounted(value.workspace_id, revision, &value,
                    &mut accounting).now_or_never().expect("memory transition is synchronous"));
            });
            prop_assert!(result.unwrap().unwrap());
            let after = [a.live_allocation_bytes(), b.live_allocation_bytes()];
            let actual_delta = i128::from(after[0]) + i128::from(after[1])
                - i128::from(before[0]) - i128::from(before[1]);
            prop_assert_eq!(actual_delta, i128::from(physical.bytes_current));
            expected.insert(id, (revision + 1, second,
                length as u64 + MemoryWorkspaceLineageStore::node_bytes()));
            let mut retained = [0, 0];
            for (_, owner, bytes) in expected.values() {
                retained[usize::from(*owner)] += bytes;
            }
            prop_assert_eq!(after, [baseline[0] + retained[0], baseline[1] + retained[1]]);
        }
        drop(store);
        prop_assert_eq!([a.live_allocation_bytes(), b.live_allocation_bytes()], baseline);
    }
}

#[test]
fn funding_only_strings_cannot_publish_an_unfunded_index_node() {
    let cancellation = CancellationToken::new();
    let value = record(1, 17, 1);
    let mut budget = crate::performance::work_budget(256, 64 * 1024);
    budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes() + 17;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let baseline = original.live_allocation_bytes();
    let original_work = &original;
    let mut accounting = original_work.accounting(&cancellation);
    let store = MemoryWorkspaceLineageStore::new();
    assert!(
        store
            .compare_accounted(value.workspace_id, 0, &value, &mut accounting)
            .now_or_never()
            .unwrap()
            .is_err()
    );
    assert!(store.records.lock().unwrap().is_none());
    assert_eq!(original.live_allocation_bytes(), baseline);

    // Actual former storage shape: copied strings are funded, opaque map
    // nodes are allocated outside that claim, and publication succeeds.
    let control = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let control_baseline = control.live_allocation_bytes();
    let control_work = &control;
    let mut accounting = control_work.accounting(&cancellation);
    let mut old = std::collections::BTreeMap::new();
    let mut copied = None;
    let physical = acyclic_test_support::measure(|| {
        copied =
            Some(work::copy::<MemoryWorkspaceLineageStoreError>(&value, &mut accounting).unwrap());
        old.insert(value.workspace_id, copied.take().unwrap());
    });
    assert!(physical.bytes_current > 17);
    assert_eq!(control.live_allocation_bytes(), control_baseline + 17);
    drop(old);
    assert_eq!(control.live_allocation_bytes(), control_baseline);
}
