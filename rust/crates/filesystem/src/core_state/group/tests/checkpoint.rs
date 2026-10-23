//! A checkpoint must obey the same acknowledgement fence as an ordinary flush.
use super::*;
use acyclic_native_runtime::testing::{DurableFault, Schedule};

async fn put(store: &LocalCoreStateStore, agent: u8) -> Result<bool, LocalCoreStateStoreError> {
    let mut record = super::super::super::tests::crash_lineage();
    record.workspace_id = WorkspaceId::from_bytes([agent; 16]);
    WorkspaceLineageStore::compare_and_swap(store, record.workspace_id, 0, record).await
}

async fn pending(store: &LocalCoreStateStore) -> DeferredDurability {
    let (deferred, guard) = store.defer_durability();
    for agent in 1..=OWNED_CHECKPOINT {
        assert!(put(&deferred, u8::try_from(agent).unwrap()).await.unwrap());
    }
    guard
}

async fn scheduled_put(
    schedule: &Schedule,
    store: &LocalCoreStateStore,
) -> Result<bool, LocalCoreStateStoreError> {
    schedule.scope(put(store, 99)).await
}

#[tokio::test]
async fn checkpoint_barrier_failure_fences_cached_reads_until_reopen() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let probe = tempfile::tempdir().unwrap();
    let probe_owner = LocalCoreStateStore::open_owned(probe.path()).unwrap();
    let _pending = pending(&probe_owner).await;
    let (schedule, mut events) = Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    assert!(scheduled_put(&schedule, &probe_owner).await.unwrap());
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    let barrier = schedule
        .durable_writes()
        .iter()
        .find(|call| call.operation == "file:sync_data")
        .unwrap()
        .ordinal;
    for fault in [
        DurableFault::Before,
        DurableFault::After,
        DurableFault::Failed,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
        let guard = pending(&owner).await;
        let (schedule, mut events) = Schedule::new();
        schedule.cut_at(barrier, fault);
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        let result = scheduled_put(&schedule, &owner).await;
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        assert_eq!(
            schedule.storage_fault().unwrap().operation,
            "file:sync_data"
        );
        let LocalCoreStateStoreError::Io(cause) = result.unwrap_err() else {
            panic!("checkpoint erased the original physical barrier failure");
        };
        assert!(cause.get_ref().is_some_and(|cause| {
            cause.is::<acyclic_native_runtime::testing::InjectedStorageFault>()
        }));
        let read = WorkspaceLineageStore::load(&owner, WorkspaceId::from_bytes([1; 16])).await;
        assert!(
            matches!(read, Err(LocalCoreStateStoreError::DurabilityPoisoned)),
            "a checkpoint barrier failure must fence later reads until reopen: {read:?}"
        );
        drop((guard, owner));
        let reopened = LocalCoreStateStore::open_owned(directory.path()).unwrap();
        assert!(
            WorkspaceLineageStore::load(&reopened, WorkspaceId::from_bytes([99; 16]))
                .await
                .unwrap()
                .is_none(),
            "failed checkpoint must precede the next append"
        );
    }
}
