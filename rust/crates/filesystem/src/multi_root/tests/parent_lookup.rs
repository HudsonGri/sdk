use super::*;

// Ownership checks may read only the participant's slots and their exact
// journal. Enumeration is deliberately unavailable through this adapter.
struct IndexedReads<S> {
    store: S,
    slots: std::sync::atomic::AtomicUsize,
    journals: std::sync::atomic::AtomicUsize,
}

impl<S: MultiRootPublicationStore> MultiRootPublicationStore for IndexedReads<S> {
    type Error = S::Error;

    async fn load(
        &self,
        operation: OperationId,
    ) -> Result<Option<MultiRootPublication>, Self::Error> {
        self.journals
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.store.load(operation).await
    }

    async fn parent_operation(
        &self,
        parent: WorkspaceContextId,
    ) -> Result<Option<OperationId>, Self::Error> {
        self.slots
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.store.parent_operation(parent).await
    }

    async fn list_operations(&self) -> Result<Vec<OperationId>, Self::Error> {
        panic!("a participant ownership check enumerated unrelated publications")
    }

    async fn compare_and_swap(
        &self,
        operation: OperationId,
        revision: u64,
        replacement: MultiRootPublication,
    ) -> Result<bool, Self::Error> {
        self.store
            .compare_and_swap(operation, revision, replacement)
            .await
    }

    async fn compare_and_delete(
        &self,
        operation: OperationId,
        revision: u64,
    ) -> Result<bool, Self::Error> {
        self.store.compare_and_delete(operation, revision).await
    }

    async fn claim_parent(
        &self,
        parent: WorkspaceContextId,
        operation: OperationId,
    ) -> Result<bool, Self::Error> {
        self.store.claim_parent(parent, operation).await
    }

    async fn release_parent(
        &self,
        parent: WorkspaceContextId,
        operation: OperationId,
    ) -> Result<bool, Self::Error> {
        self.store.release_parent(parent, operation).await
    }
}

async fn bounded_participant_lookup<S: MultiRootPublicationStore>(
    store: S,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = IndexedReads {
        store,
        slots: std::sync::atomic::AtomicUsize::new(0),
        journals: std::sync::atomic::AtomicUsize::new(0),
    };
    let plan = candidate().plan;
    assert!(
        store
            .claim_parent(plan.parent_context_id, plan.operation_id)
            .await?
    );
    assert!(
        context_has_publication(&store, plan.child_context_id, Some(plan.parent_context_id))
            .await?
    );
    assert_eq!(store.slots.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert_eq!(store.journals.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert!(
        parent_publication(&store, plan.parent_context_id)
            .await
            .is_err()
    );
    assert_eq!(store.slots.load(std::sync::atomic::Ordering::Relaxed), 3);
    assert_eq!(store.journals.load(std::sync::atomic::Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn memory_participant_lookup_does_not_visit_one_hundred_thousand_other_claims()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryMultiRootPublicationStore::default();
    for index in 1_000_u128..101_000 {
        assert!(
            store
                .claim_parent(
                    WorkspaceContextId::from_bytes(index.to_be_bytes()),
                    OperationId::from_bytes(index.to_be_bytes()),
                )
                .await?
        );
    }
    bounded_participant_lookup(store).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn durable_participant_lookup_requires_only_indexed_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    bounded_participant_lookup(crate::LocalCoreStateStore::open_owned(directory.path())?).await
}

async fn exact_parent_slot<S: MultiRootPublicationStore>(
    store: S,
) -> Result<(), Box<dyn std::error::Error>> {
    let coordinator = MultiRootPublicationCoordinator::new(store, Publisher::default(), Allow);
    let store = &coordinator.store;
    let candidate = candidate();
    let parent = candidate.plan.parent_context_id;
    let child = candidate.plan.child_context_id;
    let operation = candidate.plan.operation_id;
    if store.parent_operation(parent).await?.is_none() {
        assert!(!context_has_publication(store, child, Some(parent)).await?);
    }
    assert!(store.claim_parent(parent, operation).await?);
    // Negative control: journal enumeration sees no publication during this
    // crash cut, but the parent slot already owns both participants.
    assert!(store.list_operations().await?.is_empty());
    assert!(context_has_publication(store, parent, None).await?);
    assert!(context_has_publication(store, child, Some(parent)).await?);
    assert!(parent_publication(store, parent).await.is_err());
    assert!(
        store
            .release_parent(parent, OperationId::from_bytes([0xff; 16]))
            .await?
    );
    assert_eq!(store.parent_operation(parent).await?, Some(operation));
    let Publication::Applied(journal) = coordinator.publish_retained(candidate).await? else {
        return Err("publication did not apply".into());
    };
    assert_eq!(
        parent_publication(store, parent).await?,
        Some(journal.clone())
    );
    assert!(context_has_publication(store, child, Some(parent)).await?);
    assert!(
        !context_has_publication(
            store,
            WorkspaceContextId::from_bytes([33; 16]),
            Some(parent)
        )
        .await?
    );
    coordinator.acknowledge_applied(&journal).await?;
    assert!(!context_has_publication(store, parent, None).await?);
    assert!(!context_has_publication(store, child, Some(parent)).await?);
    Ok(())
}

#[tokio::test]
async fn memory_parent_lookup_preserves_unjournaled_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    exact_parent_slot(MemoryMultiRootPublicationStore::default()).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn durable_parent_lookup_preserves_unjournaled_ownership_across_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = crate::LocalCoreStateStore::open_owned(directory.path())?;
    let parent = WorkspaceContextId::from_bytes([8; 16]);
    let operation = OperationId::from_bytes([7; 16]);
    assert!(store.claim_parent(parent, operation).await?);
    drop(store);
    let store = crate::LocalCoreStateStore::open_owned(directory.path())?;
    assert_eq!(store.parent_operation(parent).await?, Some(operation));
    exact_parent_slot(store).await
}
