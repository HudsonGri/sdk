use super::*;

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
        assert!(
            !coordinator
                .context_has_publication(child, Some(parent))
                .await?
        );
    }
    assert!(store.claim_parent(parent, operation).await?);
    // Negative control: journal enumeration sees no publication during this
    // crash cut, but the parent slot already owns both participants.
    assert!(store.list_operations().await?.is_empty());
    assert!(coordinator.context_has_publication(parent, None).await?);
    assert!(
        coordinator
            .context_has_publication(child, Some(parent))
            .await?
    );
    assert!(coordinator.parent_publication(parent).await.is_err());
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
        coordinator.parent_publication(parent).await?,
        Some(journal.clone())
    );
    assert!(
        coordinator
            .context_has_publication(child, Some(parent))
            .await?
    );
    assert!(
        !coordinator
            .context_has_publication(WorkspaceContextId::from_bytes([33; 16]), Some(parent))
            .await?
    );
    coordinator.acknowledge_applied(&journal).await?;
    assert!(!coordinator.context_has_publication(parent, None).await?);
    assert!(
        !coordinator
            .context_has_publication(child, Some(parent))
            .await?
    );
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
