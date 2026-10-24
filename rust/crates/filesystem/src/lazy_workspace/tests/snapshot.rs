use super::*;

#[tokio::test]
async fn graph_lazy_creator_rejects_another_cut_before_creating_a_child() {
    use crate::WorkspaceLineageStore as _;
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let old = root.snapshot(accounting!()).await.unwrap();
    root.workspace()
        .write_text("/new", "new cut")
        .await
        .unwrap();
    let current = root.snapshot(accounting!()).await.unwrap();
    assert_ne!(old.authored_generation, current.authored_generation);
    let graph = crate::WorkspaceGraph::new(crate::MemoryWorkspaceLineageStore::new());
    graph
        .register_root(
            root.workspace(),
            &mut crate::kernel::KernelAccounting::standalone(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            ),
        )
        .await
        .unwrap();
    let destination = "graph-lazy-cut";
    let key = IdempotencyKey::from_bytes([91; 16]);
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let fork = async |cut, accounting: &mut KernelAccounting<'_>| {
        graph
            .fork::<_, _, _, Box<dyn std::error::Error + Send + Sync>, _>(
                root.workspace(),
                destination,
                key,
                Some(current.authored_generation),
                accounting,
                crate::lineage::LazyFork {
                    parent: &root,

                    cut,

                    permit: crate::PublicationPermit::Unrestricted,
                },
                |result| result.0.workspace(),
            )
            .await
    };
    // The former creator ignored the graph's authenticated generation, created
    // a child at `old`, and only failed when graph registration checked it.
    let failure = fork(old, &mut accounting)
        .await
        .err()
        .expect("mismatched cut must fail");
    assert!(matches!(
        failure.downcast_ref::<LazyWorkspaceError>(),
        Some(LazyWorkspaceError::OriginalSnapshotUnavailable)
    ));
    assert!(
        fs.open_workspace(destination, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .is_err()
    );
    let child_id = fs.workspace_id(destination).unwrap();
    assert!(
        root.store
            .load_lazy_workspace(child_id, &mut accounting)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!graph.store().load(child_id).await.unwrap().unwrap().ready);
    let recovered = fork(current, &mut accounting).await.unwrap();
    assert_eq!(
        graph.resolve(child_id).await.unwrap().fork_generation,
        current.authored_generation
    );
    assert_eq!(recovered.0.workspace().id(), child_id);
}

#[tokio::test]
async fn snapshot_validation_requires_available_indexes_and_generation() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let cut = root.snapshot(accounting!()).await.unwrap();
    root.validate_snapshot(cut, accounting!()).await.unwrap();
    for (generation, overlay, shadows) in [
        (
            cut.authored_generation,
            LazyOverlayId::from_bytes([0xa1; 32]),
            cut.shadows,
        ),
        (
            cut.authored_generation,
            cut.overlay,
            LazyShadowId::from_bytes([0xa2; 32]),
        ),
        (
            crate::GenerationId::new(crate::Digest::from_bytes([0xa3; 32])),
            cut.overlay,
            cut.shadows,
        ),
    ] {
        let missing =
            LazySnapshotRef::new(cut.workspace_id, generation, cut.source, overlay, shadows);
        assert!(
            root.validate_snapshot(missing, accounting!())
                .await
                .is_err()
        );
        assert_eq!(root.snapshot(accounting!()).await.unwrap(), cut);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn frozen_views_and_write_replay_keep_the_original_generation() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    root.workspace()
        .write_text("/authored.txt", "before")
        .await
        .unwrap();
    let basis = root.snapshot(accounting!()).await.unwrap();
    let key = IdempotencyKey::new();
    let publish = async |bytes| {
        root.write_at_snapshot::<_, _, Box<dyn std::error::Error + Send + Sync>>(
            basis,
            std::iter::once((
                "/new.txt",
                Some(acyclic_stream::OwnedBytes::from_static(bytes)),
            )),
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!(),
            &mut (),
        )
        .await
        .unwrap()
    };
    let (commit, published) = publish(b"published").await;
    assert!(matches!(commit, TransactionCommit::Committed(_)));
    let published = published.unwrap();
    root.workspace()
        .write_text("/authored.txt", "after")
        .await
        .unwrap();
    root.workspace()
        .write_text("/later.txt", "later")
        .await
        .unwrap();
    root.workspace()
        .write_text("/new.txt", "later")
        .await
        .unwrap();
    let (bytes, source) = root
        .read_at_snapshot(basis, "/authored.txt", None, 32, accounting!())
        .await
        .unwrap();
    assert_eq!(bytes.bytes.as_slice(), b"before");
    assert!(!source);
    let mut cursor = None;
    let mut authored = Vec::new();
    loop {
        let (page, _, _) = root
            .list_directory_at_snapshot(basis, "/", cursor, 16, accounting!())
            .await
            .unwrap();
        authored.extend(
            page.entries
                .iter()
                .filter(|e| e.authored)
                .map(|e| e.name.as_bytes().to_vec()),
        );
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(authored, [b"authored.txt".to_vec()]);
    let (commit, replayed) = publish(b"published").await;
    assert!(matches!(commit, TransactionCommit::AlreadyCommitted(_)));
    assert_eq!(replayed.unwrap(), published);
    assert_eq!(
        root.read_at_snapshot(published, "/new.txt", None, 32, accounting!())
            .await
            .unwrap()
            .0
            .bytes
            .as_slice(),
        b"published"
    );
    let (commit, proof) = publish(b"changed").await;
    assert!(matches!(commit, TransactionCommit::AlreadyCommitted(_)));
    assert!(matches!(
        proof.err().unwrap().downcast_ref::<LazyWorkspaceError>(),
        Some(LazyWorkspaceError::StaleIdentity)
    ));
}
