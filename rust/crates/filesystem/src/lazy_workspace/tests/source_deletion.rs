use super::*;

#[tokio::test]
async fn source_deletion_keeps_new_source_and_authored_versions() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"original")));
    let parent = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let (child, _) = parent
        .fork(
            "child",
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    child
        .remove("/file.txt", None, accounting!())
        .await
        .unwrap();
    let (tombstones, _) = child.source_tombstones(1, accounting!()).await.unwrap();
    let (path, deleted) = &tombstones[0];
    let head = child.head(accounting!()).await.unwrap();
    let eligible = async || {
        child
            .source_deletion_is_unmodified(path, *deleted, &head, Some(&parent), accounting!())
            .await
            .unwrap()
    };
    assert!(eligible().await);
    let old_cut = child.state(accounting!()).await.unwrap().source;
    source.replace(Bytes::from_static(b"ancestor changed the source"));
    assert!(!eligible().await);
    // A retained cut can still read the deleted version, but the shared binding
    // follows the live epoch and must reject it for deletion eligibility.
    assert_eq!(
        child
            .source_lookup(old_cut, path, accounting!())
            .await
            .unwrap()
            .unwrap()
            .version,
        *deleted
    );
    source.replace(Bytes::from_static(b"original"));
    assert!(eligible().await);
    parent
        .workspace()
        .write_text(path, "parent authored replacement")
        .await
        .unwrap();
    assert!(!eligible().await);
    child
        .workspace()
        .write_text(path, "child recreated it")
        .await
        .unwrap();
    let recreated = child.head(accounting!()).await.unwrap();
    assert!(
        !child
            .source_deletion_is_unmodified(path, *deleted, &recreated, Some(&parent), accounting!())
            .await
            .unwrap()
    );
    let foreign = parent.head(accounting!()).await.unwrap();
    assert!(matches!(
        child
            .source_deletion_is_unmodified(path, *deleted, &foreign, Some(&parent), accounting!())
            .await,
        Err(LazyWorkspaceError::WorkspaceCause(
            WorkspaceError::ForeignGeneration
        ))
    ));
}

#[tokio::test]
async fn source_deletion_failed_generation_read_never_authorizes_absence() {
    let fs = Fs::memory();
    let parent = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let (child, _) = parent
        .fork(
            "child",
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    child
        .remove("/file.txt", None, accounting!())
        .await
        .unwrap();
    let (tombstones, _) = child.source_tombstones(1, accounting!()).await.unwrap();
    let (path, deleted) = &tombstones[0];
    // A missing generation is a failed observation, not a missing path in an
    // authenticated generation. The former is_ok() predicate treated both alike.
    let unavailable = crate::Generation {
        workspace: child.workspace().clone(),
        id: crate::GenerationId::new(crate::Digest::from_bytes([0xa3; 32])),
        publication_closure: None,
    };
    assert!(matches!(
        child.source_deletion_is_unmodified(path, *deleted, &unavailable, Some(&parent), accounting!()).await,
        Err(error) if !matches!(error, LazyWorkspaceError::WorkspaceCause(WorkspaceError::NotFound))
    ));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn source_deletion_observes_live_epoch_not_frozen_cut(
        original in proptest::collection::vec(proptest::num::u8::ANY, 1..64),
        edits in proptest::collection::vec(proptest::collection::vec(proptest::num::u8::ANY, 0..64), 1..12),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let fs = Fs::memory();
            let source = Arc::new(CountingSource::new(Bytes::from(original.clone())));
            let root = attach(&fs, Arc::clone(&source), MemoryLazyWorkspaceStore::default()).await.unwrap();
            let old_source = root.state(accounting!()).await.unwrap().source;
            for bytes in edits {
                let expected = SourceVersion(*blake3::hash(&bytes).as_bytes());
                source.replace(Bytes::from(bytes));
                assert_eq!(root.current_source_version("/file.txt", accounting!()).await.unwrap(), Some(expected));
                assert_eq!(root.source_lookup(old_source, "/file.txt", accounting!()).await.unwrap().unwrap().version,
                    SourceVersion(*blake3::hash(&original).as_bytes()));
            }
        });
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn source_deletion_original_admission_never_mutates_either_view() {
    let fs = Fs::memory();
    let parent = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let (child, _) = parent
        .fork(
            "child",
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    child
        .remove("/file.txt", None, accounting!())
        .await
        .unwrap();
    let (tombstones, _) = child.source_tombstones(1, accounting!()).await.unwrap();
    let (path, deleted) = &tombstones[0];
    let child_cut = child.snapshot(accounting!()).await.unwrap();
    let parent_cut = parent.snapshot(accounting!()).await.unwrap();
    let head = child.head(accounting!()).await.unwrap();
    let (mut admitted, mut denied) = (0, 0);
    for reads in 0..64 {
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(
            WorkBudget {
                backend_read_operations: reads,
                ..crate::performance::BOUNDARY_BUDGET
            },
            &cancellation,
        )
        .unwrap_or_else(|(error, _)| panic!("{error:?}"));
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        match child
            .source_deletion_is_unmodified(path, *deleted, &head, Some(&parent), &mut ctx)
            .await
        {
            Ok(true) => admitted += 1,
            Ok(false) => panic!("an unchanged source must remain eligible"),
            Err(_) => denied += 1,
        }
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert!(work.receipt().backend_read_operations <= reads);
        assert_eq!(child.snapshot(accounting!()).await.unwrap(), child_cut);
        assert_eq!(parent.snapshot(accounting!()).await.unwrap(), parent_cut);
    }
    assert!(admitted > 0 && denied > 0);
}
