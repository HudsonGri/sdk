use super::*;
use proptest::prelude::*;

// Property: arbitrary sequences of already-satisfied directory requests (and
// empty requests) seal exactly their key, replay it without another append,
// and preserve both the tree and a different key's durable identity.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn clean_transactions_seal_and_replay_their_own_operation(
        requests in prop::collection::vec(0u8..3, 0..8),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(check(&requests));
    }
}

async fn check(requests: &[u8]) {
    let workspace = unbounded!(Fs::memory(), create_workspace("clean-operation")).unwrap();
    let mut create = unbounded!(workspace, begin_transaction(IdempotencyKey::new())).unwrap();
    unbounded!(create, create_dir_all("/a/b/c")).unwrap();
    let TransactionCommit::Committed(original) = commit(&mut create).await.unwrap() else {
        panic!("fixture did not commit");
    };
    let paths = ["/a", "/a/b", "/a/b/c"];
    let mut keys = Vec::new();
    for _ in 0..2 {
        let key = IdempotencyKey::new();
        keys.push(key);
        let mut transaction = unbounded!(workspace, begin_transaction(key)).unwrap();
        for &request in requests {
            unbounded!(transaction, create_dir_all(paths[request as usize])).unwrap();
        }
        assert!(!transaction.checkout.has_pending_mutations());
        let TransactionCommit::Committed(sealed) = commit(&mut transaction)
            .await
            .unwrap_or_else(|error| panic!("clean operation did not seal: {error}"))
        else {
            panic!("clean operation did not seal");
        };
        assert_eq!(sealed.id(), original.id());
        assert_eq!(
            unbounded!(workspace, operation_generation(key))
                .unwrap()
                .unwrap()
                .id(),
            sealed.id()
        );
        let mut replay = unbounded!(workspace, begin_transaction(key)).unwrap();
        for &request in requests {
            unbounded!(replay, create_dir_all(paths[request as usize])).unwrap();
        }
        assert!(matches!(commit(&mut replay).await.unwrap(),
                    TransactionCommit::AlreadyCommitted(ref generation) if generation.id() == sealed.id()));
        assert!(matches!(commit(&mut transaction).await.unwrap(),
                    TransactionCommit::AlreadyCommitted(ref generation) if generation.id() == sealed.id()));
    }
    for key in keys {
        let mut replay = unbounded!(workspace, begin_transaction(key)).unwrap();
        assert!(matches!(commit(&mut replay).await.unwrap(),
                    TransactionCommit::AlreadyCommitted(ref generation) if generation.id() == original.id()));
    }
    let head = unbounded!(workspace, head()).unwrap();
    assert_eq!(head.id(), original.id());
    for path in paths {
        unbounded!(head, stat(path)).unwrap();
    }
}

#[test]
#[should_panic(expected = "clean operation did not seal")]
fn restoring_clean_rejection_fails_the_same_property() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            crate::facade::RESTORE_CLEAN_REJECTION.set(false);
        }
    }
    let _reset = Reset;
    crate::facade::RESTORE_CLEAN_REJECTION.set(true);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(check(&[]));
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn clean_publication_keeps_the_owner_guard_and_unpublished_identity()
-> Result<(), Box<dyn Error>> {
    for native in [false, true] {
        let workspace = unbounded!(Fs::memory(), create_workspace("guarded-clean"))?;
        let key = IdempotencyKey::new();
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(selected_transaction_budget(), &cancellation)
                .map_err(|(error, _)| error)?;
        let mut transaction = if native {
            workspace
                .begin_transaction(key, &original, &cancellation)
                .await?
        } else {
            unbounded!(workspace, begin_transaction(key))?
        };
        let result = if native {
            transaction
                .commit_with_native_work::<WorkspaceError, _, _>(
                    None,
                    crate::PublicationPermit::Unrestricted,
                    &original,
                    &cancellation,
                    &mut |_| std::future::ready(Err(FsError::CreationRejected.into())),
                )
                .await
        } else {
            value!(
                transaction
                    .commit::<WorkspaceError, _, _>(
                        None,
                        crate::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                        |_| std::future::ready(Err(crate::OperationFailure::before_work(
                            FsError::CreationRejected.into()
                        ))),
                    )
                    .await
            )
        };
        assert!(matches!(
            result,
            Err(WorkspaceError::Filesystem(FsError::CreationRejected))
        ));
        assert!(unbounded!(workspace, operation_generation(key))?.is_none());
        let committed = if native {
            transaction
                .commit_with_native_work::<WorkspaceError, _, _>(
                    None,
                    crate::PublicationPermit::Unrestricted,
                    &original,
                    &cancellation,
                    &mut |_| std::future::ready(Ok(())),
                )
                .await?
        } else {
            commit(&mut transaction).await?
        };
        assert!(matches!(committed, TransactionCommit::Committed(_)));
    }
    Ok(())
}
