use super::*;
use futures::FutureExt;

async fn retry_after_retained_conflict() -> Result<FsAppendOutcome, Box<dyn std::error::Error>> {
    let stream = Arc::new(MemoryStream::default());
    let spy = Arc::new(ReleaseSpy::new(stream));
    let store = StreamAuthorityStore::new(spy.clone());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let authority = AuthorityId::from_bytes([77; 16]);
    store
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let head = store.head(authority, &mut work).await?;
    let request = crate::GuardedAppend {
        authority_id: authority,
        epoch: head.epoch,
        expected: head,
        commit: proposed(78),
        permit: PublicationPermit::Unrestricted,
    };
    // Advance only the publication gate after preflight. The real provider
    // durably retains the rejected request; the authority head is unchanged.
    spy.advance_gate.store(true, SeqCst);
    assert!(matches!(
        store
            .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
            .await?,
        FsAppendOutcome::Fenced { .. }
    ));
    assert_eq!(store.head(authority, &mut work).await?, head);
    assert!(
        store
            .find_operation(authority, request.commit.operation_id, &mut work)
            .await?
            .is_none()
    );
    let retried = store
        .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
        .await?;
    if matches!(retried, FsAppendOutcome::Committed(_)) {
        assert!(matches!(
            store
                .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::AlreadyCommitted(_)
        ));
        let mut changed = request;
        changed.commit.fingerprint = Digest::from_bytes([79; 32]);
        assert!(matches!(
            store
                .compare_and_append(changed, &mut work, None::<&mut ()>)
                .await?,
            FsAppendOutcome::IdempotencyConflict { .. }
        ));
        assert_eq!(store.head(authority, &mut work).await?.sequence.get(), 1);
    }
    Ok(retried)
}

fn assert_committed(result: &Result<FsAppendOutcome, Box<dyn std::error::Error>>) {
    assert!(
        matches!(result, Ok(FsAppendOutcome::Committed(_))),
        "retry after retained conflict: {result:?}"
    );
}

#[tokio::test]
async fn rejected_append_retry_preserves_operation_identity_without_poisoning_the_key() {
    assert_committed(&retry_after_retained_conflict().await);
}

#[tokio::test]
async fn omitting_request_binding_fails_the_same_retained_conflict_proof() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            super::super::append::OMIT_REQUEST_BINDING.set(false);
        }
    }
    let _reset = Reset;
    super::super::append::OMIT_REQUEST_BINDING.set(true);
    let result = retry_after_retained_conflict().await;
    assert!(
        result.as_ref().is_err_and(|error| error
            .to_string()
            .contains("Stream idempotency mismatch has no operation record")),
        "wrong control failure: {result:?}"
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| assert_committed(&result)))
            .is_err()
    );
}

#[tokio::test]
async fn omitting_the_atomic_locator_fails_the_same_operation_replay_proof() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            super::super::append::OMIT_OPERATION_LOCATOR.set(false);
        }
    }
    let _reset = Reset;
    super::super::append::OMIT_OPERATION_LOCATOR.set(true);
    let result = std::panic::AssertUnwindSafe(retry_after_retained_conflict())
        .catch_unwind()
        .await;
    assert!(
        result.is_err(),
        "missing locator escaped the replay assertion"
    );
}
