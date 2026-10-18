use super::*;
use acyclic_stream::{IdempotencyKey, MemoryStream, StreamProvider};
use proptest::prelude::*;

proptest! {
    // Attempts are disjoint retry identities; the first remains the operation
    // identity used by first-record and fork publication. No history scan.
    #[test]
    fn attempt_keys_bind_authority_operation_and_ordinal(
        authority in any::<[u8;16]>(), operation in any::<[u8;16]>(), attempt in any::<u64>(),
    ) {
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let key = |attempt, work: &mut AuthorityWork<'_>| super::replay::operation_key(
            AuthorityId::from_bytes(authority), OperationId::from_bytes(operation), attempt, work
        ).unwrap();
        let actual = key(attempt, &mut work);
        let domain: &[u8] = if attempt == 0 { b"operation\0" } else { b"operation-retry\0" };
        let mut expected = [domain, &authority, &operation].concat();
        if attempt != 0 { expected.extend_from_slice(&attempt.to_le_bytes()); }
        prop_assert_eq!(actual.as_bytes(), expected.as_slice());
        prop_assert_eq!(&actual, &key(attempt, &mut work));
        prop_assert_ne!(&actual, &key(attempt ^ 1, &mut work));
    }

    // Immutable rejections do not consume an operation. Its first committed
    // slot remains discoverable, replays exactly, and cannot append twice.
    #[test]
    fn rejected_prefixes_preserve_exactly_one_terminal_publication(
        rejected in 0_u64..5, body in "[a-z0-9]{1,16}", fingerprint in any::<[u8;32]>(),
    ) {
        futures::executor::block_on(async {
            let stream = Arc::new(MemoryStream::default());
            let store = StreamAuthorityStore::new(stream.clone());
            let cancellation = CancellationToken::new();
            let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let authority = AuthorityId::from_bytes([91;16]);
            let operation = OperationId::from_bytes([92;16]);
            store.create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>).await.unwrap();
            let head = store.head(authority, &mut work).await.unwrap();
            for attempt in 0..rejected {
                let key: IdempotencyKey = super::replay::operation_key(authority, operation, attempt, &mut work).unwrap();
                // Retain an actual Streams conflict; its mutation never applies.
                let rejected = stream.commit(CommitRequest {
                    conditions: vec![
                        CommitCondition::Tail { path: stream_path(authority, "/publication-gate").unwrap(), expected: 0 },
                        CommitCondition::Tail { path: stream_path(authority, "/records").unwrap(), expected: 1 },
                    ],
                    mutations: vec![CommitMutation::Append { path: stream_path(authority, "/records").unwrap(), records: vec![acyclic_stream::OwnedBytes::from_static(b"not published")] }],
                    idempotency_key: key,
                }).await.unwrap();
                prop_assert!(matches!(rejected, CommitOutcome::Conflict(_)));
            }
            let proposal = crate::GuardedAppend {
                authority_id: authority, epoch: head.epoch, expected: head,
                commit: ProposedCommit { operation_id: operation, fingerprint: Digest::from_bytes(fingerprint), payload: Bytes::from(body.clone()) },
                permit: PublicationPermit::Unrestricted,
            };
            let committed = store.compare_and_append(proposal.clone(), &mut work, None::<&mut ()>).await.unwrap();
            let FsAppendOutcome::Committed(committed) = committed else { return Err(TestCaseError::fail("rejected prefix blocked publication")); };
            prop_assert_eq!(committed.payload.as_ref(), body.as_bytes());
            let found = store.find_operation(authority, operation, &mut work).await.unwrap().unwrap();
            prop_assert_eq!(&committed, &found);
            let retried = store.compare_and_append(proposal, &mut work, None::<&mut ()>).await.unwrap();
            prop_assert_eq!(retried, FsAppendOutcome::AlreadyCommitted(committed));
            prop_assert_eq!(store.head(authority, &mut work).await.unwrap().sequence.get(), 1);
            Ok(())
        })?;
    }
}
