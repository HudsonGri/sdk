use super::*;
use acyclic_stream::{self as stream, IdempotencyKey, MemoryStream, StreamProvider};
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

const RACED: AuthorityId = AuthorityId::from_bytes([93; 16]);

#[derive(Clone, Copy, Debug)]
enum Race {
    Reject,
    RejectAfterFence,
    CommitDivergent,
    FailInspect,
}

/// Lands a rival write in the operation's slot between the authority's slot
/// lookup and its own proposal: the cut a retained Streams retry key exposes.
struct Racer {
    inner: Arc<MemoryStream>,
    race: Race,
    fired: std::sync::atomic::AtomicBool,
    cancellation: CancellationToken,
}

fn operation_identity(key: &IdempotencyKey) -> bool {
    key.as_bytes().starts_with(b"operation")
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl StreamProvider for Racer {
    fn operation_claims(&self) -> &stream::OperationClaims {
        self.inner.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<stream::IdempotencyObservation>, stream::StreamError> {
        if matches!(self.race, Race::FailInspect) && operation_identity(&key) {
            self.cancellation.cancel();
            return Err(stream::StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, stream::StreamError> {
        if operation_identity(&request.idempotency_key)
            && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let mut rival = request.clone();
            if matches!(self.race, Race::CommitDivergent) {
                // Same operation and record bytes, different request bytes.
                let before = rival.conditions.len();
                rival
                    .conditions
                    .retain(|condition| !matches!(condition, CommitCondition::Absent { .. }));
                assert!(rival.conditions.len() < before);
                let outcome = self.inner.commit(rival).await?;
                assert!(matches!(outcome, CommitOutcome::Committed(_)));
            } else {
                if matches!(self.race, Race::RejectAfterFence) {
                    let store = StreamAuthorityStore::new(self.inner.clone());
                    let token = CancellationToken::new();
                    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
                    let head = store.head(RACED, &mut work).await.unwrap();
                    store.fence(RACED, head, &mut work).await.unwrap();
                }
                // Require an existing path to be absent: a retained rejection.
                let condition = rival
                    .conditions
                    .iter_mut()
                    .find(|condition| !matches!(condition, CommitCondition::Absent { .. }))
                    .unwrap();
                let (CommitCondition::Tail { path, .. } | CommitCondition::Head { path, .. }) =
                    condition.clone()
                else {
                    unreachable!()
                };
                *condition = CommitCondition::Absent { path };
                let outcome = self.inner.commit(rival).await?;
                assert!(matches!(outcome, CommitOutcome::Conflict(_)));
            }
        }
        self.inner.commit(request).await
    }
    async fn tail(&self, path: stream::StreamPath) -> Result<u64, stream::StreamError> {
        self.inner.tail(path).await
    }
    async fn read(
        &self,
        request: stream::ReadRequest,
    ) -> Result<stream::RecordStream, stream::StreamError> {
        self.inner.read(request).await
    }
    async fn bounds(
        &self,
        path: stream::StreamPath,
    ) -> Result<stream::StreamBounds, stream::StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: stream::AppendRequest,
    ) -> Result<stream::AppendOutcome, stream::StreamError> {
        self.inner.append(request).await
    }
    async fn fork(
        &self,
        request: stream::ForkRequest,
    ) -> Result<stream::ForkReceipt, stream::StreamError> {
        self.inner.fork(request).await
    }
    async fn follow(
        &self,
        path: stream::StreamPath,
        from: u64,
    ) -> Result<stream::RecordStream, stream::StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: stream::ChildrenRequest,
    ) -> Result<stream::ChildStream, stream::StreamError> {
        self.inner.children(request).await
    }
    async fn read_commit(
        &self,
        id: stream::CommitId,
    ) -> Result<stream::CommittedEnvelope, stream::StreamError> {
        self.inner.read_commit(id).await
    }
}

fn raced(race: Race) -> (StreamAuthorityStore<Racer>, CancellationToken) {
    let cancellation = CancellationToken::new();
    let racer = Racer {
        inner: Arc::new(MemoryStream::default()),
        race,
        fired: Default::default(),
        cancellation: cancellation.clone(),
    };
    (StreamAuthorityStore::new(Arc::new(racer)), cancellation)
}

// Every idempotency-mismatch outcome follows the authority contract: a
// committed operation replays by fingerprint, an uncommitted one is ordinary
// contention classified by epoch, and neither is reported as corruption.
fn rival_in_the_operation_slot(race: Race) {
    futures::executor::block_on(async {
        let (store, cancellation) = raced(race);
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        store
            .create_authority(RACED, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await
            .unwrap();
        let head = store.head(RACED, &mut work).await.unwrap();
        let proposal = crate::GuardedAppend {
            authority_id: RACED,
            epoch: head.epoch,
            expected: head,
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes([94; 16]),
                fingerprint: Digest::from_bytes([95; 32]),
                payload: Bytes::from_static(b"raced"),
            },
            permit: PublicationPermit::Unrestricted,
        };
        let first = store
            .compare_and_append(proposal.clone(), &mut work, None::<&mut ()>)
            .await;
        match race {
            Race::Reject => {
                assert_eq!(first.unwrap(), FsAppendOutcome::Conflict { actual: head });
                let retried = store
                    .compare_and_append(proposal, &mut work, None::<&mut ()>)
                    .await
                    .unwrap();
                let FsAppendOutcome::Committed(committed) = retried else {
                    panic!("the successor slot must publish: {retried:?}");
                };
                assert_eq!(committed.payload.as_ref(), b"raced");
            }
            Race::RejectAfterFence => assert!(
                matches!(first, Ok(FsAppendOutcome::Fenced { actual_epoch }) if actual_epoch != head.epoch),
                "{race:?}: {first:?}"
            ),
            Race::CommitDivergent => assert!(
                matches!(&first, Ok(FsAppendOutcome::AlreadyCommitted(found)) if found.payload.as_ref() == b"raced"),
                "{race:?}: {first:?}"
            ),
            Race::FailInspect => unreachable!(),
        }
    });
}

#[test]
fn a_rejected_rival_is_contention_and_its_successor_slot_publishes() {
    rival_in_the_operation_slot(Race::Reject);
}

#[test]
fn a_rejected_rival_after_a_fence_reports_the_fence() {
    rival_in_the_operation_slot(Race::RejectAfterFence);
}

#[test]
fn a_committed_divergent_rival_replays_by_fingerprint() {
    rival_in_the_operation_slot(Race::CommitDivergent);
}

// Ordinary custody reports the cancellation that interrupted a failed lookup.
#[test]
fn cancellation_outranks_a_failed_operation_lookup() {
    futures::executor::block_on(async {
        let (store, cancellation) = raced(Race::FailInspect);
        let token = CancellationToken::new();
        let mut setup = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        store
            .create_authority(RACED, Epoch::GENESIS, &mut setup, None::<&mut ()>)
            .await
            .unwrap();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let found = store
            .find_operation(RACED, OperationId::from_bytes([94; 16]), &mut work)
            .await;
        assert!(
            matches!(found, Err(AuthorityStoreError::Cancelled)),
            "{found:?}"
        );
    });
}
