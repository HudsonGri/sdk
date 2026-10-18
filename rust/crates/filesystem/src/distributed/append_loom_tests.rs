use super::*;
use acyclic_stream::{AppendRequest, MemoryStream, StreamProvider};
use std::cell::Cell;

loom::thread_local! { static LEGACY_KEY: Cell<bool> = Cell::new(false); }

pub(super) fn legacy_key() -> bool {
    LEGACY_KEY.with(Cell::get)
}

fn ready<F: Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn proposal(authority: AuthorityId, expected: Head) -> crate::GuardedAppend {
    crate::GuardedAppend {
        authority_id: authority,
        epoch: expected.epoch,
        expected,
        commit: ProposedCommit {
            operation_id: OperationId::from_bytes([72; 16]),
            fingerprint: Digest::from_bytes([73; 32]),
            payload: Bytes::from_static(b"one publication"),
        },
        permit: PublicationPermit::Unrestricted,
    }
}

fn competing_proposal(
    authority: AuthorityId,
    head: Head,
    work: &mut AuthorityWork<'_>,
) -> CommitRequest {
    let commit = proposal(authority, head).commit;
    let mut durable = DurableCommit {
        epoch: head.epoch,
        sequence: Sequence::new(1),
        operation_id: commit.operation_id,
        fingerprint: commit.fingerprint,
        previous_digest: head.digest,
        digest: Digest::ZERO,
        payload: commit.payload,
        settled_at_micros: None,
    };
    durable.digest = authority_commit_digest(
        authority,
        durable.epoch,
        durable.sequence,
        durable.operation_id,
        durable.fingerprint,
        head.digest,
        &durable.payload,
    );
    CommitRequest {
        conditions: vec![
            CommitCondition::Absent {
                path: stream_path(authority, "/retirement").unwrap(),
            },
            CommitCondition::Tail {
                path: stream_path(authority, "/records").unwrap(),
                expected: 1,
            },
            CommitCondition::Tail {
                path: stream_path(authority, "/epochs").unwrap(),
                expected: 1,
            },
            CommitCondition::Tail {
                path: stream_path(authority, "/publication-gate").unwrap(),
                expected: 1,
            },
        ],
        mutations: vec![CommitMutation::Append {
            path: stream_path(authority, "/records").unwrap(),
            records: vec![
                StreamsDurableRecord::encode(&durable, STREAM_RECORD_LIMIT)
                    .unwrap()
                    .into(),
            ],
        }],
        idempotency_key: super::replay::operation_key(authority, durable.operation_id, 0, work)
            .unwrap(),
    }
}

fn model(broken: bool, duplicate: bool) {
    loom::model(move || {
        LEGACY_KEY.with(|fault| fault.set(broken));
        // The actual MemoryStream uses its crate-local acyclic_loom shim. Its
        // provider retains its documented state -> watch lock order; no outer
        // guard crosses a provider call.
        // Each actor owns its AuthorityWork/cancellation state independently.
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream.clone());
        let authority = AuthorityId::from_bytes([71; 16]);
        let epoch = Epoch::GENESIS.get().to_le_bytes();
        // Exact initial authority bytes, in one atomic setup publication. This
        // bounds the model without replaying unrelated creation/codec checks.
        let initial = CommitRequest {
            conditions: vec![],
            mutations: [
                ("", AUTHORITY_MARKER.to_vec()),
                ("/records", [GENESIS_DOMAIN, epoch.as_slice()].concat()),
                ("/epochs", [EPOCH_DOMAIN, epoch.as_slice()].concat()),
                ("/publication-gate", PUBLICATION_GATE_FREE.to_vec()),
            ]
            .into_iter()
            .map(|(suffix, bytes)| CommitMutation::Append {
                path: stream_path(authority, suffix).unwrap(),
                records: vec![bytes.into()],
            })
            .collect(),
            idempotency_key: acyclic_stream::IdempotencyKey::new(
                acyclic_stream::OwnedBytes::from_static(b"setup"),
            )
            .unwrap(),
        };
        assert!(matches!(
            ready(stream.commit(initial)).unwrap(),
            CommitOutcome::Committed(_)
        ));
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let head = Head::genesis(Epoch::GENESIS);
        // The competing writer's read-only preparation is immutable setup.
        // Race its actual atomic Stream publication against every read and
        // publication in the complete authority append/lookup/retry path.
        let competing = duplicate.then(|| competing_proposal(authority, head, &mut work));
        let writer = loom::thread::spawn(move || {
            if let Some(competing) = competing {
                assert!(matches!(
                    ready(stream.commit(competing)).unwrap(),
                    CommitOutcome::Committed(_)
                ));
                return;
            }
            // A competing reservation's release changes only the gate. The
            // operation and authority head remain eligible for a fresh attempt.
            ready(stream.append(AppendRequest {
                path: stream_path(authority, "/publication-gate").unwrap(),
                records: vec![acyclic_stream::OwnedBytes::from_static(
                    PUBLICATION_GATE_FREE,
                )],
                if_tail: None,
                idempotency_key: None,
            }))
            .unwrap();
        });
        let first =
            ready(store.compare_and_append(proposal(authority, head), &mut work, None::<&mut ()>))
                .unwrap();
        assert!(matches!(
            first,
            FsAppendOutcome::Committed(_)
                | FsAppendOutcome::AlreadyCommitted(_)
                | FsAppendOutcome::Fenced { .. }
                | FsAppendOutcome::Conflict { .. }
        ));
        writer.join().unwrap();
        let retried =
            ready(store.compare_and_append(proposal(authority, head), &mut work, None::<&mut ()>));
        assert!(
            matches!(
                retried,
                Ok(FsAppendOutcome::Committed(_) | FsAppendOutcome::AlreadyCommitted(_))
            ),
            "retrying a fenced proposal must publish exactly once: {retried:?}"
        );
        let operation = match retried.unwrap() {
            FsAppendOutcome::Committed(record) | FsAppendOutcome::AlreadyCommitted(record) => {
                record
            }
            _ => unreachable!(),
        };
        assert_eq!(operation.sequence.get(), 1);
        assert_eq!(operation.payload.as_ref(), b"one publication");
        assert_eq!(operation.fingerprint, Digest::from_bytes([73; 32]));
        assert_eq!(
            ready(
                store
                    .provider()
                    .tail(stream_path(authority, "/records").unwrap())
            )
            .unwrap(),
            2
        );
    });
}

#[test]
fn a_new_gate_proposal_survives_every_publication_cut() {
    model(false, false);
}

#[test]
fn concurrent_retries_publish_the_operation_once() {
    model(false, true);
}

#[test]
#[should_panic(expected = "retrying a fenced proposal must publish exactly once")]
fn reusing_the_operation_only_key_is_detected() {
    model(true, false);
}
