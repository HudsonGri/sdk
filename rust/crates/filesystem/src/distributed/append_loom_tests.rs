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

fn model(broken: bool) {
    loom::model(move || {
        LEGACY_KEY.with(|fault| fault.set(broken));
        // The actual MemoryStream uses its crate-local acyclic_loom shim. Its
        // state lock is the only shared lock; no guard crosses a provider call.
        // Each actor owns its AuthorityWork/cancellation state independently.
        let stream = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(stream.clone());
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let authority = AuthorityId::from_bytes([71; 16]);
        ready(store.create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>))
            .unwrap();
        let head = ready(store.head(authority, &mut work)).unwrap();
        let writer = loom::thread::spawn(move || {
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
        let request = |expected: Head| crate::GuardedAppend {
            authority_id: authority,
            epoch: expected.epoch,
            expected,
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes([72; 16]),
                fingerprint: Digest::from_bytes([73; 32]),
                payload: Bytes::from_static(b"one publication"),
            },
            permit: PublicationPermit::Unrestricted,
        };
        let first =
            ready(store.compare_and_append(request(head), &mut work, None::<&mut ()>)).unwrap();
        assert!(matches!(
            first,
            FsAppendOutcome::Committed(_)
                | FsAppendOutcome::Fenced { .. }
                | FsAppendOutcome::Conflict { .. }
        ));
        writer.join().unwrap();
        let current = ready(store.head(authority, &mut work)).unwrap();
        let retried = ready(store.compare_and_append(request(current), &mut work, None::<&mut ()>));
        assert!(
            matches!(
                retried,
                Ok(FsAppendOutcome::Committed(_) | FsAppendOutcome::AlreadyCommitted(_))
            ),
            "retrying a fenced proposal must publish exactly once: {retried:?}"
        );
        let final_head = ready(store.head(authority, &mut work)).unwrap();
        assert_eq!(final_head.sequence.get(), 1);
        let operation = ready(store.authority_find_operation(
            authority,
            OperationId::from_bytes([72; 16]),
            &mut work,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(operation.payload.as_ref(), b"one publication");
        assert_eq!(operation.fingerprint, Digest::from_bytes([73; 32]));
    });
}

#[test]
fn a_new_gate_proposal_survives_every_publication_cut() {
    model(false);
}

#[test]
#[should_panic(expected = "retrying a fenced proposal must publish exactly once")]
fn reusing_the_operation_only_key_is_detected() {
    model(true);
}
