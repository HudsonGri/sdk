//! Race one prepared provider commit against the real authority append path.
use super::*;
use crate::sync::atomic::AtomicBool as ModeledBool;
use std::{cell::Cell, future::Future};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;
loom::thread_local! { static BROKEN: Cell<bool> = Cell::new(false); }

pub(in crate::distributed) fn reuse_operation_key() -> bool {
    BROKEN.with(Cell::get)
}

fn ready<F: Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn scenario(broken: bool, same_operation: bool) {
    BROKEN.with(|fault| fault.set(broken));
    let stream = Arc::new(MemoryStream::default());
    let authority = AuthorityId::from_bytes([71; 16]);
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    let initial = StreamAuthorityStore::new(Arc::clone(&stream));
    ready(initial.create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)).unwrap();
    let head = ready(initial.head(authority, &mut work)).unwrap();
    let peer = proposed(if same_operation { 73 } else { 72 });
    let mut record = DurableCommit {
        epoch: head.epoch,
        sequence: head.sequence.checked_next().unwrap(),
        operation_id: peer.operation_id,
        fingerprint: peer.fingerprint,
        previous_digest: head.digest,
        digest: Digest::ZERO,
        payload: peer.payload,
        settled_at_micros: None,
    };
    record.digest = authority_commit_digest(
        authority,
        record.epoch,
        record.sequence,
        record.operation_id,
        record.fingerprint,
        record.previous_digest,
        &record.payload,
    );
    let path = stream_path(authority, "/records").unwrap();
    let peer_request = CommitRequest {
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: head.sequence.get() + 1,
        }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![
                StreamsDurableRecord::encode(&record, STREAM_RECORD_LIMIT)
                    .unwrap()
                    .into(),
            ],
        }],
        idempotency_key: super::super::replay::operation_key(
            authority,
            peer.operation_id,
            0,
            &mut work,
        )
        .unwrap(),
    };
    let once = ModeledBool::new(true);
    let inner = Arc::clone(&stream);
    let mut provider = ReleaseSpy::new(stream);
    provider.before_commit = Some(Box::new(move || {
        if !once.swap(false, SeqCst) {
            return None;
        }
        let stream = Arc::clone(&inner);
        let request = peer_request.clone();
        Some(loom::thread::spawn(move || ready(stream.commit(request))))
    }));
    let store = StreamAuthorityStore::new(Arc::new(provider));
    let request = |expected| crate::GuardedAppend {
        authority_id: authority,
        epoch: head.epoch,
        expected,
        commit: proposed(73),
        permit: PublicationPermit::Unrestricted,
    };
    let result = ready(store.compare_and_append(request(head), &mut work, None::<&mut ()>));
    // The conflicting provider outcome has already retained the first attempt's
    // key. Rebase the same logical operation; do not replay that failed request.
    let result = match result {
        Ok(FsAppendOutcome::Conflict { actual }) => {
            ready(store.compare_and_append(request(actual), &mut work, None::<&mut ()>))
        }
        result => result,
    };
    assert!(
        matches!(
            &result,
            Ok(FsAppendOutcome::Committed(_) | FsAppendOutcome::AlreadyCommitted(_))
        ),
        "rebased authority append did not commit: {result:?}"
    );
    let retained = ready(store.find_operation(authority, proposed(73).operation_id, &mut work))
        .unwrap()
        .unwrap();
    assert_eq!(retained.fingerprint, proposed(73).fingerprint);
    assert!(matches!(
        ready(store.compare_and_append(request(head), &mut work, None::<&mut ()>)).unwrap(),
        FsAppendOutcome::AlreadyCommitted(_)
    ));
    let (history, _) = ready(store.replay(
        authority,
        Sequence::GENESIS,
        ReplayLimit {
            records: 3,
            payload_bytes: 1024,
        },
        &mut work,
    ))
    .unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|record| record.operation_id == proposed(73).operation_id)
            .count(),
        1
    );
}

#[test]
fn a_lost_cas_does_not_poison_the_rebased_operation_identity() {
    for same_operation in [false, true] {
        loom::model(move || scenario(false, same_operation));
    }
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true, false)))
        .expect_err("reusing the failed attempt identity was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("rebased authority append did not commit"),
        "{message}"
    );
}
