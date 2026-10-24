use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use crate::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryLimits, MemoryStream,
    OwnedBytes, StreamProvider,
};
use std::sync::atomic::{AtomicU64, Ordering};

fn request(turn: u64) -> Result<CommitRequest, crate::StreamError> {
    let path = StreamPath::new("tree/source")?;
    let mut conditions = vec![if turn == 0 {
        CommitCondition::Absent { path: path.clone() }
    } else {
        CommitCondition::Tail {
            path: path.clone(),
            expected: turn,
        }
    }];
    let mut mutations = vec![CommitMutation::Append {
        path: path.clone(),
        records: vec![OwnedBytes::from_static(b"next").into()],
    }];
    if turn > 0 && turn.is_multiple_of(4) {
        let destination = StreamPath::new(format!("tree/forks/{turn}"))?;
        conditions.push(CommitCondition::Absent {
            path: destination.clone(),
        });
        mutations.push(CommitMutation::Fork {
            source: path,
            destination,
            at_tail: turn / 2,
            records: vec![OwnedBytes::from_static(b"fork").into()],
        });
    }
    Ok(CommitRequest {
        idempotency_key: IdempotencyKey::new(turn.to_be_bytes().to_vec())?,
        conditions,
        mutations,
    })
}

fn apply_request(
    io: &mut impl PageIo,
    state: Root,
    request: &mut CommitRequest,
) -> Result<Applied, Box<dyn std::error::Error>> {
    let digest = crate::request::commit_digest(request)?;
    let prepared = planning::prepare(io, state, request, digest, 7)?;
    Ok(apply(io, prepared, &request.idempotency_key, digest)?)
}

pub(super) fn assert_envelope(
    io: &mut impl PageRead,
    state: Root,
    expected: &CommittedEnvelope,
) -> Result<(), Box<dyn std::error::Error>> {
    let actual = retained::envelope(io, state, expected.commit_id)?.ok_or("missing envelope")?;
    assert_eq!(&actual.value, expected);
    Ok(())
}

async fn conflicting_ancestor(
    memory: &MemoryStream,
    disk: &mut Disk,
    state: Root,
    turn: u64,
) -> Result<Root, Box<dyn std::error::Error>> {
    let root = StreamPath::new("tree")?;
    assert_eq!(
        catalog::get(disk, state, &root)?
            .ok_or("missing implicit ancestor")?
            .tail,
        0
    );
    let mut conflict = CommitRequest {
        idempotency_key: IdempotencyKey::new(format!("conflict-{turn}").into_bytes())?,
        conditions: vec![CommitCondition::Absent { path: root.clone() }],
        mutations: vec![CommitMutation::Append {
            path: root,
            records: vec![OwnedBytes::from_static(b"refused").into()],
        }],
    };
    let expected = memory.commit(conflict.clone()).await?;
    assert!(matches!(expected, CommitOutcome::Conflict(_)));
    let applied = apply_request(disk, state, &mut conflict)?;
    assert_eq!(applied.outcome.value, IdempotencyOutcome::Commit(expected));
    assert_eq!(applied.state.decision, state.decision);
    assert_eq!(applied.state.committed_at_micros, state.committed_at_micros);
    Ok(applied.state)
}

#[tokio::test]
async fn published_roots_match_memory_envelopes_retries_and_implicit_ancestors()
-> Result<(), Box<dyn std::error::Error>> {
    let memory = MemoryStream::new_with_clock(MemoryLimits::default(), Arc::new(AtomicU64::new(7)));
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(work))?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let mut state = Root::default();
    let source = StreamPath::new("tree/source")?;
    for turn in 0..32 {
        let old = state;
        let mut request = request(turn)?;
        let expected = memory.commit(request.clone()).await?;
        let applied = apply_request(&mut disk, state, &mut request)?;
        assert_eq!(
            applied.outcome.value,
            IdempotencyOutcome::Commit(expected.clone())
        );
        state = applied.state;
        let CommitOutcome::Committed(envelope) = expected else {
            return Err("unexpected conflict".into());
        };
        assert_eq!(state.decision, turn + 1);
        assert_envelope(&mut disk, state, &envelope)?;
        let actual = retained::observe(&mut disk, state, &request.idempotency_key)?
            .ok_or("missing retry")?;
        assert_eq!(
            Some(&actual.value),
            memory
                .inspect_idempotency(request.idempotency_key.clone())
                .await?
                .as_ref()
        );
        if turn > 0 {
            let previous = catalog::get(&mut disk, old, &source)?.ok_or("missing old source")?;
            assert_eq!(
                previous.tail, turn,
                "publication never mutates an earlier root"
            );
            assert!(previous.record(&mut disk, turn)?.is_none());
        }
        drop(actual);
        drop(applied);
        // Immutable ancestor heads have zero tails and participate in absence
        // conditions and child discovery just like the reference backend.
        state = conflicting_ancestor(&memory, &mut disk, state, turn).await?;
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    for parent in [
        None,
        Some(StreamPath::new("tree")?),
        Some(StreamPath::new("tree/forks")?),
    ] {
        let page = memory
            .children_page(crate::ChildrenPageRequest {
                parent: parent.clone(),
                after: None,
                hierarchy_version: None,
                limit: 100,
            })
            .await?;
        let mut actual = Vec::new();
        let mut previous = None;
        while let Some(child) = children::next(
            &mut disk,
            state,
            parent.as_ref(),
            children::Order::Alphabetical,
            previous.as_ref().map(|(path, decision)| (path, *decision)),
        )? {
            actual.push(child.path.clone());
            previous = Some((child.path, child.decision));
        }
        assert_eq!(
            actual,
            page.children
                .into_iter()
                .map(|child| child.path)
                .collect::<Vec<_>>()
        );
        assert_eq!(state.hierarchy_version, page.hierarchy_version);
    }
    drop(disk);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn successful_and_conflicting_decisions_reopen_from_only_the_published_root()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = store::Store::open(
        directory.path(),
        2 * 1024 * 1024,
        super::super::super::LocalDurability::FullFlush,
    )?;
    for turn in 0..16 {
        let mut request = request(turn)?;
        let mut transaction = store.begin(128 * 1024, None)?;
        let state = transaction.state;
        let applied = apply_request(&mut transaction, state, &mut request)?;
        let expected = applied.state;
        transaction.state = expected;
        transaction.publish()?;
        drop(store);
        store = store::Store::open(
            directory.path(),
            2 * 1024 * 1024,
            super::super::super::LocalDurability::FullFlush,
        )?;
        assert_eq!(store.state()?, expected);
        let mut reader = store.reader(None);
        let actual = retained::observe(&mut reader, expected, &request.idempotency_key)?
            .ok_or("lost retained outcome")?;
        assert_eq!(actual.value.outcome, applied.outcome.value);
        let source = catalog::get(&mut reader, expected, &StreamPath::new("tree/source")?)?
            .ok_or("lost source")?;
        assert_eq!(source.tail, turn + 1);
        assert_eq!(
            source
                .record(&mut reader, turn)?
                .ok_or("lost record")?
                .value,
            OwnedBytes::from_static(b"next")
        );
    }
    Ok(())
}
