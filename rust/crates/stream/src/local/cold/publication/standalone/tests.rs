use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use crate::{
    AppendRequest, ForkRequest, MemoryLimits, MemoryStream, OwnedBytes, StreamError, StreamProvider,
};
use std::sync::atomic::{AtomicU64, Ordering};

fn assert_replay(
    disk: &mut Disk,
    state: Root,
    key: &IdempotencyKey,
    family: retained::Family,
    digest: [u8; 32],
    expected: &IdempotencyOutcome,
) -> Result<(), Box<dyn std::error::Error>> {
    let replayed = retry::replay(disk, state, Some(key), family, digest)?.ok_or("missing retry")?;
    assert_eq!(&replayed.value, expected);
    Ok(())
}

async fn append_turn(
    memory: &MemoryStream,
    disk: &mut Disk,
    mut state: Root,
    source: &StreamPath,
    turn: u64,
    sampled_millis: u64,
    meter: &Meter,
) -> Result<Root, Box<dyn std::error::Error>> {
    let key = IdempotencyKey::new(format!("append-{turn}").into_bytes())?;
    let request = AppendRequest {
        path: source.clone(),
        if_tail: Some(turn),
        records: vec![OwnedBytes::from_static(b"next").into()],
        idempotency_key: Some(key.clone()),
    };
    let digest = crate::request::append_digest(&request)?;
    let expected = memory.append(request.clone()).await?;
    let plan = planning::standalone::append(disk, state, &request, digest, sampled_millis)?;
    let applied = apply(disk, plan, Some(&key), digest)?;
    assert_eq!(
        applied.outcome.value,
        IdempotencyOutcome::Append(expected.clone())
    );
    state = applied.state;
    let AppendOutcome::Committed(receipt) = expected else {
        return Err("unexpected append conflict".into());
    };
    super::super::tests::assert_envelope(
        disk,
        state,
        &memory.read_commit(receipt.commit_id).await?,
    )?;
    let writes = meter.write_operations.load(Ordering::SeqCst);
    assert_replay(
        disk,
        state,
        &key,
        retained::Family::Append,
        digest,
        &applied.outcome.value,
    )?;
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), writes);
    drop(applied);
    Ok(state)
}

async fn fork_turn(
    memory: &MemoryStream,
    disk: &mut Disk,
    mut state: Root,
    source: &StreamPath,
    turn: u64,
) -> Result<Root, Box<dyn std::error::Error>> {
    let key = IdempotencyKey::new(format!("fork-{turn}").into_bytes())?;
    let request = ForkRequest {
        source: source.clone(),
        destination: StreamPath::new(format!("forks/{turn}"))?,
        at_tail: Some(1),
        idempotency_key: Some(key.clone()),
    };
    let digest = crate::request::fork_digest(&request)?;
    let expected = memory.fork(request.clone()).await?;
    let plan = planning::standalone::fork(disk, state, &request, digest)?;
    let applied = apply(disk, plan, Some(&key), digest)?;
    assert_eq!(
        applied.outcome.value,
        IdempotencyOutcome::Fork(expected.clone())
    );
    state = applied.state;
    assert_eq!(state.decision, 2 * (turn + 1));
    assert_eq!(
        state.committed_at_micros, 7_000,
        "fork has no record clock to sample"
    );
    super::super::tests::assert_envelope(
        disk,
        state,
        &memory.read_commit(expected.commit_id).await?,
    )?;
    let head = catalog::get(disk, state, &expected.destination)?.ok_or("missing fork")?;
    assert_eq!(head.tail, 1);
    assert_eq!(
        head.record(disk, 0)?.ok_or("lost shared prefix")?.value,
        OwnedBytes::from_static(b"next")
    );
    assert_replay(
        disk,
        state,
        &key,
        retained::Family::Fork,
        digest,
        &applied.outcome.value,
    )?;
    drop(applied);
    Ok(state)
}

#[tokio::test]
async fn append_and_fork_match_reference_decision_order_clock_envelopes_and_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(AtomicU64::new(7));
    let memory = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(work))?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let mut state = Root::default();
    let source = StreamPath::new("tree/source")?;
    for turn in 0..16 {
        state = append_turn(
            &memory,
            &mut disk,
            state,
            &source,
            turn,
            clock.load(Ordering::SeqCst),
            &meter,
        )
        .await?;
        clock.store(9_000, Ordering::SeqCst);
        state = fork_turn(&memory, &mut disk, state, &source, turn).await?;
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
        clock.store(1, Ordering::SeqCst);
    }
    let request = ForkRequest {
        source: StreamPath::new("tree")?,
        destination: StreamPath::new("empty-fork")?,
        at_tail: None,
        idempotency_key: None,
    };
    let digest = crate::request::fork_digest(&request)?;
    let expected = memory.fork(request.clone()).await?;
    let plan = planning::standalone::fork(&mut disk, state, &request, digest)?;
    let applied = apply(&mut disk, plan, None, digest)?;
    assert_eq!(applied.outcome.value, IdempotencyOutcome::Fork(expected));
    let head = catalog::get(&mut disk, applied.state, &request.destination)?
        .ok_or("missing empty fork")?;
    assert_eq!(head.tail, 0);
    assert!(head.record(&mut disk, 0)?.is_none());
    drop(applied);
    drop(disk);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn standalone_conflicts_preserve_missing_stream_tail_and_destination_precedence()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let state = Root::default();
    let missing = StreamPath::new("missing")?;
    let request = AppendRequest {
        path: missing.clone(),
        if_tail: Some(1),
        idempotency_key: None,
        records: vec![OwnedBytes::from_static(b"refused").into()],
    };
    let digest = crate::request::append_digest(&request)?;
    let plan = planning::standalone::append(&mut disk, state, &request, digest, 7)?;
    assert_eq!(
        plan.outcome.value,
        IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail: 0 })
    );
    assert!(
        plan.envelope.is_none(),
        "a rejected append has no publication"
    );
    let applied = apply(&mut disk, plan, None, digest)?;
    assert_eq!(applied.state, state);
    assert_eq!(
        applied.outcome.value,
        IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail: 0 })
    );
    let mut state = state;
    state.decision = 1;
    let destination = StreamPath::new("existing")?;
    catalog::put(
        &mut disk,
        &mut state,
        &destination,
        catalog::Head {
            tail: 0,
            created_decision: 1,
            records: None,
        },
    )?;
    let request = ForkRequest {
        source: missing,
        destination,
        at_tail: None,
        idempotency_key: None,
    };
    let digest = crate::request::fork_digest(&request)?;
    assert!(matches!(
        planning::standalone::fork(&mut disk, state, &request, digest),
        Err(LocalStreamError::Replay(StreamError::AlreadyExists))
    ));
    Ok(())
}
