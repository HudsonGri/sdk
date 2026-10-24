use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use crate::{
    AppendRequest, CommitOutcome, IdempotencyKey, MemoryStream, OwnedBytes, RecordHeaders,
    StreamProvider,
};
use std::sync::atomic::Ordering;

fn seed(
    disk: &mut Disk,
    identity: &StreamPath,
    records: &[Record],
) -> Result<Root, LocalStreamError> {
    let mut head = Head {
        tail: 0,
        records: None,
        created_decision: 1,
    };
    for record in records {
        head.append_record(disk, record)?;
    }
    let mut state = Root {
        decision: 1,
        committed_at_micros: records
            .first()
            .map_or(0, |record| record.committed_at_micros),
        ..Root::default()
    };
    catalog::put(disk, &mut state, identity, head)?;
    Ok(state)
}

fn request(source: &StreamPath, destination: &StreamPath) -> Result<CommitRequest, StreamError> {
    Ok(CommitRequest {
        idempotency_key: IdempotencyKey::new(b"coordinated".to_vec())?,
        conditions: vec![
            CommitCondition::Tail {
                path: source.clone(),
                expected: 2,
            },
            CommitCondition::Absent {
                path: destination.clone(),
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: source.clone(),
                records: vec![OwnedBytes::from_static(b"source-next").into()],
            },
            CommitMutation::Fork {
                source: source.clone(),
                destination: destination.clone(),
                at_tail: 1,
                records: vec![OwnedBytes::from_static(b"fork-next").into()],
            },
        ],
    })
}

#[tokio::test]
async fn coordinated_facts_match_the_memory_planner_and_forks_capture_the_precommit_root_in_both_orders()
-> Result<(), Box<dyn std::error::Error>> {
    for (source, destination) in [("a", "z"), ("z", "a")] {
        let source = StreamPath::new(source)?;
        let destination = StreamPath::new(destination)?;
        let memory = MemoryStream::default();
        let receipt = memory
            .append(AppendRequest {
                path: source.clone(),
                records: vec![
                    OwnedBytes::from_static(b"zero").into(),
                    OwnedBytes::from_static(b"one").into(),
                ],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        let crate::AppendOutcome::Committed(receipt) = receipt else {
            return Err("seed conflict".into());
        };
        let initial = memory.read_commit(receipt.commit_id).await?;
        let [CommittedMutation::Append(initial)] = initial.mutations.as_slice() else {
            return Err("seed shape".into());
        };
        let mut disk = Disk::new(None)?;
        let state = seed(&mut disk, &source, &initial.records)?;
        let source_root = catalog::get(&mut disk, state, &source)?
            .ok_or("missing source")?
            .records;
        let mut request = request(&source, &destination)?;
        let digest = crate::request::commit_digest(&mut request)?;
        let expected = memory.commit(request.clone()).await?;
        let CommitOutcome::Committed(expected_envelope) = &expected else {
            return Err("unexpected conflict".into());
        };
        let sampled = expected_envelope
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                CommittedMutation::Append(append) => append.records.first(),
                CommittedMutation::Fork(fork) => fork.records.first(),
            })
            .ok_or("missing new record")?
            .committed_at_micros
            / 1_000;
        let prepared = prepare(&mut disk, state, &request, digest, sampled)?;
        assert_eq!(prepared.outcome.value, expected);
        assert_eq!(prepared.state.decision, 2);
        let CommitOutcome::Committed(envelope) = &prepared.outcome.value else {
            return Err("cold conflict".into());
        };
        for (mutation, head) in envelope.mutations.iter().zip(&prepared.heads.value) {
            assert_eq!(
                head.records, source_root,
                "neither append nor fork rebuilds the shared source index"
            );
            match mutation {
                CommittedMutation::Append(_) => {
                    assert_eq!((head.tail, head.created_decision), (2, 1));
                }
                CommittedMutation::Fork(_) => {
                    assert_eq!((head.tail, head.created_decision), (1, 2));
                }
            }
        }
        assert_eq!(
            catalog::get(&mut disk, state, &source)?
                .ok_or("lost source")?
                .tail,
            2
        );
        assert!(catalog::get(&mut disk, state, &destination)?.is_none());
    }
    Ok(())
}

fn initial_record(sequence: u64) -> Record {
    Record {
        sequence,
        value: OwnedBytes::from_static(b"old"),
        headers: RecordHeaders::default(),
        commit_id: CommitId::from_bytes([3; 32]),
        committed_at_micros: 50_000,
    }
}

#[test]
fn forged_head_conflicts_win_over_missing_fork_authority_without_mutating_the_cut()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let source = StreamPath::new("source")?;
    let destination = StreamPath::new("fork")?;
    let state = seed(&mut disk, &source, &[initial_record(0), initial_record(1)])?;
    let mut request = request(&source, &destination)?;
    request.conditions = vec![
        CommitCondition::Head {
            path: source.clone(),
            expected: 2,
            commit_id: CommitId::from_bytes([4; 32]),
        },
        CommitCondition::Absent {
            path: destination.clone(),
        },
    ];
    for mutation in &mut request.mutations {
        if let CommitMutation::Fork { source, .. } = mutation {
            *source = StreamPath::new("missing")?;
        }
    }
    let digest = crate::request::commit_digest(&mut request)?;
    let prepared = prepare(&mut disk, state, &request, digest, 1);
    assert!(
        prepared.is_ok(),
        "failed conditions precede fork source authority"
    );
    let prepared = prepared?;
    assert_eq!(
        prepared.state, state,
        "a retained conflict cannot advance decision, clock or indexes"
    );
    assert!(prepared.heads.value.is_empty());
    assert_eq!(
        prepared.outcome.value,
        CommitOutcome::Conflict(vec![CommitConflict::Head {
            path: source.clone(),
            expected: CommitId::from_bytes([4; 32]),
            actual: CommitId::from_bytes([3; 32]),
        }])
    );
    request.conditions = vec![
        CommitCondition::Tail {
            path: source,
            expected: 2,
        },
        CommitCondition::Absent { path: destination },
    ];
    let digest = crate::request::commit_digest(&mut request)?;
    assert!(matches!(
        prepare(&mut disk, state, &request, digest, 1),
        Err(LocalStreamError::Replay(StreamError::NotFound))
    ));
    Ok(())
}

fn retire_path(
    identity: &mut StreamPath,
    meter: &Arc<Meter>,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = meter.held.load(Ordering::SeqCst);
    let probe = StreamPath::new_native(identity.as_str(), &meter.original())??;
    let expected = meter.held.load(Ordering::SeqCst) - before;
    drop(probe);
    let retired = std::mem::replace(identity, StreamPath::new("retired")?);
    drop(retired);
    assert_eq!(
        before - meter.held.load(Ordering::SeqCst),
        expected,
        "output path owns its original text and metadata after input retirement"
    );
    Ok(())
}

#[test]
fn partial_preparation_refunds_every_allocation_and_returned_paths_outlive_the_input()
-> Result<(), Box<dyn std::error::Error>> {
    let mut reached_success = false;
    for allowance in 0..64 {
        let meter = Arc::new(Meter::local());
        let work = crate::memory::Work::new(meter.original())?;
        let mut disk = Disk::new(Some(work))?;
        let source = StreamPath::new("source")?;
        let destination = StreamPath::new("fork")?;
        let state = seed(&mut disk, &source, &[initial_record(0), initial_record(1)])?;
        let mut request = request(&source, &destination)?;
        let digest = crate::request::commit_digest(&mut request)?;
        let baseline = meter.held.load(Ordering::SeqCst);
        let before = meter.allocations.load(Ordering::SeqCst);
        meter.limit.store(before + allowance, Ordering::SeqCst);
        let mut prepared = prepare(&mut disk, state, &request, digest, 1);
        let success = prepared.is_ok();
        drop(request);
        if let Ok(prepared) = &mut prepared {
            meter.limit.store(u64::MAX, Ordering::SeqCst);
            let CommitOutcome::Committed(envelope) = &mut prepared.outcome.value else {
                return Err("unexpected conflict".into());
            };
            for mutation in &mut envelope.mutations {
                match mutation {
                    CommittedMutation::Append(append) => retire_path(&mut append.path, &meter)?,
                    CommittedMutation::Fork(fork) => {
                        retire_path(&mut fork.source, &meter)?;
                        retire_path(&mut fork.destination, &meter)?;
                    }
                }
            }
        }
        drop(prepared);
        assert_eq!(
            meter.held.load(Ordering::SeqCst),
            baseline,
            "allocation prefix {allowance}"
        );
        drop(disk);
        meter.assert_refunded();
        if success {
            reached_success = true;
            break;
        }
    }
    assert!(reached_success);
    Ok(())
}
