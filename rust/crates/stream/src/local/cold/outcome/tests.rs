use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use crate::{
    AppendReceipt, CommitId, CommittedEnvelope, CommittedFork, CommittedMutation, ForkReceipt,
};
use std::sync::atomic::Ordering;

fn conflict_fixture() -> Result<IdempotencyOutcome, crate::StreamError> {
    Ok(IdempotencyOutcome::Commit(CommitOutcome::Conflict(vec![
        CommitConflict::Head {
            path: StreamPath::new("a")?,
            expected: CommitId::from_bytes([1; 32]),
            actual: CommitId::from_bytes([2; 32]),
        },
        CommitConflict::Tail {
            path: StreamPath::new("b")?,
            expected: 7,
            actual: None,
        },
        CommitConflict::Tail {
            path: StreamPath::new("c")?,
            expected: 7,
            actual: Some(8),
        },
        CommitConflict::Exists {
            path: StreamPath::new("d")?,
        },
    ])))
}

#[test]
fn all_terminal_outcome_families_round_trip_without_replaying_other_payloads()
-> Result<(), Box<dyn std::error::Error>> {
    for native in [false, true] {
        let meter = Arc::new(Meter::local());
        let work = native
            .then(|| crate::memory::Work::new(meter.original()))
            .transpose()?;
        let mut disk = Disk::new(work)?;
        let commit_id = CommitId::from_bytes([9; 32]);
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Fork(CommittedFork {
                source: StreamPath::new("a")?,
                destination: StreamPath::new("b")?,
                forked_at: 0,
                tail: 0,
                records: vec![],
            })],
        };
        let envelope_reference =
            envelope::append(&mut disk, &envelope, |_, _| Err(LocalStreamError::Corrupt))?;
        let cases = [
            IdempotencyOutcome::Append(AppendOutcome::Committed(AppendReceipt {
                start: 7,
                end: 9,
                tail: 9,
                commit_id,
            })),
            IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail: 0 }),
            IdempotencyOutcome::Fork(ForkReceipt {
                source: StreamPath::new("a")?,
                destination: StreamPath::new("b")?,
                forked_at: 7,
                tail: 7,
                commit_id,
            }),
            conflict_fixture()?,
            IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)),
        ];
        for expected in cases {
            let coordinated = matches!(
                expected,
                IdempotencyOutcome::Commit(CommitOutcome::Committed(_))
            );
            let reference = append(
                &mut disk,
                &expected,
                coordinated.then_some(envelope_reference),
            )?;
            for _ in 0..16 {
                disk.append(PageKind::Fact, b"unrelated")?;
            }
            let baseline = meter.held.load(Ordering::SeqCst);
            disk.reads = 0;
            let actual = read(&mut disk, reference)?;
            assert_eq!(actual.value, expected);
            assert_eq!(disk.reads, if coordinated { 2 } else { 1 });
            if native
                && let IdempotencyOutcome::Commit(CommitOutcome::Conflict(conflicts)) =
                    &actual.value
            {
                let owned = meter.held.load(Ordering::SeqCst) - baseline;
                let mut path_bytes = 0;
                for conflict in conflicts {
                    let before = meter.held.load(Ordering::SeqCst);
                    let path = StreamPath::new_native(
                        conflict_path(conflict).as_str(),
                        &meter.original(),
                    )??;
                    path_bytes += meter.held.load(Ordering::SeqCst) - before;
                    drop(path);
                }
                let container =
                    u64::try_from(conflicts.capacity() * std::mem::size_of::<CommitConflict>())?;
                assert_eq!(
                    owned,
                    path_bytes + container,
                    "returned conflict paths and vector keep their exact original funding"
                );
            }
            drop(actual);
            assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
        }
        drop(disk);
        meter.assert_refunded();
    }
    Ok(())
}

#[test]
fn every_partial_conflict_decode_keeps_paths_and_vector_on_the_original_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    let mut reached_success = false;
    for allowance in 0..32 {
        let meter = Arc::new(Meter::local());
        let work = crate::memory::Work::new(meter.original())?;
        let mut disk = Disk::new(Some(work))?;
        let expected = conflict_fixture()?;
        let reference = append(&mut disk, &expected, None)?;
        let baseline = meter.held.load(Ordering::SeqCst);
        let before = meter.allocations.load(Ordering::SeqCst);
        meter.limit.store(before + allowance, Ordering::SeqCst);
        let actual = read(&mut disk, reference);
        let success = actual.is_ok();
        if success {
            assert_eq!(
                actual.as_ref().map(|output| &output.value).ok(),
                Some(&expected)
            );
        }
        drop(actual);
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

#[test]
fn reauthenticated_bad_outcomes_and_unrepresentable_receipts_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let expected = conflict_fixture()?;
    let reference = append(&mut disk, &expected, None)?;
    let body = disk.read(reference)?.body()?.to_vec();
    for mutation in 0..8 {
        let mut bytes = body.clone();
        match mutation {
            0 => *bytes.first_mut().ok_or("missing tag")? = 255,
            1 => *bytes.get_mut(1).ok_or("missing outcome")? = 255,
            2 => bytes.truncate(3),
            3 => bytes.push(0),
            4 => bytes.get_mut(2..6).ok_or("missing count")?.fill(255),
            5 => *bytes.get_mut(6).ok_or("missing conflict kind")? = 255,
            6 => bytes.get_mut(7..11).ok_or("missing path length")?.fill(255),
            _ => {
                let expected = bytes.get(12..44).ok_or("missing expected commit")?.to_vec();
                bytes
                    .get_mut(44..76)
                    .ok_or("missing actual commit")?
                    .copy_from_slice(&expected);
            }
        }
        let bad = disk.append(PageKind::Fact, &bytes)?;
        assert!(read(&mut disk, bad).is_err(), "mutation {mutation}");
    }
    let invalid = IdempotencyOutcome::Append(AppendOutcome::Committed(AppendReceipt {
        start: 0,
        end: 0,
        tail: 0,
        commit_id: CommitId::default(),
    }));
    assert!(append(&mut disk, &invalid, None).is_err());
    assert!(append(&mut disk, &expected, Some(reference)).is_err());
    assert_eq!(read(&mut disk, reference)?.value, expected);
    Ok(())
}
