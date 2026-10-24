use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use crate::{AppendOutcome, CommitId, CommitOutcome};
use std::sync::atomic::Ordering;

#[test]
fn retry_family_and_digest_are_checked_before_the_terminal_fact_is_loaded()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(work))?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let key = IdempotencyKey::new(b"retry".to_vec())?;
    let digest = [3; 32];
    let mut state = Root::default();
    let expected = IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail: 5 });
    let reference = outcome::append(&mut disk, &expected, None)?;
    retained::put(
        &mut disk,
        &mut state,
        &key,
        retained::Binding {
            family: Family::Append,
            request_digest: digest,
            outcome: reference,
        },
    )?;
    disk.reads = 0;
    assert!(replay(&mut disk, state, None, Family::Append, digest)?.is_none());
    assert_eq!(disk.reads, 0);
    let replayed =
        replay(&mut disk, state, Some(&key), Family::Append, digest)?.ok_or("missing retry")?;
    assert_eq!(replayed.value, expected);
    assert_eq!(disk.reads, 3);
    drop(replayed);
    assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    for (family, supplied) in [
        (Family::Fork, digest),
        (Family::Commit, digest),
        (Family::Append, [4; 32]),
    ] {
        disk.reads = 0;
        assert!(matches!(
            replay(&mut disk, state, Some(&key), family, supplied),
            Err(LocalStreamError::Replay(
                crate::StreamError::IdempotencyMismatch
            ))
        ));
        assert_eq!(disk.reads, 2, "a mismatch cannot load a terminal payload");
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    drop(disk);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn a_matching_retry_binding_still_rejects_a_terminal_fact_from_another_family()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let key = IdempotencyKey::new(b"retry".to_vec())?;
    let mut state = Root::default();
    let conflict = crate::CommitConflict::Head {
        path: crate::StreamPath::new("source")?,
        expected: CommitId::from_bytes([2; 32]),
        actual: CommitId::from_bytes([1; 32]),
    };
    let reference = outcome::append(
        &mut disk,
        &IdempotencyOutcome::Commit(CommitOutcome::Conflict(vec![conflict])),
        None,
    )?;
    retained::put(
        &mut disk,
        &mut state,
        &key,
        retained::Binding {
            family: Family::Append,
            request_digest: [3; 32],
            outcome: reference,
        },
    )?;
    assert!(matches!(
        replay(&mut disk, state, Some(&key), Family::Append, [3; 32]),
        Err(LocalStreamError::Corrupt)
    ));
    Ok(())
}
