use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

#[test]
fn retry_bindings_are_account_scoped_immutable_and_bounded_across_one_thousand_decisions()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(work))?;
    let mut state = Root::default();
    let first = IdempotencyKey::new(0_u64.to_be_bytes().to_vec())?;
    let outcome = disk.append(PageKind::Fact, b"outcome")?;
    let expected = Binding {
        family: Family::Append,
        request_digest: [7; 32],
        outcome,
    };
    for position in 0..1_000_u64 {
        let identity = IdempotencyKey::new(position.to_be_bytes().to_vec())?;
        put(&mut disk, &mut state, &identity, expected)?;
        let held = meter.held.load(Ordering::SeqCst);
        disk.reads = 0;
        assert_eq!(get(&mut disk, state, &first)?, Some(expected));
        assert!(
            disk.reads <= 291,
            "SHA-256 key width bounds reads, not retained decisions"
        );
        assert_eq!(meter.held.load(Ordering::SeqCst), held);
    }
    let bytes = disk.file.metadata()?.len();
    put(&mut disk, &mut state, &first, expected)?;
    assert_eq!(
        disk.file.metadata()?.len(),
        bytes,
        "retry writes no duplicate fact"
    );
    let before = state;
    for family in [Family::Fork, Family::Commit] {
        assert!(
            put(
                &mut disk,
                &mut state,
                &first,
                Binding { family, ..expected }
            )
            .is_err()
        );
        assert_eq!(state, before);
    }
    assert!(
        put(
            &mut disk,
            &mut state,
            &first,
            Binding {
                request_digest: [8; 32],
                ..expected
            }
        )
        .is_err()
    );
    assert_eq!(get(&mut disk, state, &first)?, Some(expected));
    let missing = IdempotencyKey::new(b"missing".to_vec())?;
    assert_eq!(get(&mut disk, state, &missing)?, None);
    drop(disk);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn rebound_retry_identity_and_reauthenticated_descriptor_layouts_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let mut state = Root::default();
    let identity = IdempotencyKey::new(b"a".to_vec())?;
    let other = IdempotencyKey::new(b"b".to_vec())?;
    let outcome = disk.append(PageKind::Fact, b"outcome")?;
    let binding = Binding {
        family: Family::Commit,
        request_digest: [9; 32],
        outcome,
    };
    put(&mut disk, &mut state, &identity, binding)?;
    let identity_key = key(0, identity.as_bytes(), None)?;
    let reference = index::get(&mut disk, state.points, &identity_key)?.ok_or("missing retry")?;
    let other_key = key(0, other.as_bytes(), None)?;
    let mut forged = state;
    forged.points = Some(index::put(&mut disk, state.points, &other_key, reference)?);
    assert!(get(&mut disk, forged, &other).is_err());
    let body = disk.read(reference)?.body()?.to_vec();
    for kind in 0..7 {
        let mut changed = body.clone();
        match kind {
            0 => *changed.first_mut().ok_or("missing tag")? = 255,
            1 => *changed.get_mut(1).ok_or("missing family")? = 255,
            2 => changed.truncate(3),
            3 => changed.push(0),
            4 => *changed.get_mut(2).ok_or("missing length")? = 255,
            5 => *changed.get_mut(4).ok_or("missing key")? = b'b',
            // A freshly authenticated page may not reference itself/future.
            _ => {
                let start = changed.len().checked_sub(52).ok_or("missing reference")?;
                changed
                    .get_mut(start..start + 8)
                    .ok_or("missing segment")?
                    .copy_from_slice(&u64::MAX.to_le_bytes());
            }
        }
        let bad = disk.append(PageKind::Fact, &changed)?;
        let mut forged = state;
        forged.points = Some(index::put(&mut disk, state.points, &identity_key, bad)?);
        assert!(
            get(&mut disk, forged, &identity).is_err(),
            "layout case {kind}"
        );
    }
    assert_eq!(get(&mut disk, state, &identity)?, Some(binding));
    Ok(())
}

#[test]
fn immutable_commit_directory_uses_the_envelopes_own_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let mut state = Root::default();
    let commit = CommitId::from_bytes([1; 32]);
    let empty = crate::CommittedEnvelope {
        commit_id: commit,
        mutations: vec![crate::CommittedMutation::Fork(crate::CommittedFork {
            source: crate::StreamPath::new("source")?,
            destination: crate::StreamPath::new("target")?,
            forked_at: 0,
            tail: 0,
            records: vec![],
        })],
    };
    let reference = envelope::append(&mut disk, &empty, |_, _| Err(LocalStreamError::Corrupt))?;
    put_envelope(&mut disk, &mut state, commit, reference)?;
    assert_eq!(
        envelope(&mut disk, state, commit)?
            .ok_or("missing envelope")?
            .value,
        empty
    );
    let bytes = disk.file.metadata()?.len();
    put_envelope(&mut disk, &mut state, commit, reference)?;
    assert_eq!(disk.file.metadata()?.len(), bytes);
    let forged = CommitId::from_bytes([2; 32]);
    assert!(envelope(&mut disk, state, forged)?.is_none());
    put_envelope(&mut disk, &mut state, forged, reference)?;
    assert!(envelope(&mut disk, state, forged).is_err());
    let another = disk.append(PageKind::Fact, b"another")?;
    assert!(put_envelope(&mut disk, &mut state, commit, another).is_err());
    Ok(())
}

#[test]
fn observation_checks_the_terminal_outcome_family_without_loading_other_decisions()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let mut state = Root::default();
    let identity = IdempotencyKey::new(b"retry".to_vec())?;
    let expected =
        crate::IdempotencyOutcome::Append(crate::AppendOutcome::TailConflict { actual_tail: 17 });
    let outcome = outcome::append(&mut disk, &expected, None)?;
    let binding = Binding {
        family: Family::Append,
        request_digest: [1; 32],
        outcome,
    };
    put(&mut disk, &mut state, &identity, binding)?;
    disk.reads = 0;
    let observed = observe(&mut disk, state, &identity)?.ok_or("missing observation")?;
    assert_eq!(
        observed.value,
        crate::IdempotencyObservation {
            idempotency_key: identity.clone(),
            request_digest: [1; 32],
            outcome: expected,
        }
    );
    assert_eq!(
        disk.reads, 3,
        "index leaf, retry descriptor and selected compact outcome"
    );
    let wrong = IdempotencyKey::new(b"wrong-family".to_vec())?;
    put(
        &mut disk,
        &mut state,
        &wrong,
        Binding {
            family: Family::Commit,
            ..binding
        },
    )?;
    assert!(observe(&mut disk, state, &wrong).is_err());
    Ok(())
}
