use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use crate::{CommittedAppend, CommittedFork, OwnedBytes, RecordHeaders};
use std::sync::atomic::Ordering;

fn sample(sequence: u64) -> Record {
    Record {
        sequence,
        value: OwnedBytes::from_static(&[b'x'; 4_096]),
        headers: RecordHeaders::default(),
        commit_id: CommitId::from_bytes([7; 32]),
        committed_at_micros: 100,
    }
}

type Fixture = (CommittedEnvelope, Vec<Vec<PageRef>>);

fn fixture(disk: &mut Disk) -> Result<Fixture, Box<dyn std::error::Error>> {
    let expected = CommittedEnvelope {
        commit_id: CommitId::from_bytes([7; 32]),
        mutations: vec![
            CommittedMutation::Append(CommittedAppend {
                path: StreamPath::new("a")?,
                start: 3,
                end: 5,
                tail: 5,
                records: vec![sample(3), sample(4)],
            }),
            CommittedMutation::Fork(CommittedFork {
                source: StreamPath::new("origin")?,
                destination: StreamPath::new("b")?,
                forked_at: 5,
                tail: 6,
                records: vec![sample(5)],
            }),
            CommittedMutation::Fork(CommittedFork {
                source: StreamPath::new("a")?,
                destination: StreamPath::new("c")?,
                forked_at: 0,
                tail: 0,
                records: vec![],
            }),
        ],
    };
    let mut references = Vec::new();
    for mutation in &expected.mutations {
        let mut records = Vec::new();
        for value in parts(mutation)?.records {
            records.push(record::append(disk, value)?);
        }
        references.push(records);
    }
    Ok((expected, references))
}

fn reference_at(
    references: &[Vec<PageRef>],
    mutation: usize,
    record: usize,
) -> Result<PageRef, LocalStreamError> {
    references
        .get(mutation)
        .and_then(|records| records.get(record))
        .copied()
        .ok_or(LocalStreamError::Corrupt)
}

#[test]
fn envelope_descriptors_share_existing_payload_facts_and_read_only_their_own_batch()
-> Result<(), Box<dyn std::error::Error>> {
    for native in [false, true] {
        let meter = Arc::new(Meter::local());
        let work = native
            .then(|| crate::memory::Work::new(meter.original()))
            .transpose()?;
        let mut disk = Disk::new(work)?;
        let (expected, references) = fixture(&mut disk)?;
        let reference = append(&mut disk, &expected, |mutation, record| {
            reference_at(&references, mutation, record)
        })?;
        assert!(
            reference.bytes < 512,
            "descriptor must not duplicate the 12 KiB of values"
        );
        // Unrelated history is physically present but cannot be replayed by a
        // point read of the earlier envelope.
        for _ in 0..64 {
            record::append(&mut disk, &sample(0))?;
        }
        disk.reads = 0;
        let actual = read(&mut disk, reference, expected.commit_id)?;
        assert_eq!(actual.value, expected);
        assert_eq!(disk.reads, 4, "one descriptor plus its three new records");
        drop(disk);
        if native {
            assert!(meter.held.load(Ordering::SeqCst) > 12_288);
        }
        drop(actual);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }
    Ok(())
}

#[test]
fn every_partial_envelope_decode_retires_its_original_container_and_record_funding()
-> Result<(), Box<dyn std::error::Error>> {
    let mut reached_success = false;
    for allowance in 0..32 {
        let meter = Arc::new(Meter::local());
        let work = crate::memory::Work::new(meter.original())?;
        let mut disk = Disk::new(Some(work))?;
        let (expected, references) = fixture(&mut disk)?;
        let reference = append(&mut disk, &expected, |mutation, record| {
            reference_at(&references, mutation, record)
        })?;
        let baseline = meter.held.load(Ordering::SeqCst);
        let before = meter.allocations.load(Ordering::SeqCst);
        meter.limit.store(before + allowance, Ordering::SeqCst);
        let actual = read(&mut disk, reference, expected.commit_id);
        let success = actual.is_ok();
        if success {
            assert_eq!(
                actual.as_ref().map(|value| &value.value).ok(),
                Some(&expected)
            );
        }
        drop(actual);
        assert_eq!(
            meter.held.load(Ordering::SeqCst),
            baseline,
            "allowance {allowance}"
        );
        drop(disk);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        if success {
            reached_success = true;
            break;
        }
    }
    assert!(
        reached_success,
        "fixture must cover every allocation prefix through success"
    );
    Ok(())
}

#[test]
fn envelope_decoder_rejects_reauthenticated_wrong_identity_layout_and_record_bindings()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let (expected, references) = fixture(&mut disk)?;
    let reference = append(&mut disk, &expected, |mutation, record| {
        reference_at(&references, mutation, record)
    })?;
    assert!(read(&mut disk, reference, CommitId::from_bytes([8; 32])).is_err());
    let original = disk.read(reference)?.body()?.to_vec();
    for mutation in 0..7 {
        let mut bytes = original.clone();
        match mutation {
            0 => *bytes.first_mut().ok_or("missing tag")? = 255,
            1 => {
                bytes.pop();
            }
            2 => bytes.push(0),
            3 => bytes.get_mut(33..37).ok_or("missing count")?.fill(255),
            4 => *bytes.get_mut(37).ok_or("missing mutation")? = 255,
            5 => bytes
                .get_mut(38..42)
                .ok_or("missing path length")?
                .fill(255),
            _ => bytes.get_mut(43..51).ok_or("missing sequence")?.fill(0),
        }
        let forged = disk.append(PageKind::Fact, &bytes)?;
        assert!(
            read(&mut disk, forged, expected.commit_id).is_err(),
            "mutation {mutation}"
        );
    }
    // A structurally valid descriptor with another record's authenticated
    // address must still fail the sequence and envelope-identity checks.
    let wrong = append(&mut disk, &expected, |_, _| reference_at(&references, 1, 0))?;
    assert!(read(&mut disk, wrong, expected.commit_id).is_err());
    let mut noncanonical = expected;
    noncanonical.mutations.swap(0, 1);
    assert!(
        append(&mut disk, &noncanonical, |mutation, record| reference_at(
            &references,
            mutation,
            record
        ))
        .is_err()
    );
    Ok(())
}

#[test]
fn failed_record_reference_still_records_its_completed_metadata_copy_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(work))?;
    let (expected, references) = fixture(&mut disk)?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let copied = meter.copied.load(Ordering::SeqCst);
    let writes = meter.write_operations.load(Ordering::SeqCst);
    assert!(
        append(&mut disk, &expected, |mutation, record| {
            if record == 1 {
                return Err(LocalStreamError::Corrupt);
            }
            reference_at(&references, mutation, record)
        })
        .is_err()
    );
    assert_eq!(
        meter.copied.load(Ordering::SeqCst) - copied,
        37 + 1 + 4 + 1 + 8 + 4 + 52
    );
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), writes);
    assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    drop(disk);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}
