use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use crate::{CommitId, OwnedBytes, Record, RecordHeaders, StreamPath};
use std::sync::atomic::Ordering;

fn record(sequence: u64, value: &'static [u8]) -> Record {
    Record {
        sequence,
        value: OwnedBytes::from_static(value),
        commit_id: CommitId::from_bytes([7; 32]),
        committed_at_micros: 100,
        headers: RecordHeaders::default(),
    }
}

#[test]
fn every_fork_cut_masks_inherited_suffix_and_replacement_keeps_the_source_immutable()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let mut source = Head {
        tail: 0,
        created_decision: 1,
        records: None,
    };
    for sequence in 0..16 {
        source.append_record(&mut disk, &record(sequence, b"original"))?;
    }
    assert!(source.fork(17, 2).is_err());
    assert!(source.fork(0, 0).is_err());
    for cut in 0..=16 {
        let mut fork = source.fork(cut, 2)?;
        for sequence in 0..cut {
            assert_eq!(
                fork.record(&mut disk, sequence)?,
                Some(record(sequence, b"original"))
            );
        }
        disk.reads = 0;
        assert_eq!(fork.record(&mut disk, cut)?, None);
        assert_eq!(disk.reads, 0, "masked suffix must never be loaded");
        // The shared index physically still contains the suffix: omitting the
        // visibility cut makes this negative control expose the old record.
        if cut < 16 {
            assert!(index::get(&mut disk, fork.records, &cut.to_be_bytes())?.is_some());
        }
        fork.append_record(&mut disk, &record(cut, b"replacement"))?;
        assert_eq!(
            fork.record(&mut disk, cut)?,
            Some(record(cut, b"replacement"))
        );
        if cut < 16 {
            assert_eq!(
                source.record(&mut disk, cut)?,
                Some(record(cut, b"original"))
            );
        }
        assert_eq!(fork.record(&mut disk, cut + 1)?, None);
    }
    Ok(())
}

#[test]
fn one_thousand_forks_read_the_shared_record_without_walking_parent_history()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(work))?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let mut state = Root {
        decision: 1,
        ..Default::default()
    };
    let mut head = Head {
        tail: 0,
        created_decision: 1,
        records: None,
    };
    head.append_record(&mut disk, &record(0, b"original"))?;
    for decision in 1..=1_000 {
        state.decision = decision;
        head = head.fork(1, decision)?;
        let path = StreamPath::new(format!("fork-{decision}"))?;
        put(&mut disk, &mut state, &path, head)?;
        disk.reads = 0;
        let fetched = get(&mut disk, state, &path)?.ok_or("missing head")?;
        assert!(disk.reads <= 291, "point index has a fixed 32-byte key");
        disk.reads = 0;
        let actual = fetched
            .record(&mut disk, 0)?
            .ok_or("missing shared record")?;
        assert_eq!(actual, record(0, b"original"));
        assert_eq!(
            disk.reads, 2,
            "one sequence leaf and one fact, independent of fork depth"
        );
        drop(actual);
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    drop(disk);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn authenticated_path_head_cannot_be_rebound_or_accept_a_noncanonical_layout()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let path = StreamPath::new("tasks/source")?;
    let other = StreamPath::new("tasks/target")?;
    let head = Head {
        tail: 0,
        created_decision: 1,
        records: None,
    };
    let reference = head.append(&mut disk, &path)?;
    assert_eq!(Head::load(&mut disk, reference, &path)?, head);
    assert!(Head::load(&mut disk, reference, &other).is_err());
    let original = disk.read(reference)?.body()?.to_vec();
    for mutation in 0..6 {
        let mut bytes = original.clone();
        match mutation {
            0 => *bytes.first_mut().ok_or("missing tag")? = 255,
            1 => {
                bytes.pop();
            }
            2 => bytes.push(0),
            3 => bytes.get_mut(9..17).ok_or("missing creation")?.fill(0),
            4 => *bytes.get_mut(18).ok_or("missing absent reference")? = 1,
            _ => *bytes.get_mut(1).ok_or("missing tail")? = 1,
        }
        let forged = disk.append(PageKind::Fact, &bytes)?;
        assert!(
            Head::load(&mut disk, forged, &path).is_err(),
            "mutation {mutation}"
        );
    }
    let mut state = Root {
        decision: 1,
        ..Default::default()
    };
    put(&mut disk, &mut state, &path, head)?;
    state.decision = 0;
    assert!(get(&mut disk, state, &path).is_err());
    assert!(put(&mut disk, &mut state, &path, head).is_err());
    Ok(())
}
