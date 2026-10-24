use super::super::tests::Disk;
use super::*;
use crate::test_meter::Meter;
use crate::{OwnedBytes, RecordHeaders};
use std::sync::atomic::Ordering;

fn append(
    io: &mut impl PageIo,
    head: &mut Head,
    sequence: u64,
    size: usize,
) -> Result<(), LocalStreamError> {
    head.append_record(
        io,
        &Record {
            sequence,
            committed_at_micros: sequence,
            commit_id: CommitId::from_bytes(
                [u8::try_from(sequence % 251).map_err(|_| LocalStreamError::Corrupt)?; 32],
            ),
            value: OwnedBytes::from(vec![7; size]),
            headers: RecordHeaders::default(),
        },
    )?;
    Ok(())
}

#[test]
fn opening_and_resuming_at_one_to_one_thousand_records_never_loads_historical_payloads()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let mut disk = Disk::new(Some(work))?;
    let path = StreamPath::new("tasks/one")?;
    let mut state = Root {
        decision: 1,
        ..Default::default()
    };
    let mut head = Head {
        tail: 0,
        created_decision: 1,
        records: None,
    };
    for count in 1..=1_000 {
        append(&mut disk, &mut head, count - 1, 64)?;
        if ![1, 10, 100, 1_000].contains(&count) {
            continue;
        }
        catalog::put(&mut disk, &mut state, &path, head)?;
        let before = meter.read.load(Ordering::SeqCst);
        let request = ReadRequest {
            path: path.clone(),
            from: count - 1,
            limit: 1,
        };
        let mut cursor = Records::open(&mut disk, state, &request)?;
        assert!(
            meter.read.load(Ordering::SeqCst) - before < 2_048,
            "opening a read must not materialize earlier records"
        );
        anchor(
            &mut disk,
            head,
            count,
            Some(CommitId::from_bytes([u8::try_from((count - 1) % 251)?; 32])),
        )?;
        assert!(meter.read.load(Ordering::SeqCst) - before < 16_384);
        let record = cursor.next(&mut disk)?.ok_or("missing selected record")?;
        assert_eq!(record.sequence, count - 1);
        assert_eq!(record.value.as_ref(), &[7; 64]);
        assert!(cursor.next(&mut disk)?.is_none());
        drop(record);
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    drop(disk);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn a_read_keeps_its_captured_tail_and_original_record_root_after_later_publications()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let path = StreamPath::new("tasks/cut")?;
    let mut state = Root {
        decision: 1,
        ..Default::default()
    };
    let mut head = Head {
        tail: 0,
        created_decision: 1,
        records: None,
    };
    for sequence in 0..3 {
        append(&mut disk, &mut head, sequence, 3)?;
    }
    catalog::put(&mut disk, &mut state, &path, head)?;
    let captured = state;
    let mut bounded = Records::open(
        &mut disk,
        state,
        &ReadRequest {
            path: path.clone(),
            from: 1,
            limit: 1,
        },
    )?;
    assert_eq!(
        bounded
            .next(&mut disk)?
            .ok_or("missing bounded record")?
            .sequence,
        1
    );
    assert!(
        bounded.next(&mut disk)?.is_none(),
        "a finite read must stop at its requested bound"
    );
    let mut cursor = Records::open(
        &mut disk,
        state,
        &ReadRequest {
            path: path.clone(),
            from: 1,
            limit: 10,
        },
    )?;
    append(&mut disk, &mut head, 3, 4)?;
    catalog::put(&mut disk, &mut state, &path, head)?;
    assert_eq!(
        watermarks(&mut disk, state, captured, &path)?,
        StreamWatermarks {
            accepted: 4,
            durable: 3
        }
    );
    assert_eq!(cursor.next(&mut disk)?.ok_or("missing record")?.sequence, 1);
    assert_eq!(cursor.next(&mut disk)?.ok_or("missing record")?.sequence, 2);
    assert!(cursor.next(&mut disk)?.is_none());
    assert_eq!(
        watermarks(&mut disk, captured, Root::default(), &path)?,
        StreamWatermarks {
            accepted: 3,
            durable: 0
        }
    );
    assert!(watermarks(&mut disk, captured, state, &path).is_err());
    Ok(())
}

#[test]
fn invalid_bounds_missing_paths_and_forged_anchors_preserve_public_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let path = StreamPath::new("tasks/bounds")?;
    let mut state = Root {
        decision: 1,
        ..Default::default()
    };
    let mut head = Head {
        tail: 0,
        created_decision: 1,
        records: None,
    };
    append(&mut disk, &mut head, 0, 1)?;
    catalog::put(&mut disk, &mut state, &path, head)?;
    for (from, limit, expected) in [
        (0, 0, StreamError::LimitExceeded),
        (2, 1, StreamError::OutOfRange),
    ] {
        let result = Records::open(
            &mut disk,
            state,
            &ReadRequest {
                path: path.clone(),
                from,
                limit,
            },
        );
        assert!(matches!(result, Err(LocalStreamError::Replay(error)) if error == expected));
    }
    let missing = ReadRequest {
        path: StreamPath::new("absent")?,
        from: 0,
        limit: 1,
    };
    assert!(matches!(
        Records::open(&mut disk, state, &missing),
        Err(LocalStreamError::Replay(StreamError::NotFound))
    ));
    assert!(matches!(
        anchor(&mut disk, head, 0, Some(CommitId::default())),
        Err(LocalStreamError::Replay(StreamError::InvalidArgument))
    ));
    assert!(matches!(
        anchor(&mut disk, head, 1, Some(CommitId::from_bytes([8; 32]))),
        Err(LocalStreamError::Replay(StreamError::PrefixNotRetained))
    ));
    let mut empty = Records::open(
        &mut disk,
        state,
        &ReadRequest {
            path,
            from: 1,
            limit: 1,
        },
    )?;
    let before = disk.reads;
    assert!(empty.next(&mut disk)?.is_none());
    assert_eq!(disk.reads, before);
    Ok(())
}
