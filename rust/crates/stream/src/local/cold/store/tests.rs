use super::*;
use crate::local::LocalDurability;
use crate::test_meter::Meter;
use crate::{CommitId, OwnedBytes, Record, RecordHeaders};
use std::sync::atomic::Ordering;

mod atomic_segments;

const MAXIMUM: u64 = 8 * 1024;

fn sample(sequence: u64) -> Record {
    let mut commit = [0; 32];
    if let Some(bytes) = commit.get_mut(..8) {
        bytes.copy_from_slice(&sequence.to_be_bytes());
    }
    Record {
        sequence,
        committed_at_micros: sequence * 1_000,
        commit_id: CommitId::from_bytes(commit),
        value: OwnedBytes::from_static(b"body"),
        headers: RecordHeaders::default(),
    }
}

fn write(
    store: &mut Store,
    sequence: u64,
    work: Option<Arc<crate::memory::Work>>,
) -> Result<Slot, LocalStreamError> {
    let mut transaction = store.begin(4_096, work)?;
    let reference = record::append(&mut transaction, &sample(sequence))?;
    let old = transaction.state.points;
    transaction.state.points = Some(index::put(
        &mut transaction,
        old,
        &sequence.to_be_bytes(),
        reference,
    )?);
    transaction.state.decision = sequence;
    transaction.state.committed_at_micros = sequence * 1_000;
    transaction.publish()
}

#[test]
fn cold_store_rollover_reopen_and_historical_roots_keep_exact_records_on_the_original_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    assert!(matches!(
        Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush),
        Err(LocalStreamError::AlreadyOpen)
    ));
    let meter = Arc::new(Meter {
        memory_limit: 64 * 1024,
        ..Meter::local()
    });
    let work = crate::memory::Work::new(meter.original())?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let mut roots = Vec::new();
    for sequence in 1..=32 {
        write(&mut store, sequence, Some(Arc::clone(&work)))?;
        let state = store.state()?;
        roots.push((sequence, state));
        let mut reader = store.reader(Some(Arc::clone(&work)));
        let reference = index::get(&mut reader, state.points, &sequence.to_be_bytes())?
            .ok_or("missing record")?;
        assert_eq!(record::read(&mut reader, reference)?, sample(sequence));
        drop(reader);
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    assert!(
        store.active.number > 0,
        "fixture must actually seal segments"
    );
    drop(store);
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    assert_eq!(store.state()?.decision, 32);
    for (sequence, state) in roots {
        let mut reader = store.reader(Some(Arc::clone(&work)));
        let reference = index::get(&mut reader, state.points, &sequence.to_be_bytes())?
            .ok_or("missing historical record")?;
        assert_eq!(record::read(&mut reader, reference)?, sample(sequence));
    }
    drop(store);
    drop(work);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn abandoned_transaction_and_native_preparation_denial_never_advance_the_selected_cut()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let initial = store.state()?;
    let initial_end = store.active.end;
    {
        let mut transaction = store.begin(4_096, None)?;
        record::append(&mut transaction, &sample(1))?;
    }
    assert!(store.active.end > initial_end);
    assert_eq!(store.state()?, initial);
    drop(store);
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    assert_eq!(store.state()?, initial);
    assert_eq!(store.active.end, initial_end);
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    meter.deny_allocation.store(true, Ordering::SeqCst);
    assert!(write(&mut store, 1, Some(Arc::clone(&work))).is_err());
    assert_eq!(store.state()?, initial);
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    write(&mut store, 1, None)?;
    assert_eq!(store.state()?.decision, 1);
    drop(store);
    drop(work);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn corrupt_selected_store_root_and_missing_selected_segment_never_fall_back()
-> Result<(), Box<dyn std::error::Error>> {
    for missing_segment in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
        let older = write(&mut store, 1, None)?;
        let latest = write(&mut store, 2, None)?;
        assert_eq!(Root::load(&mut store.reader(None), older.root)?.decision, 1);
        if missing_segment {
            locator::advance(&store.head, 0, 1, None)?;
            physical::sync(&store.head, LocalDurability::FullFlush, None)?;
        } else {
            acyclic_native_runtime::write_all_at(
                &store.active.file,
                latest.root.offset,
                b"!",
                None,
            )?;
            physical::sync(&store.active.file, LocalDurability::FullFlush, None)?;
        }
        drop(store);
        assert!(Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush).is_err());
        // The old root remains readable, making a fallback an observable loss.
        let file = open_segment(directory.path(), 0, Access::Read, None)?;
        assert!(page::read(&file, older.root, None).is_ok());
    }
    Ok(())
}

#[test]
fn failed_publication_keeps_its_original_cause_and_fences_subsequent_store_use()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    meter.deny_durability.store(true, Ordering::SeqCst);
    assert!(write(&mut store, 1, Some(Arc::clone(&work))).is_err());
    assert!(store.state().is_err());
    assert!(store.begin(4_096, None).is_err());
    let original = work
        .finish::<(), crate::StreamError>(Err(crate::StreamError::Unavailable))
        .err()
        .ok_or("lost original cause")?;
    assert!(original.to_string().contains("durability"));
    drop(store);
    drop(work);
    meter.assert_refunded();
    let store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    // The first data flush was denied before any recovery slot was written.
    assert_eq!(store.state()?.decision, 0);
    Ok(())
}

#[test]
fn native_directory_ordering_is_a_durability_attempt_and_storage_bounds_are_explicit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    physical::sync_directory(directory.path(), LocalDurability::FullFlush, Some(&work))?;
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 1);
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    drop(work);
    meter.assert_refunded();
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    assert!(store.begin(root::ROOT_BYTES - 1, None).is_err());
    drop(store);
    assert!(Store::open(directory.path(), MAXIMUM + 1, LocalDurability::FullFlush).is_err());
    Ok(())
}

fn accept(
    store: &mut Store,
    sequence: u64,
    work: Option<Arc<crate::memory::Work>>,
) -> Result<(), LocalStreamError> {
    let mut transaction = store.begin(4_096, work)?;
    let reference = record::append(&mut transaction, &sample(sequence))?;
    let points = transaction.state.points;
    transaction.state.points = Some(index::put(
        &mut transaction,
        points,
        &sequence.to_be_bytes(),
        reference,
    )?);
    transaction.state.decision = sequence;
    transaction.accept()
}

#[test]
fn one_thousand_accepted_roots_leave_recovery_slots_untouched_until_one_phase_barrier()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let maximum = 32 * 1024 * 1024;
    let mut store = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let mut header = vec![0; usize::try_from(segment::DATA_START)?];
    page::read_exact(&store.active.file, 0, &mut header, None)?;
    for sequence in 1..=1_000 {
        accept(&mut store, sequence, Some(Arc::clone(&work)))?;
        assert_eq!(store.state()?.decision, sequence);
        assert_eq!(store.durable_state()?.decision, 0);
        assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    let mut after = vec![0; header.len()];
    page::read_exact(&store.active.file, 0, &mut after, None)?;
    assert_eq!(
        header, after,
        "accepted roots cannot overwrite either durable slot"
    );
    store.seal(Some(Arc::clone(&work)))?;
    let sealed = store.state()?;
    assert_eq!(store.durable_state()?, sealed);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 2);
    let writes = meter.write_operations.load(Ordering::SeqCst);
    store.seal(Some(Arc::clone(&work)))?;
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), writes);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 2);
    for sequence in 1_001..=1_016 {
        accept(&mut store, sequence, Some(Arc::clone(&work)))?;
    }
    assert_eq!(store.state()?.decision, 1_016);
    assert_eq!(store.durable_state()?, sealed);
    drop(store);
    let mut store = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    assert_eq!(
        store.state()?,
        sealed,
        "unsealed accepted pages never become recoverable roots"
    );
    let mut reader = store.reader(Some(Arc::clone(&work)));
    let reference = index::get(&mut reader, sealed.points, &1_000_u64.to_be_bytes())?
        .ok_or("lost durable record")?;
    assert_eq!(record::read(&mut reader, reference)?, sample(1_000));
    assert!(index::get(&mut reader, sealed.points, &1_001_u64.to_be_bytes())?.is_none());
    drop(reader);
    drop(store);
    drop(work);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn rollover_seals_the_accepted_predecessor_before_advancing_the_locator()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let mut rollovers = 0;
    for sequence in 1..=32 {
        let previous_segment = store.active.number;
        let previous = store.state()?;
        accept(&mut store, sequence, None)?;
        if store.active.number != previous_segment {
            rollovers += 1;
            assert_eq!(store.durable_state()?, previous);
        }
    }
    assert!(rollovers > 0);
    let durable = store.durable_state()?;
    drop(store);
    let store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    assert_eq!(store.state()?, durable);
    Ok(())
}
