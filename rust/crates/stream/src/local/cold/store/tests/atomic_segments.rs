use super::*;

fn stage(
    store: &mut Store,
    count: u64,
    work: Option<Arc<crate::memory::Work>>,
) -> Result<Transaction<'_>, LocalStreamError> {
    let mut transaction = store.begin(256 * 1024, work)?;
    for sequence in 0..count {
        let reference = record::append(&mut transaction, &sample(sequence))?;
        let points = transaction.state.points;
        transaction.state.points = Some(index::put(
            &mut transaction,
            points,
            &sequence.to_be_bytes(),
            reference,
        )?);
        transaction.state.decision = sequence + 1;
    }
    Ok(transaction)
}

#[test]
fn an_atomic_publication_can_cross_segments_without_publishing_an_intermediate_root()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let maximum = 4_096;
    let mut store = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let staged = stage(&mut store, 64, Some(Arc::clone(&work)));
    assert!(
        staged.is_ok(),
        "a transaction budget is not a segment capacity"
    );
    let transaction = staged?;
    assert!(
        transaction.store.active.number > 1,
        "fixture did not cross segments"
    );
    assert_eq!(transaction.store.state()?.decision, 0);
    assert_eq!(transaction.store.durable_state()?.decision, 0);
    transaction.publish()?;
    drop(store);
    let mut reopened = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    let state = reopened.state()?;
    assert_eq!(state.decision, 64);
    let mut reader = reopened.reader(Some(Arc::clone(&work)));
    for sequence in 0..64_u64 {
        let reference = index::get(&mut reader, state.points, &sequence.to_be_bytes())?
            .ok_or("lost cross-segment record")?;
        assert_eq!(record::read(&mut reader, reference)?, sample(sequence));
    }
    drop(reader);
    drop(reopened);
    drop(work);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn abandoning_a_cross_segment_transaction_preserves_the_last_acknowledged_cut()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let maximum = 4_096;
    let mut store = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    write(&mut store, 1, None)?;
    let acknowledged = store.state()?;
    let transaction = stage(&mut store, 64, None)?;
    assert!(transaction.store.active.number > 1);
    assert_eq!(transaction.store.state()?, acknowledged);
    drop(transaction);
    drop(store);
    let mut reopened = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    assert_eq!(reopened.state()?, acknowledged);
    let mut reader = reopened.reader(None);
    assert!(index::get(&mut reader, acknowledged.points, &63_u64.to_be_bytes())?.is_none());
    Ok(())
}

#[test]
fn rollover_can_seal_prior_accepted_state_without_exposing_a_private_transaction()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let maximum = 4_096;
    let mut store = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    for sequence in 1..=4 {
        accept(&mut store, sequence, None)?;
    }
    let accepted = store.state()?;
    let transaction = stage(&mut store, 64, None)?;
    assert!(transaction.store.active.number > 1);
    assert_eq!(transaction.store.state()?, accepted);
    assert_eq!(transaction.store.durable_state()?, accepted);
    drop(transaction);
    drop(store);
    let mut reopened = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    assert_eq!(reopened.state()?, accepted);
    let mut reader = reopened.reader(None);
    assert!(index::get(&mut reader, accepted.points, &63_u64.to_be_bytes())?.is_none());
    Ok(())
}

#[test]
fn a_page_larger_than_a_segment_is_denied_without_rotation_or_physical_io()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let maximum = 4_096;
    let mut store = Store::open(directory.path(), maximum, LocalDurability::FullFlush)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut transaction = store.begin(256 * 1024, Some(Arc::clone(&work)))?;
    let before = transaction.store.active.number;
    let body = vec![0; usize::try_from(maximum)?];
    assert!(matches!(
        transaction.append(PageKind::Fact, &body),
        Err(LocalStreamError::InvalidLimits)
    ));
    assert_eq!(transaction.store.active.number, before);
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    drop(transaction);
    drop(store);
    drop(work);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn predecessor_flush_denial_cannot_create_a_later_segment_or_advance_its_locator()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), 4_096, LocalDurability::FullFlush)?;
    let mut transaction = store.begin(256 * 1024, None)?;
    record::append(&mut transaction, &sample(0))?;
    drop(transaction);
    let previous = store.state()?;
    let number = store.active.number;
    let next = segment_path(directory.path(), number + 1, None)?;
    assert!(!next.value.try_exists()?);
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    meter.deny_durability.store(true, Ordering::SeqCst);
    assert!(store.rollover(Some(&work)).is_err());
    assert_eq!(
        meter.write_operations.load(Ordering::SeqCst),
        0,
        "private pages must flush before the next segment is created"
    );
    assert_eq!(store.active.number, number);
    assert_eq!(store.state()?, previous);
    assert!(!next.value.try_exists()?);
    drop(store);
    drop(work);
    meter.assert_refunded();
    let reopened = Store::open(directory.path(), 4_096, LocalDurability::FullFlush)?;
    assert_eq!(reopened.state()?, previous);
    Ok(())
}
