use super::*;
use crate::local::LocalDurability;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;
use std::task::Poll;

const MAXIMUM: u64 = 2 * 1024 * 1024;

fn poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut std::task::Context::from_waker(
        futures::task::noop_waker_ref(),
    ))
}

fn accept(store: &mut Store, decision: u64) -> Result<(), LocalStreamError> {
    let mut transaction = store.begin(MAXIMUM, None)?;
    transaction.state.decision = decision;
    transaction.accept()
}

#[tokio::test]
async fn accepted_and_durable_pushes_do_not_need_the_writer_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let mut observer = store.observe();
    {
        let changed = observer.changed();
        tokio::pin!(changed);
        assert!(futures::poll!(&mut changed).is_pending());
        accept(&mut store, 1)?;
        assert!(
            matches!(futures::poll!(&mut changed), Poll::Ready(Ok(()))),
            "accepted publication did not push before durability"
        );
    }
    let (cut, durable) = observer.snapshot()?;
    assert_eq!(cut.state.decision, 1);
    assert_eq!(durable.decision, 0);
    // The caller deliberately holds the mutation lock. Obtaining a read cut
    // through the observer cannot reenter that lock, even on the same thread.
    let writer = std::sync::Mutex::new(store);
    let mut locked = writer.lock().map_err(|_| "writer lock poisoned")?;
    assert_eq!(observer.snapshot()?.0.state.decision, 1);
    {
        let changed = observer.changed();
        tokio::pin!(changed);
        assert!(poll_once(changed.as_mut()).is_pending());
        locked.seal(None)?;
        assert!(
            matches!(poll_once(changed.as_mut()), Poll::Ready(Ok(()))),
            "durable publication did not push its watermark"
        );
    }
    drop(locked);
    let (cut, durable) = observer.snapshot()?;
    assert_eq!(cut.state, durable);
    Ok(())
}

#[tokio::test]
async fn a_snapshot_consumes_its_cut_and_preserves_the_next_publication_wakeup()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let mut observer = store.observe();
    accept(&mut store, 1)?;
    assert_eq!(observer.snapshot()?.0.state.decision, 1);
    {
        let changed = observer.changed();
        tokio::pin!(changed);
        assert!(
            futures::poll!(&mut changed).is_pending(),
            "snapshot left an already-consumed publication pending"
        );
        accept(&mut store, 2)?;
        assert!(
            matches!(futures::poll!(&mut changed), Poll::Ready(Ok(()))),
            "snapshot lost the next publication wakeup"
        );
    }
    assert_eq!(observer.snapshot()?.0.state.decision, 2);
    Ok(())
}

#[tokio::test]
async fn failed_durability_wakes_and_fences_existing_read_observers()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), MAXIMUM, LocalDurability::FullFlush)?;
    accept(&mut store, 1)?;
    let mut observer = store.observe();
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    {
        let changed = observer.changed();
        tokio::pin!(changed);
        assert!(futures::poll!(&mut changed).is_pending());
        meter.deny_durability.store(true, Ordering::SeqCst);
        assert!(store.seal(Some(Arc::clone(&work))).is_err());
        assert!(
            matches!(futures::poll!(&mut changed), Poll::Ready(Err(_))),
            "poisoned publication did not wake existing read observers"
        );
    }
    assert!(
        observer.snapshot().is_err(),
        "poisoned cut remained readable"
    );
    drop(store);
    drop(observer);
    drop(work);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn watched_cut_capture_performs_no_historical_work_at_one_to_one_thousand_accepts()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let mut observer = store.observe();
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let allocations = meter.allocations.load(Ordering::SeqCst);
    for decision in 1..=1_000 {
        accept(&mut store, decision)?;
        let (cut, durable) = observer.snapshot()?;
        assert_eq!(cut.state.decision, decision);
        assert_eq!(durable.decision, 0);
        let captured = store.capture(Some(Arc::clone(&work)))?;
        assert_eq!(captured.state, cut.state);
    }
    assert_eq!(
        meter.allocations.load(Ordering::SeqCst),
        allocations,
        "watch capture allocated historical working storage"
    );
    assert_eq!(
        meter.read_operations.load(Ordering::SeqCst),
        0,
        "watch capture performed historical disk work"
    );
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    drop(work);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn watched_accepted_records_are_readable_on_the_readers_ledger_under_the_writer_lock()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::local::cold::{engine::Validated, query};
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let mut observer = store.observe();
    let writer = Arc::new(Meter::local());
    let writer_work = crate::memory::Work::new(writer.original())?;
    let path = crate::StreamPath::new("tree/accepted")?;
    let request = crate::AppendRequest {
        path: path.clone(),
        records: vec![crate::OwnedBytes::from_static(b"visible before flush").into()],
        if_tail: Some(0),
        idempotency_key: None,
    };
    let validated = Validated::append(&request, Some(Arc::clone(&writer_work)))?;
    drop(validated.execute(&mut store, MAXIMUM, || Ok(()), || 7, None)?);
    drop(validated);
    let writer_reads = writer.read_operations.load(Ordering::SeqCst);
    writer.cancelled.store(true, Ordering::SeqCst);
    let mutex = std::sync::Mutex::new(store);
    let locked = mutex.lock().map_err(|_| "writer lock poisoned")?;
    let (cut, durable) = observer.snapshot()?;
    assert_eq!(durable.decision, 0);
    let state = cut.state;
    let reader = Arc::new(Meter::local());
    let reader_work = crate::memory::Work::new(reader.original())?;
    let mut captured = cut.reader(Some(Arc::clone(&reader_work)));
    let mut records = query::Records::open(
        &mut captured,
        state,
        &crate::ReadRequest {
            path,
            from: 0,
            limit: 1,
        },
    )?;
    let record = records
        .next(&mut captured)?
        .ok_or("accepted record unavailable before flush")?;
    assert_eq!(record.value.as_ref(), b"visible before flush");
    assert!(records.next(&mut captured)?.is_none());
    assert!(
        reader.read_operations.load(Ordering::SeqCst) > 0,
        "watched query bypassed its reader ledger"
    );
    assert_eq!(
        writer.read_operations.load(Ordering::SeqCst),
        writer_reads,
        "watched query reused the writer ledger"
    );
    assert_eq!(locked.durable_state()?.decision, 0);
    drop(record);
    drop(captured);
    drop(reader_work);
    drop(locked);
    drop(mutex);
    drop(observer);
    drop(writer_work);
    assert_eq!(reader.held.load(Ordering::SeqCst), 0);
    assert_eq!(writer.held.load(Ordering::SeqCst), 0);
    reader.assert_refunded();
    writer.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn failed_page_write_wakes_and_fences_existing_read_observers()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let mut observer = store.observe();
    let meter = Arc::new(Meter {
        late_write_error: true,
        ..Meter::local()
    });
    let work = crate::memory::Work::new(meter.original())?;
    {
        let changed = observer.changed();
        tokio::pin!(changed);
        assert!(futures::poll!(&mut changed).is_pending());
        let mut transaction = store.begin(MAXIMUM, Some(Arc::clone(&work)))?;
        assert!(transaction.append(PageKind::Root, b"failed page").is_err());
        assert!(
            matches!(futures::poll!(&mut changed), Poll::Ready(Err(_))),
            "failed page write did not fence existing read observers"
        );
    }
    assert!(
        meter.write_bytes.load(Ordering::SeqCst) > 0,
        "fixture never reached a physical write"
    );
    assert!(observer.snapshot().is_err());
    drop(store);
    drop(observer);
    drop(work);
    meter.assert_refunded();
    Ok(())
}

#[tokio::test]
async fn failed_locator_advance_wakes_and_fences_existing_read_observers()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let mut observer = store.observe();
    // Reads still work, but the actual locator write fails on this handle.
    // The new segment is prepared normally before that publication boundary.
    store.head = File::open(root.path().join(HEAD))?;
    {
        let changed = observer.changed();
        tokio::pin!(changed);
        assert!(futures::poll!(&mut changed).is_pending());
        assert!(store.rollover(None).is_err());
        assert!(store.failed, "fixture did not reach locator publication");
        assert!(
            matches!(futures::poll!(&mut changed), Poll::Ready(Err(_))),
            "failed locator advance did not fence existing read observers"
        );
    }
    assert!(observer.snapshot().is_err());
    Ok(())
}

#[tokio::test]
async fn preflight_denial_keeps_the_accepted_cut_readable_without_a_false_wakeup()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), MAXIMUM, LocalDurability::FullFlush)?;
    accept(&mut store, 1)?;
    let mut observer = store.observe();
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    meter.cancelled.store(true, Ordering::SeqCst);
    {
        let changed = observer.changed();
        tokio::pin!(changed);
        assert!(futures::poll!(&mut changed).is_pending());
        let mut transaction = store.begin(MAXIMUM, Some(Arc::clone(&work)))?;
        assert!(transaction.append(PageKind::Root, b"denied page").is_err());
        assert!(
            futures::poll!(&mut changed).is_pending(),
            "preflight denial invalidated the accepted publication"
        );
    }
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(observer.snapshot()?.0.state.decision, 1);
    drop(work);
    meter.assert_refunded();
    Ok(())
}
