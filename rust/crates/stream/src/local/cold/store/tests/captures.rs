use super::*;

#[test]
fn capturing_one_to_one_thousand_accepted_cuts_never_allocates_or_opens_a_file()
-> Result<(), Box<dyn std::error::Error>> {
    for native in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut store = Store::open(
            directory.path(),
            32 * 1024 * 1024,
            LocalDurability::FullFlush,
        )?;
        let meter = Arc::new(Meter::local());
        let work = native
            .then(|| crate::memory::Work::new(meter.original()))
            .transpose()?;
        for sequence in 1..=1_000 {
            accept(&mut store, sequence, work.clone())?;
            if ![1, 10, 100, 1_000].contains(&sequence) {
                continue;
            }
            let reads = meter.read_operations.load(Ordering::SeqCst);
            let writes = meter.write_operations.load(Ordering::SeqCst);
            let held = meter.held.load(Ordering::SeqCst);
            meter.deny_allocation.store(true, Ordering::SeqCst);
            let mut captured = None;
            let observed = acyclic_test_support::measure(|| {
                captured = Some(store.capture(work.clone()));
            });
            let captured = captured.ok_or("capture was not called")?;
            assert!(
                captured.is_ok(),
                "capturing a cold root allocated or reopened storage"
            );
            let captured = captured?;
            assert_eq!(captured.state.decision, sequence);
            assert_eq!(
                observed.count_max, 0,
                "capturing a cold root allocated storage"
            );
            assert_eq!(observed.bytes_max, 0);
            assert_eq!(meter.read_operations.load(Ordering::SeqCst), reads);
            assert_eq!(meter.write_operations.load(Ordering::SeqCst), writes);
            assert_eq!(meter.held.load(Ordering::SeqCst), held);
            drop(captured);
            meter.deny_allocation.store(false, Ordering::SeqCst);
        }
        drop(store);
        drop(work);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }
    Ok(())
}

#[test]
fn a_captured_cut_reads_only_on_its_query_ledger_and_keeps_the_writer_handle_funded()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut store = Store::open(directory.path(), MAXIMUM, LocalDurability::FullFlush)?;
    let writer = Arc::new(Meter::local());
    let writer_work = crate::memory::Work::new(writer.original())?;
    for sequence in 1..=32 {
        accept(&mut store, sequence, Some(Arc::clone(&writer_work)))?;
    }
    assert!(
        store.active.number > 0,
        "fixture did not rotate its writer handle"
    );
    let handle_bytes = store.active.file.funded_bytes();
    assert!(handle_bytes > 0);
    let reader = Arc::new(Meter::local());
    let reader_work = crate::memory::Work::new(reader.original())?;
    let mut captured = store.capture(Some(Arc::clone(&reader_work)))?;
    let state = captured.state;
    let writer_reads = writer.read_operations.load(Ordering::SeqCst);
    writer.cancelled.store(true, Ordering::SeqCst);
    drop(writer_work);
    drop(store);
    assert_eq!(
        writer.held.load(Ordering::SeqCst),
        handle_bytes,
        "captured file retired its original writer funding"
    );
    let reference = index::get(&mut captured, state.points, &32_u64.to_be_bytes())?
        .ok_or("lost captured record")?;
    assert_eq!(record::read(&mut captured, reference)?, sample(32));
    assert!(
        reader.read_operations.load(Ordering::SeqCst) > 0,
        "captured query bypassed its own read ledger"
    );
    assert_eq!(
        writer.read_operations.load(Ordering::SeqCst),
        writer_reads,
        "captured query reused the cancelled writer ledger"
    );
    drop(captured);
    assert_eq!(writer.held.load(Ordering::SeqCst), 0);
    drop(reader_work);
    assert_eq!(reader.held.load(Ordering::SeqCst), 0);
    writer.assert_refunded();
    reader.assert_refunded();
    Ok(())
}
