use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

#[test]
fn native_page_buffers_preallocate_their_freeze_headers_before_io()
-> Result<(), Box<dyn std::error::Error>> {
    for length in [0, 1, 127, 4096] {
        let meter = Arc::new(Meter::local());
        let work = crate::memory::Work::new(meter.original())?;
        let mut buffer = Buffer::new(length, Some(&work))?;
        let baseline = meter.allocations.load(Ordering::SeqCst);
        buffer.bytes.resize(length, 42)?;
        meter.deny_allocation.store(true, Ordering::SeqCst);
        let bytes = buffer
            .freeze()
            .map_err(|_| "page freeze allocated after completed I/O")?;
        assert_eq!(bytes.as_slice(), vec![42; length]);
        assert_eq!(meter.allocations.load(Ordering::SeqCst), baseline);
        assert!(meter.held.load(Ordering::SeqCst) > 0);
        drop(work);
        // The frozen payload, including its header, still owns the caller's
        // original allocation ledger after its request driver has gone away.
        assert!(meter.held.load(Ordering::SeqCst) > 0);
        drop(bytes);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
    }
    Ok(())
}

#[test]
fn cold_page_body_freezes_without_late_allocation_and_keeps_its_exact_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let file = tempfile::tempfile()?;
    let mut end = 0;
    let reference = super::super::append(&file, 0, &mut end, PageKind::Fact, b"payload", None)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let page = super::super::read(&file, reference, Some(&work))?;
    let baseline = meter.allocations.load(Ordering::SeqCst);
    meter.deny_allocation.store(true, Ordering::SeqCst);
    let bytes = page
        .into_body()
        .map_err(|_| "page freeze allocated after completed I/O")?;
    assert_eq!(bytes.as_slice(), b"payload");
    assert_eq!(meter.allocations.load(Ordering::SeqCst), baseline);
    drop(work);
    assert!(meter.held.load(Ordering::SeqCst) > 0);
    drop(bytes);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn page_encoders_cannot_exceed_their_original_admitted_bound()
-> Result<(), Box<dyn std::error::Error>> {
    for length in [0, 1, 127, 4096] {
        let meter = Arc::new(Meter::local());
        let work = crate::memory::Work::new(meter.original())?;
        for owner in [None, Some(&work)] {
            let mut buffer = Buffer::new(length, owner)?;
            let capacity = buffer.bytes.capacity();
            let allocations = meter.allocations.load(Ordering::SeqCst);
            buffer.bytes.resize(length, 42)?;
            assert!(matches!(
                buffer.bytes.push(1),
                Err(LocalStreamError::InvalidLimits)
            ));
            assert!(matches!(
                buffer.bytes.extend_from_slice(&[1, 2]),
                Err(LocalStreamError::InvalidLimits)
            ));
            assert!(matches!(
                buffer.bytes.resize(length + 1, 1),
                Err(LocalStreamError::InvalidLimits)
            ));
            assert_eq!(buffer.bytes.len(), length);
            assert!(buffer.bytes.iter().all(|byte| *byte == 42));
            assert_eq!(buffer.bytes.capacity(), capacity);
            assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
            buffer.bytes.resize(0, 0)?;
            buffer.bytes.resize(length, 43)?;
            assert_eq!(buffer.freeze()?.as_slice(), vec![43; length]);
        }
        drop(work);
        meter.assert_refunded();
    }
    Ok(())
}
#[test]
fn denied_page_ownership_header_is_observed_before_any_physical_read()
-> Result<(), Box<dyn std::error::Error>> {
    let file = tempfile::tempfile()?;
    let mut end = 0;
    let reference = super::super::append(&file, 0, &mut end, PageKind::Fact, b"payload", None)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let held = meter.held.load(Ordering::SeqCst);
    // Admit the page payload but deny its escaped ownership header.
    meter.limit.store(
        meter.allocations.load(Ordering::SeqCst) + 1,
        Ordering::SeqCst,
    );
    let result = super::super::read(&file, reference, Some(&work));
    assert!(
        result.is_err(),
        "read completed before its ownership header was admitted"
    );
    assert_eq!(meter.read_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.read.load(Ordering::SeqCst), 0);
    assert_eq!(meter.held.load(Ordering::SeqCst), held);
    let original = work
        .finish::<(), crate::StreamError>(Err(crate::StreamError::Unavailable))
        .err()
        .ok_or("header admission lost its original error")?;
    assert_eq!(original.kind(), std::io::ErrorKind::OutOfMemory);
    drop(work);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}
