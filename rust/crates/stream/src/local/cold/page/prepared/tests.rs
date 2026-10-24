use super::*;
use crate::test_meter::Meter as Observed;
use std::sync::atomic::Ordering;

fn fixture() -> Result<(File, PageRef), LocalStreamError> {
    let file = tempfile::tempfile()?;
    let mut end = 0;
    let reference = append(&file, 0, &mut end, PageKind::Fact, b"payload", None)?;
    Ok((file, reference))
}

#[test]
fn a_thousand_prepared_page_reads_keep_one_original_buffer_and_flat_physical_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (file, reference) = fixture()?;
    let observed = Arc::new(Observed::default());
    let work = crate::memory::Work::new(observed.original())?;
    let native =
        acyclic_native_runtime::NativeFile::from_file(file, None).map_err(|(error, _)| error)?;
    let operation = native.prepare_operation(&work.meter)?;
    let length = reference.bytes as usize;
    let mut plan = PreparedRead::new(work.clone(), &[length], length * 1000, 1000)?;
    let withheld_hash = observed.withheld_codec().hashed;
    assert_eq!(
        withheld_hash,
        1000 * (u64::from(reference.bytes) + PAGE_DOMAIN.len() as u64 + 16),
        "prepared read hash credit was not withheld"
    );
    let allocations = observed.allocations.load(Ordering::SeqCst);
    let held = observed.held.load(Ordering::SeqCst);
    observed.deny_allocation.store(true, Ordering::SeqCst);
    observed.deny_codec.store(true, Ordering::SeqCst);
    observed.cancelled.store(true, Ordering::SeqCst);
    let mut result: Result<(), Box<dyn std::error::Error>> = Ok(());
    let allocation = acyclic_test_support::measure(|| {
        result = operation
            .run(|file| {
                Ok((|| {
                    let mut pointer = None;
                    for step in 1..=1000 {
                        let page = plan.read_prepared(file, reference).map_err(|error| {
                            format!("prepared page read requested fresh admission: {error}")
                        })?;
                        let body = page.into_body()?;
                        assert_eq!(body.as_slice(), b"payload");
                        assert_eq!(*pointer.get_or_insert(body.as_ptr()), body.as_ptr());
                        assert_eq!(observed.read_operations.load(Ordering::SeqCst), step);
                        assert_eq!(
                            observed.read.load(Ordering::SeqCst),
                            step * u64::from(reference.bytes)
                        );
                        assert_eq!(observed.allocations.load(Ordering::SeqCst), allocations);
                        assert_eq!(observed.held.load(Ordering::SeqCst), held);
                    }
                    Ok(())
                })())
            })
            .map_err(Into::into)
            .and_then(|result| result);
    });
    result?;
    assert_eq!(
        allocation.count_max, 0,
        "prepared page reads allocated storage"
    );
    assert_eq!(
        allocation.bytes_max, 0,
        "prepared page reads allocated storage"
    );
    drop(plan);
    drop(work);
    assert_eq!(observed.held.load(Ordering::SeqCst), 0);
    observed.assert_refunded();
    Ok(())
}

#[test]
fn escaped_pages_fence_their_slots_before_any_new_physical_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let (file, reference) = fixture()?;
    let observed = Arc::new(Observed::default());
    let work = crate::memory::Work::new(observed.original())?;
    let length = reference.bytes as usize;
    let mut plan = PreparedRead::new(work.clone(), &[length, length], length * 3, 3)?;
    let first = plan.read(&file, reference)?.into_body()?;
    let second = plan.read(&file, reference)?.into_body()?;
    assert!(plan.read(&file, reference).is_err());
    assert_eq!(observed.read_operations.load(Ordering::SeqCst), 2);
    assert_eq!(first.as_slice(), b"payload");
    let alias = first.slice(1..3);
    drop(first);
    assert!(plan.read(&file, reference).is_err());
    drop(alias);
    let last = plan.read(&file, reference)?.into_body()?;
    assert_eq!(observed.read_operations.load(Ordering::SeqCst), 3);
    drop(plan);
    drop(work);
    assert!(observed.held.load(Ordering::SeqCst) > 0);
    assert_eq!(second.as_slice(), b"payload");
    assert_eq!(last.as_slice(), b"payload");
    drop(second);
    drop(last);
    assert_eq!(observed.held.load(Ordering::SeqCst), 0);
    observed.assert_refunded();
    Ok(())
}

#[test]
fn exhausted_page_credit_stops_before_io_and_never_overdraws_original_reservations()
-> Result<(), Box<dyn std::error::Error>> {
    let (file, reference) = fixture()?;
    let length = reference.bytes as usize;
    for (bytes, attempts) in [(length, 2), (2 * length, 1)] {
        let observed = Arc::new(Observed::default());
        let work = crate::memory::Work::new(observed.original())?;
        let mut plan = PreparedRead::new(work.clone(), &[length], bytes, attempts)?;
        drop(plan.read(&file, reference)?);
        assert!(plan.read(&file, reference).is_err());
        assert_eq!(observed.read_operations.load(Ordering::SeqCst), 1);
        assert_eq!(
            observed.read.load(Ordering::SeqCst),
            u64::from(reference.bytes)
        );
        drop(plan);
        drop(work);
        observed.assert_refunded();
    }
    Ok(())
}

#[test]
fn prepared_page_authentication_and_short_reads_consume_only_original_credit()
-> Result<(), Box<dyn std::error::Error>> {
    let (file, reference) = fixture()?;
    let length = reference.bytes as usize;
    let observed = Arc::new(Observed::default());
    let work = crate::memory::Work::new(observed.original())?;
    let mut plan = PreparedRead::new(work.clone(), &[length], 2 * length, 2 * length as u64)?;
    observed.cancelled.store(true, Ordering::SeqCst);
    let mut invalid = reference;
    for byte in &mut invalid.digest {
        *byte ^= 1;
    }
    assert!(matches!(
        plan.read(&file, invalid),
        Err(LocalStreamError::Corrupt)
    ));
    assert_eq!(
        observed.read.load(Ordering::SeqCst),
        u64::from(reference.bytes)
    );
    let page = plan.read_with(reference, |offset, bytes| {
        let count = bytes.len().min(3);
        acyclic_native_runtime::read_at(
            &file,
            offset,
            bytes
                .get_mut(..count)
                .ok_or(std::io::ErrorKind::InvalidInput)?,
        )
    })?;
    assert_eq!(page.body()?, b"payload");
    assert_eq!(
        observed.read.load(Ordering::SeqCst),
        2 * u64::from(reference.bytes)
    );
    assert_eq!(
        observed.read_operations.load(Ordering::SeqCst),
        1 + u64::from(reference.bytes).div_ceil(3)
    );
    drop(page);
    drop(plan);
    drop(work);
    observed.assert_refunded();
    Ok(())
}

#[test]
fn declared_page_bounds_cannot_spend_allocator_spare_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let (file, reference) = fixture()?;
    let length = reference.bytes as usize;
    let observed = Arc::new(Observed::default());
    let work = crate::memory::Work::new(observed.original())?;
    let mut plan = PreparedRead::new(work.clone(), &[length], length, 1)?;
    // Model an allocator retaining more physical storage than the declared
    // page bound. The logical bound remains authoritative even before I/O.
    plan.buffers
        .value
        .first_mut()
        .ok_or("missing prepared slot")?
        .maximum = length - 1;
    assert!(
        plan.read(&file, reference).is_err(),
        "page read spent allocator spare capacity"
    );
    assert_eq!(observed.read_operations.load(Ordering::SeqCst), 0);
    drop(plan);
    drop(work);
    observed.assert_refunded();
    Ok(())
}

#[test]
fn a_late_prepared_read_failure_keeps_its_completed_physical_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let (file, reference) = fixture()?;
    let length = reference.bytes as usize;
    let observed = Arc::new(Observed::local());
    let work = crate::memory::Work::new(observed.original())?;
    let mut plan = PreparedRead::new(work.clone(), &[length], length, length as u64)?;
    let result = plan.read_with(reference, |offset, bytes| {
        let prefix = bytes.get_mut(..3).ok_or(std::io::ErrorKind::InvalidInput)?;
        let count = acyclic_native_runtime::read_at(&file, offset, prefix)?;
        observed.cancelled.store(true, Ordering::SeqCst);
        Ok(count)
    });
    assert!(result.is_err());
    assert_eq!(observed.read_operations.load(Ordering::SeqCst), 1);
    assert_eq!(
        observed.read.load(Ordering::SeqCst),
        3,
        "prepared read lost its physical prefix"
    );
    drop(plan);
    let original = work
        .finish::<(), crate::StreamError>(Err(crate::StreamError::Unavailable))
        .err()
        .ok_or("late read failure lost its original cause")?;
    assert_eq!(original.kind(), std::io::ErrorKind::Interrupted);
    drop(work);
    observed.assert_refunded();
    Ok(())
}
