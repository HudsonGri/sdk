use super::*;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

fn prepare(
    meter: &Arc<Meter>,
    length: usize,
) -> Result<ReusableOwnedBytes<ReadAllocation>, Box<dyn std::error::Error>> {
    let original = meter.original();
    let bytes = vec![0; length];
    let allocation = ReadAllocation::new(&original, bytes.capacity())?;
    ReusableOwnedBytes::new(bytes, allocation, &original)
        .map_err(|failure| format!("{:?}", failure.into_error()).into())
}

#[test]
fn a_thousand_page_loans_reuse_exact_storage_after_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::default());
    let mut buffer = prepare(&meter, 4096)?;
    let held = meter.held.load(Ordering::SeqCst);
    let allocations = meter.allocations.load(Ordering::SeqCst);
    meter.cancelled.store(true, Ordering::SeqCst);
    meter.deny_allocation.store(true, Ordering::SeqCst);
    meter.deny_codec.store(true, Ordering::SeqCst);
    let mut result: Result<(), Box<dyn std::error::Error>> = Ok(());
    let observed = acyclic_test_support::measure(|| {
        result = (|| {
            let mut original = None;
            let mut value = 1_u8;
            for step in 1..=1000 {
                let bytes = buffer
                    .try_writable()
                    .ok_or("previous page loan is retained")?;
                let pointer = bytes.as_ptr();
                assert_eq!(*original.get_or_insert(pointer), pointer);
                bytes.fill(value);
                let length = step % 4097;
                let page = buffer
                    .snapshot(length)
                    .ok_or("page prefix exceeds storage")?;
                assert_eq!(
                    page.as_ptr(),
                    pointer,
                    "page loan copied its preallocated bytes"
                );
                assert_eq!(page.len(), length);
                assert!(page.iter().all(|byte| *byte == value));
                assert!(
                    buffer.try_writable().is_none(),
                    "published page was mutable"
                );
                drop(page);
                value = value.wrapping_add(1);
            }
            Ok(())
        })();
    });
    result?;
    assert_eq!(observed.count_max, 0, "reusing a page allocated storage");
    assert_eq!(observed.bytes_max, 0, "reusing a page allocated storage");
    assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
    assert_eq!(meter.held.load(Ordering::SeqCst), held);
    drop(buffer);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn escaped_slices_fence_reuse_and_retain_original_custody() -> Result<(), Box<dyn std::error::Error>>
{
    let meter = Arc::new(Meter::default());
    let mut buffer = prepare(&meter, 8)?;
    buffer
        .try_writable()
        .ok_or("new buffer is shared")?
        .copy_from_slice(b"abcdefgh");
    assert!(buffer.snapshot(9).is_none());
    let page = buffer.snapshot(8).ok_or("missing page")?;
    let slice = page.slice(2..6);
    let alias = slice.clone();
    drop(page);
    assert!(buffer.try_writable().is_none());
    drop(slice);
    assert!(buffer.try_writable().is_none());
    assert_eq!(alias.as_slice(), b"cdef");
    drop(alias);
    buffer
        .try_writable()
        .ok_or("last slice did not release storage")?
        .fill(42);
    let page = buffer.snapshot(8).ok_or("missing reused page")?;
    let held = meter.held.load(Ordering::SeqCst);
    drop(buffer);
    assert_eq!(meter.held.load(Ordering::SeqCst), held);
    assert_eq!(page.as_slice(), &[42; 8]);
    drop(page);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn empty_reusable_pages_still_fence_their_ownership_header()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::default());
    let mut buffer = prepare(&meter, 0)?;
    let page = buffer.snapshot(0).ok_or("missing empty page")?;
    assert!(buffer.try_writable().is_none());
    assert!(buffer.snapshot(1).is_none());
    drop(page);
    assert_eq!(buffer.try_writable(), Some([].as_mut_slice()));
    drop(buffer);
    meter.assert_refunded();
    Ok(())
}
