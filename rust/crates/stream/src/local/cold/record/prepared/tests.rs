use super::*;
use crate::local::cold::tests::Disk;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

#[test]
fn prepared_record_reads_share_every_header_with_the_original_page_after_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    for (keys, attributes) in [
        (0, 0),
        (1, 1),
        (crate::MAX_RECORD_KEYS, crate::MAX_RECORD_ATTRIBUTES),
    ] {
        let mut disk = Disk::new(None)?;
        let expected = super::super::tests::sample(keys, attributes)?;
        let reference = append(&mut disk, &expected)?;
        let native = acyclic_native_runtime::NativeFile::from_file(disk.file, None)
            .map_err(|(error, _)| error)?;
        let observed = Arc::new(Meter::default());
        let work = crate::memory::Work::new(observed.original())?;
        let length = reference.bytes as usize;
        let mut pages = page::PreparedRead::new(work.clone(), &[length], length, 1)?;
        let record = super::super::PreparedRecord::new(&work)?;
        let operation = native.prepare_operation(&work.meter)?;
        observed.deny_allocation.store(true, Ordering::SeqCst);
        observed.deny_codec.store(true, Ordering::SeqCst);
        observed.cancelled.store(true, Ordering::SeqCst);
        let mut result = None;
        let allocated = acyclic_test_support::measure(|| {
            result = Some(operation.run(|file| Ok(record.read(&mut pages, file, reference))));
        });
        let actual = result.ok_or("missing prepared result")???;
        assert_eq!(
            allocated.count_max, 0,
            "prepared record decode allocated controls"
        );
        assert_eq!(actual, expected);
        assert_eq!(observed.read_operations.load(Ordering::SeqCst), 1);
        drop(pages);
        drop(work);
        assert!(observed.held.load(Ordering::SeqCst) > 0);
        assert_eq!(actual, expected);
        drop(actual);
        assert_eq!(observed.held.load(Ordering::SeqCst), 0);
        observed.assert_refunded();
    }
    Ok(())
}
