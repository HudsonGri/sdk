use super::*;
use crate::test_meter::Meter;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

pub(in crate::local::cold) struct Disk {
    pub(in crate::local::cold) file: File,
    end: u64,
    pub(in crate::local::cold) reads: usize,
    work: Option<Arc<crate::memory::Work>>,
}

impl Disk {
    pub(in crate::local::cold) fn new(
        work: Option<Arc<crate::memory::Work>>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            file: tempfile::tempfile()?,
            end: 0,
            reads: 0,
            work,
        })
    }
}

impl PageRead for Disk {
    fn work(&self) -> Option<&Arc<crate::memory::Work>> {
        self.work.as_ref()
    }
    fn read(&mut self, reference: PageRef) -> Result<Page, LocalStreamError> {
        self.reads += 1;
        page::read(&self.file, reference, self.work())
    }
}
impl PageIo for Disk {
    fn append(&mut self, kind: PageKind, body: &[u8]) -> Result<PageRef, LocalStreamError> {
        page::append(&self.file, 0, &mut self.end, kind, body, self.work.as_ref())
    }
}

#[test]
fn ordered_index_covers_prefixes_zero_bytes_replacements_and_historical_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let mut root = None;
    let mut oracle = BTreeMap::new();
    let mut keys = vec![Vec::new()];
    for a in [0, 1, 128, 255] {
        keys.push(vec![a]);
        for b in [0, 1, 128, 255] {
            keys.push(vec![a, b]);
        }
    }
    let mut historical = Vec::new();
    // Reverse insertion exercises splits above existing compressed branches.
    for key in keys.iter().rev().chain(keys.iter()) {
        let value = disk.append(PageKind::Fact, key)?;
        root = Some(index::put(&mut disk, root, key, value)?);
        oracle.insert(key.clone(), value);
        historical.push((root, oracle.clone()));
    }
    let mut probes = keys;
    probes.extend([
        vec![0, 0, 0],
        vec![0, 2],
        vec![127],
        vec![128, 255, 255],
        vec![255, 255, 0],
    ]);
    for (root, oracle) in historical {
        let first = index::next(&mut disk, root, None)?;
        assert_eq!(
            first.as_ref().map(index::entry).transpose()?,
            oracle
                .first_key_value()
                .map(|(key, value)| (key.as_slice(), *value))
        );
        for probe in &probes {
            assert_eq!(
                index::get(&mut disk, root, probe)?,
                oracle.get(probe).copied()
            );
            let found = index::next(&mut disk, root, Some(probe))?;
            let expected = oracle
                .iter()
                .find(|(key, _)| key.as_slice() > probe.as_slice());
            assert_eq!(
                found.as_ref().map(index::entry).transpose()?,
                expected.map(|(key, value)| (key.as_slice(), *value))
            );
        }
    }
    Ok(())
}

#[test]
fn index_queries_and_working_buffers_do_not_retain_historical_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let retained = meter.held.load(Ordering::SeqCst);
    let mut disk = Disk::new(Some(work))?;
    let mut root = None;
    let mut expected = None;
    for sequence in 0_u64..1_000 {
        let key = sequence.to_be_bytes();
        let fact = disk.append(PageKind::Fact, &key)?;
        root = Some(index::put(&mut disk, root, &key, fact)?);
        assert_eq!(meter.held.load(Ordering::SeqCst), retained);
        expected = Some(fact);
    }
    disk.reads = 0;
    assert_eq!(
        index::get(&mut disk, root, &999_u64.to_be_bytes())?,
        expected
    );
    assert!(disk.reads <= 8 * 9 + 1);
    // A linear scan must examine all 1,000 facts to find the last one; the
    // index follows only bounded key decisions. This is an algorithm control,
    // not a measurement of the existing MemoryStream's balanced indexes.
    let old_scan = (0_u64..1_000)
        .position(|sequence| sequence == 999)
        .ok_or("missing negative control")?
        + 1;
    assert!(old_scan > 8 * 9 + 1);
    assert_eq!(meter.held.load(Ordering::SeqCst), retained);
    drop(disk);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn cold_page_address_hash_short_read_and_late_cancellation_preserve_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let mut disk = Disk::new(Some(Arc::clone(&work)))?;
    let reference = disk.append(PageKind::Fact, b"body")?;
    let page = disk.read(reference)?;
    assert_eq!(page.body()?, b"body");
    assert_eq!(
        meter.held.load(Ordering::SeqCst),
        baseline
            + u64::from(reference.bytes)
            + crate::OwnedBytes::vec_guard_header_bytes::<acyclic_native_runtime::ReadAllocation>()
    );
    drop(page);
    assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    for reference in [
        PageRef {
            segment: 1,
            ..reference
        },
        PageRef {
            offset: 1,
            ..reference
        },
    ] {
        assert!(disk.read(reference).is_err());
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    let before = meter.read.load(Ordering::SeqCst);
    disk.file.set_len(u64::from(reference.bytes) - 2)?;
    assert!(disk.read(reference).is_err());
    assert_eq!(
        meter.read.load(Ordering::SeqCst) - before,
        u64::from(reference.bytes) - 2
    );
    assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    drop(disk);
    drop(work);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();

    let mut limits = Meter::local();
    limits.cancel_after_read = 1;
    let meter = Arc::new(limits);
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(Arc::clone(&work)))?;
    let reference = disk.append(PageKind::Fact, b"cancel")?;
    let before = meter.read.load(Ordering::SeqCst);
    assert!(disk.read(reference).is_err());
    assert_eq!(
        meter.read.load(Ordering::SeqCst) - before,
        u64::from(reference.bytes)
    );
    let original = work
        .finish::<(), crate::StreamError>(Err(crate::StreamError::Unavailable))
        .err()
        .ok_or("lost original cancellation")?;
    assert!(original.to_string().contains("cancelled"));
    drop(disk);
    drop(work);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn cold_page_allocation_denial_precedes_any_file_transfer() -> Result<(), Box<dyn std::error::Error>>
{
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let mut disk = Disk::new(Some(Arc::clone(&work)))?;
    let reference = disk.append(PageKind::Fact, b"before denial")?;
    let end = disk.end;
    let reads = meter.read.load(Ordering::SeqCst);
    let writes = meter.write_bytes.load(Ordering::SeqCst);
    let retained = meter.held.load(Ordering::SeqCst);
    meter.deny_allocation.store(true, Ordering::SeqCst);
    assert!(disk.read(reference).is_err());
    assert!(disk.append(PageKind::Fact, b"denied").is_err());
    assert_eq!(disk.end, end);
    assert_eq!(disk.file.metadata()?.len(), end);
    assert_eq!(meter.read.load(Ordering::SeqCst), reads);
    assert_eq!(meter.write_bytes.load(Ordering::SeqCst), writes);
    assert_eq!(meter.held.load(Ordering::SeqCst), retained);
    let original = work
        .finish::<(), crate::StreamError>(Err(crate::StreamError::Unavailable))
        .err()
        .ok_or("lost original allocation denial")?;
    assert!(original.to_string().contains("allocation"));
    drop(disk);
    drop(work);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

mod properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn arbitrary_insert_order_and_missing_key_successors_match_the_ordered_oracle(
            keys in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..12), 0..32),
            probes in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..12), 0..32),
        ) {
            let mut disk = Disk::new(None)?;
            let mut root = None;
            let mut oracle = BTreeMap::new();
            for key in &keys {
                let value = disk.append(PageKind::Fact, key)?;
                root = Some(index::put(&mut disk, root, key, value)?);
                oracle.insert(key.clone(), value);
            }
            for probe in probes.iter().chain(&keys) {
                prop_assert_eq!(index::get(&mut disk, root, probe)?, oracle.get(probe).copied());
                let found = index::next(&mut disk, root, Some(probe))?;
                let expected = oracle.iter().find(|(key, _)| key.as_slice() > probe.as_slice());
                prop_assert_eq!(found.as_ref().map(index::entry).transpose()?, expected.map(|(key, value)| (key.as_slice(), *value)));
            }
        }

        #[test]
        fn arbitrary_deletions_preserve_other_keys_successors_and_historical_roots(
            keys in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..12), 0..32),
            removals in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..12), 0..32),
        ) {
            let mut disk = Disk::new(None)?;
            let mut root = None;
            let mut oracle = BTreeMap::new();
            for key in &keys {
                let value = disk.append(PageKind::Fact, key)?;
                root = Some(index::put(&mut disk, root, key, value)?);
                oracle.insert(key.clone(), value);
            }
            for key in removals.iter().chain(&keys) {
                let historical = root;
                let old = oracle.remove(key);
                root = index::remove(&mut disk, root, key)?;
                prop_assert_eq!(index::get(&mut disk, historical, key)?, old);
                prop_assert_eq!(index::get(&mut disk, root, key)?, None);
                if old.is_some() {
                    // Keeping the old root is the no-deletion control.
                    prop_assert_ne!(root, historical);
                } else {
                    prop_assert_eq!(root, historical);
                }
                for probe in removals.iter().chain(&keys) {
                    prop_assert_eq!(index::get(&mut disk, root, probe)?, oracle.get(probe).copied());
                    let found = index::next(&mut disk, root, Some(probe))?;
                    let expected = oracle.iter().find(|(key, _)| key.as_slice() > probe.as_slice());
                    prop_assert_eq!(found.as_ref().map(index::entry).transpose()?, expected.map(|(key, value)| (key.as_slice(), *value)));
                }
            }
            prop_assert_eq!(root, None);
        }
    }
}

#[test]
fn native_index_deletion_retires_traversal_capacity_without_retiring_old_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let mut disk = Disk::new(Some(work))?;
    let mut root = None;
    for sequence in 0_u64..1_000 {
        let key = sequence.to_be_bytes();
        let value = disk.append(PageKind::Fact, &key)?;
        root = Some(index::put(&mut disk, root, &key, value)?);
    }
    let historical = root;
    for sequence in 0_u64..1_000 {
        let key = sequence.to_be_bytes();
        disk.reads = 0;
        root = index::remove(&mut disk, root, &key)?;
        assert!(disk.reads <= 8 * 9 + 1);
        assert_eq!(index::get(&mut disk, root, &key)?, None);
        assert!(index::get(&mut disk, historical, &key)?.is_some());
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    }
    assert_eq!(root, None);
    drop(disk);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}
