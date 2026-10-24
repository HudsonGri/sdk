use super::*;
use crate::local::LocalDurability;
use crate::test_meter::Meter;
use std::sync::atomic::Ordering;

const MAXIMUM: u64 = 1024 * 1024;

struct Active<'a> {
    segment: &'a mut Segment,
    work: Option<Arc<crate::memory::Work>>,
}
impl PageRead for Active<'_> {
    fn work(&self) -> Option<&Arc<crate::memory::Work>> {
        self.work.as_ref()
    }
    fn read(&mut self, reference: PageRef) -> Result<Page, LocalStreamError> {
        if reference.segment != self.segment.number
            || reference.offset < HEADER_BYTES as u64
            || reference
                .offset
                .checked_add(u64::from(reference.bytes))
                .is_none_or(|end| end > self.segment.end)
        {
            return Err(LocalStreamError::Corrupt);
        }
        page::read(&self.segment.file, reference, self.work())
    }
}
impl PageIo for Active<'_> {
    fn append(&mut self, kind: PageKind, body: &[u8]) -> Result<PageRef, LocalStreamError> {
        self.segment.append(kind, body, self.work.as_ref())
    }
}

fn append_root(segment: &mut Segment, decision: u64) -> Result<Slot, LocalStreamError> {
    let root = root::Root {
        decision,
        ..Default::default()
    }
    .append(&mut Active {
        segment,
        work: None,
    })?;
    Ok(Slot {
        revision: decision,
        root,
    })
}

#[test]
fn segment_reopen_reads_fixed_metadata_and_selected_root_at_one_to_one_thousand_publications()
-> Result<(), Box<dyn std::error::Error>> {
    let file = tempfile::tempfile()?;
    let mut segment = Segment::create(file.try_clone()?, 0, None, MAXIMUM, None)?;
    let mut roots = Vec::new();
    for decision in 1..=1_000 {
        let slot = append_root(&mut segment, decision)?;
        roots.push(slot.root);
        segment.publish(slot, LocalDurability::FullFlush, None)?;
        if [1, 10, 100, 1_000].contains(&decision) {
            let meter = Arc::new(Meter::local());
            let work = crate::memory::Work::new(meter.original())?;
            let mut reopened = Segment::open(file.try_clone()?, 0, MAXIMUM, Some(&work))?;
            assert_eq!(reopened.published()?, slot);
            let state = root::Root::load(
                &mut Active {
                    segment: &mut reopened,
                    work: Some(Arc::clone(&work)),
                },
                slot.root,
            )?;
            assert_eq!(state.decision, decision);
            reopened.discard_unpublished(Some(&work))?;
            assert_eq!(meter.read_operations.load(Ordering::SeqCst), 3);
            assert_eq!(
                meter.read.load(Ordering::SeqCst),
                HEADER_BYTES as u64 + u64::from(slot.root.bytes)
            );
            if decision == 1_000 {
                let bounded_bytes = meter.read.load(Ordering::SeqCst);
                let bounded_operations = meter.read_operations.load(Ordering::SeqCst);
                // Full-publication recovery is the scan control. Measure its
                // actual authenticated reads, rather than estimating a cost.
                for reference in &roots {
                    root::Root::load(
                        &mut Active {
                            segment: &mut reopened,
                            work: Some(Arc::clone(&work)),
                        },
                        *reference,
                    )?;
                }
                assert_eq!(
                    meter.read_operations.load(Ordering::SeqCst) - bounded_operations,
                    decision
                );
                assert!(meter.read.load(Ordering::SeqCst) - bounded_bytes > bounded_bytes * 100);
            }
            drop(reopened);
            drop(work);
            assert_eq!(meter.held.load(Ordering::SeqCst), 0);
            meter.assert_refunded();
        }
    }
    Ok(())
}

#[test]
fn segment_rollover_inherits_the_exact_root_and_recovery_discards_only_unpublished_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let file = tempfile::tempfile()?;
    let mut segment = Segment::create(file.try_clone()?, 3, None, MAXIMUM, None)?;
    let slot = append_root(&mut segment, 1)?;
    segment.publish(slot, LocalDurability::FullFlush, None)?;
    segment.append(PageKind::Fact, b"unpublished", None)?;
    let mut reopened = Segment::open(file.try_clone()?, 3, MAXIMUM, None)?;
    assert_eq!(reopened.published()?, slot);
    assert_eq!(
        root::Root::load(
            &mut Active {
                segment: &mut reopened,
                work: None
            },
            slot.root
        )?
        .decision,
        1
    );
    reopened.discard_unpublished(None)?;
    assert_eq!(file.metadata()?.len(), reopened.end);
    let next_file = tempfile::tempfile()?;
    let next = Segment::create(next_file.try_clone()?, 4, Some(slot), MAXIMUM, None)?;
    physical::sync(&next.file, LocalDurability::FullFlush, None)?;
    let next = Segment::open(next_file, 4, MAXIMUM, None)?;
    assert_eq!(next.published()?, slot);
    assert_eq!(next.end, HEADER_BYTES as u64);
    assert!(Segment::open(file.try_clone()?, 4, MAXIMUM, None).is_err());
    assert!(Segment::create(tempfile::tempfile()?, 3, Some(slot), MAXIMUM, None).is_err());
    Ok(())
}

#[test]
fn selected_segment_root_corruption_is_not_recovered_by_an_older_valid_slot()
-> Result<(), Box<dyn std::error::Error>> {
    let file = tempfile::tempfile()?;
    let mut segment = Segment::create(file.try_clone()?, 0, None, MAXIMUM, None)?;
    let old = append_root(&mut segment, 1)?;
    segment.publish(old, LocalDurability::FullFlush, None)?;
    let latest = append_root(&mut segment, 2)?;
    segment.publish(latest, LocalDurability::FullFlush, None)?;
    acyclic_native_runtime::write_all_at(&file, latest.root.offset, b"!", None)?;
    let mut reopened = Segment::open(file.try_clone()?, 0, MAXIMUM, None)?;
    assert_eq!(reopened.published()?, latest);
    assert!(
        root::Root::load(
            &mut Active {
                segment: &mut reopened,
                work: None
            },
            latest.root
        )
        .is_err()
    );
    // Falling back would succeed, silently losing acknowledged state.
    assert_eq!(
        root::Root::load(
            &mut Active {
                segment: &mut reopened,
                work: None
            },
            old.root
        )?
        .decision,
        1
    );
    Ok(())
}

#[test]
fn native_late_write_failure_poison_is_owned_by_the_segment()
-> Result<(), Box<dyn std::error::Error>> {
    let mut segment = Segment::create(tempfile::tempfile()?, 0, None, MAXIMUM, None)?;
    let slot = append_root(&mut segment, 1)?;
    segment.publish(slot, LocalDurability::FullFlush, None)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    meter.cancel_on_write.store(true, Ordering::SeqCst);
    assert!(
        segment
            .append(PageKind::Fact, b"accepted prefix", Some(&work))
            .is_err()
    );
    let bytes = meter.write_bytes.load(Ordering::SeqCst);
    assert!(bytes >= b"accepted prefix".len() as u64);
    assert!(matches!(
        segment.append(PageKind::Fact, b"again", None),
        Err(LocalStreamError::IncompleteFrame)
    ));
    assert!(segment.published().is_err());
    assert!(
        segment
            .publish(slot, LocalDurability::FullFlush, None)
            .is_err()
    );
    assert!(segment.discard_unpublished(None).is_err());
    // A caller-only poison flag cannot prevent this unguarded primitive.
    let mut end = segment.end;
    assert!(page::append(&segment.file, 0, &mut end, PageKind::Fact, b"bypass", None).is_ok());
    drop(work);
    assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn page_preparation_denial_performs_no_write_and_keeps_the_segment_available()
-> Result<(), Box<dyn std::error::Error>> {
    let mut segment = Segment::create(tempfile::tempfile()?, 0, None, MAXIMUM, None)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    meter.deny_allocation.store(true, Ordering::SeqCst);
    assert!(
        segment
            .append(PageKind::Fact, b"denied", Some(&work))
            .is_err()
    );
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert!(segment.append(PageKind::Fact, b"admitted", None).is_ok());
    drop(work);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn derived_native_paths_retain_capacity_and_deny_before_allocating()
-> Result<(), Box<dyn std::error::Error>> {
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    let baseline = meter.held.load(Ordering::SeqCst);
    let path = physical::path(
        std::path::Path::new("fixture"),
        "0000000000000000",
        Some(&work),
    )?;
    assert_eq!(
        path.value,
        std::path::Path::new("fixture").join("0000000000000000")
    );
    assert_eq!(
        meter.held.load(Ordering::SeqCst),
        baseline + path.value.capacity() as u64
    );
    drop(path);
    assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    meter.deny_allocation.store(true, Ordering::SeqCst);
    assert!(physical::path(std::path::Path::new("fixture"), "next", Some(&work)).is_err());
    assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
    drop(work);
    meter.assert_refunded();
    Ok(())
}
