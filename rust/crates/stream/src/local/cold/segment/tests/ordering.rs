use super::*;
use std::io::{Read, Seek, SeekFrom};

fn header(file: &File) -> std::io::Result<[u8; HEADER_BYTES]> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0; HEADER_BYTES];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

#[test]
fn data_flush_denial_cannot_expose_a_new_recovery_slot() -> Result<(), Box<dyn std::error::Error>> {
    let file = tempfile::tempfile()?;
    let mut segment = Segment::create(file.try_clone()?, 0, None, MAXIMUM, None)?;
    let previous = append_root(&mut segment, 1)?;
    segment.publish(previous, LocalDurability::FullFlush, None)?;
    let before = header(&file)?;
    let next = append_root(&mut segment, 2)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    meter.deny_durability.store(true, Ordering::SeqCst);
    assert!(
        segment
            .publish(next, LocalDurability::FullFlush, Some(&work))
            .is_err()
    );
    assert_eq!(
        header(&file)?,
        before,
        "a slot cannot precede its data flush"
    );
    assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
    let reopened = Segment::open(file.try_clone()?, 0, MAXIMUM, None)?;
    assert_eq!(reopened.published()?, previous);
    assert!(
        segment.published().is_err(),
        "failed publication fences the writer"
    );
    drop(work);
    meter.assert_refunded();
    Ok(())
}

#[test]
fn every_torn_slot_prefix_selects_an_authenticated_durable_root()
-> Result<(), Box<dyn std::error::Error>> {
    let file = tempfile::tempfile()?;
    let mut segment = Segment::create(file.try_clone()?, 0, None, MAXIMUM, None)?;
    for decision in 1..=2 {
        let slot = append_root(&mut segment, decision)?;
        segment.publish(slot, LocalDurability::FullFlush, None)?;
    }
    let before = header(&file)?;
    let next = append_root(&mut segment, 3)?;
    let meter = Arc::new(Meter::local());
    let work = crate::memory::Work::new(meter.original())?;
    segment.publish(next, LocalDurability::FullFlush, Some(&work))?;
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 2);
    assert_eq!(meter.write_bytes.load(Ordering::SeqCst), SLOT_BYTES as u64);
    let mut complete = Vec::new();
    let mut input = file.try_clone()?;
    input.seek(SeekFrom::Start(0))?;
    input.read_to_end(&mut complete)?;
    let offset = PREFIX_BYTES + SLOT_BYTES;
    // Data is durable before any slot byte can be written. Enumerate every
    // persisted prefix of the subsequent slot overwrite, including none/all.
    for prefix in 0..=SLOT_BYTES {
        let mut image = complete.clone();
        image
            .get_mut(..HEADER_BYTES)
            .ok_or("missing recovery header")?
            .copy_from_slice(&before);
        let published_prefix = complete
            .get(offset..offset + prefix)
            .ok_or("missing published slot")?;
        image
            .get_mut(offset..offset + prefix)
            .ok_or("missing torn slot")?
            .copy_from_slice(published_prefix);
        let crash = tempfile::tempfile()?;
        acyclic_native_runtime::write_all_at(&crash, 0, &image, None)?;
        let mut recovered = Segment::open(crash, 0, MAXIMUM, None)?;
        let selected = recovered.published()?;
        assert!([2, 3].contains(&selected.revision), "prefix {prefix}");
        let state = root::Root::load(
            &mut Active {
                segment: &mut recovered,
                work: None,
            },
            selected.root,
        )?;
        assert_eq!(state.decision, selected.revision, "prefix {prefix}");
        if prefix == SLOT_BYTES {
            assert_eq!(selected, next);
        }
    }
    drop(work);
    meter.assert_refunded();
    Ok(())
}
