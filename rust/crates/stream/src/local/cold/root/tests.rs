use super::super::tests::Disk;
use super::*;

#[test]
fn root_open_reads_one_page_at_every_historical_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let mut state = Root::default();
    let mut roots = Vec::new();
    for decision in 1_u64..=1_000 {
        let value = disk.append(PageKind::Fact, &decision.to_le_bytes())?;
        state.points = Some(index::put(&mut disk, state.points, b"head", value)?);
        state.decision = decision;
        state.committed_at_micros = decision * 1_000;
        roots.push((state.append(&mut disk)?, state));
    }
    for (reference, expected) in roots {
        disk.reads = 0;
        assert_eq!(Root::load(&mut disk, reference)?, expected);
        assert_eq!(disk.reads, 1);
    }
    Ok(())
}

#[test]
fn publication_slots_bind_generation_position_and_revision()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let reference = Root::default().append(&mut disk)?;
    let slot = Slot {
        revision: 4,
        root: reference,
    };
    let bytes = slot.encoded(7, 1);
    assert_eq!(Slot::decode(&bytes, 7, 1)?, Some(slot));
    assert_eq!(Slot::decode(&bytes, 6, 1)?, None);
    assert_eq!(Slot::decode(&bytes, 7, 0)?, None);
    assert_ne!(checksum(7, 1, b"slot"), checksum(7, 0, b"slot"));
    for position in 0..bytes.len() {
        let mut torn = bytes;
        *torn.get_mut(position).ok_or("invalid test position")? ^= 1;
        assert_eq!(Slot::decode(&torn, 7, 1)?, None);
    }
    let older = Slot {
        revision: 3,
        ..slot
    };
    assert_eq!(Slot::select(Some(older), Some(slot))?, slot);
    assert_eq!(Slot::select(None, Some(slot))?, slot);
    assert!(Slot::select(None, None).is_err());
    let other = Slot {
        root: Root::default().append(&mut disk)?,
        ..slot
    };
    assert!(Slot::select(Some(other), Some(slot)).is_err());
    Ok(())
}

#[test]
fn selected_complete_slot_never_rolls_back_after_root_corruption()
-> Result<(), Box<dyn std::error::Error>> {
    let mut disk = Disk::new(None)?;
    let older = Slot {
        revision: 1,
        root: Root::default().append(&mut disk)?,
    };
    let latest = Slot {
        revision: 2,
        root: Root {
            decision: 1,
            ..Root::default()
        }
        .append(&mut disk)?,
    };
    let selected = Slot::select(Some(older), Some(latest))?;
    acyclic_native_runtime::write_all_at(&disk.file, latest.root.offset, b"broken", None)?;
    assert!(Root::load(&mut disk, selected.root).is_err());
    // The forbidden fallback would quietly report an older, valid state.
    assert_eq!(Root::load(&mut disk, older.root)?.decision, 0);
    Ok(())
}
