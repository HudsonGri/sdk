//! An active segment publishes an authenticated root in alternating fixed slots.
//! Sealed segments keep the same immutable page addresses.
use super::*;
use root::Slot;

mod header;
#[cfg(test)]
mod tests;

use header::{HEADER_BYTES, PREFIX_BYTES, SLOT_BYTES};
pub(super) const DATA_START: u64 = HEADER_BYTES as u64;

pub(super) struct Segment {
    pub(super) file: super::handle::SharedFile,
    pub(super) number: u64,
    pub(super) end: u64,
    maximum: u64,
    published: Option<Slot>,
    poisoned: bool,
}

impl Segment {
    /// The owner must make this segment's directory entry durable before
    /// advancing its locator. An inherited slot keeps the previous root live
    /// through rollover without walking any earlier segment.
    pub(super) fn create(
        file: File,
        number: u64,
        inherited: Option<Slot>,
        maximum: u64,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        if maximum <= HEADER_BYTES as u64
            || inherited.is_some_and(|slot| slot.root.segment >= number)
        {
            return Err(LocalStreamError::InvalidLimits);
        }
        let file = super::handle::SharedFile::new(file, work)?;
        let hash_bytes = header::hash_bytes(u64::from(inherited.is_some()));
        page::codec(work, HEADER_BYTES as u64, hash_bytes, true)?;
        let bytes = header::encode(number, inherited)?;
        page::codec(work, HEADER_BYTES as u64, hash_bytes, false)?;
        page::observe(
            work,
            acyclic_native_runtime::write_all_at(
                &file,
                0,
                &bytes,
                work.map(|work| (work.meter.as_ref(), false)),
            ),
        )?;
        Ok(Self {
            file,
            number,
            end: HEADER_BYTES as u64,
            maximum,
            published: inherited,
            poisoned: false,
        })
    }

    /// Opens the selected segment from fixed metadata only. Reachable page
    /// authentication belongs to the store; a corrupt selected root must fail
    /// there rather than falling back to an older slot.
    pub(super) fn open(
        file: File,
        number: u64,
        maximum: u64,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        let file = super::handle::SharedFile::new(file, work)?;
        let mut bytes = [0; HEADER_BYTES];
        page::read_exact(&file, 0, &mut bytes, work)?;
        page::codec(work, 0, header::hash_bytes(2), true)?;
        let published = header::decode(&bytes, number);
        page::codec(work, 0, header::hash_bytes(2), false)?;
        let published = published?;
        let end = if published.root.segment == number {
            if published.root.offset < HEADER_BYTES as u64 {
                return Err(LocalStreamError::Corrupt);
            }
            published
                .root
                .offset
                .checked_add(u64::from(published.root.bytes))
                .ok_or(LocalStreamError::Corrupt)?
        } else {
            HEADER_BYTES as u64
        };
        if end > maximum {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(Self {
            file,
            number,
            end,
            maximum,
            published: Some(published),
            poisoned: false,
        })
    }

    pub(super) fn published(&self) -> Result<Slot, LocalStreamError> {
        if self.poisoned {
            return Err(LocalStreamError::IncompleteFrame);
        }
        self.published.ok_or(LocalStreamError::Corrupt)
    }

    /// Truncation is explicit and follows authentication of the selected root.
    /// It never guesses a recovery point by scanning or ignoring bad pages.
    pub(super) fn discard_unpublished(
        &self,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<(), LocalStreamError> {
        if self.poisoned {
            return Err(LocalStreamError::IncompleteFrame);
        }
        let length = physical::operation(work, false, || {
            self.file.metadata().map(|metadata| metadata.len())
        })?;
        if length < self.end {
            return Err(LocalStreamError::Corrupt);
        }
        if length > self.end {
            physical::operation(work, true, || self.file.set_len(self.end))?;
        }
        Ok(())
    }

    pub(super) fn remaining(&self) -> u64 {
        self.maximum.saturating_sub(self.end)
    }

    pub(super) fn append(
        &mut self,
        kind: PageKind,
        body: &[u8],
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<PageRef, LocalStreamError> {
        if self.poisoned {
            return Err(LocalStreamError::IncompleteFrame);
        }
        let length = u64::try_from(body.len())
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .checked_add(page::HEADER_BYTES as u64)
            .ok_or(LocalStreamError::InvalidLimits)?;
        if length > self.remaining() {
            return Err(LocalStreamError::InvalidLimits);
        }
        let prepared = page::prepare(self.number, self.end, kind, body, work)?;
        let result = prepared.write(&self.file, &mut self.end, work);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Data pages become durable before a recovery slot can name them. The
    /// second flush makes that slot durable before acknowledgement. A single
    /// flush after both writes would allow a crash to persist the slot alone.
    /// The caller authenticates the root; failed publication fences its owner.
    pub(super) fn publish(
        &mut self,
        slot: Slot,
        durability: super::super::LocalDurability,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<(), LocalStreamError> {
        if self.poisoned {
            return Err(LocalStreamError::IncompleteFrame);
        }
        if slot.root.segment != self.number
            || slot.root.offset < HEADER_BYTES as u64
            || slot.root.offset.checked_add(u64::from(slot.root.bytes)) != Some(self.end)
            || self
                .published
                .is_some_and(|old| old.revision.checked_add(1) != Some(slot.revision))
        {
            return Err(LocalStreamError::Corrupt);
        }
        let ordinal = u8::from(slot.revision & 1 != 0);
        page::codec(work, SLOT_BYTES as u64, Slot::hash_bytes(), true)?;
        let bytes = slot.encoded(self.number, ordinal);
        page::codec(work, SLOT_BYTES as u64, Slot::hash_bytes(), false)?;
        let offset = (PREFIX_BYTES + usize::from(ordinal) * SLOT_BYTES) as u64;
        let durable = physical::sync(&self.file, durability, work);
        let written = durable.and_then(|()| {
            page::observe(
                work,
                acyclic_native_runtime::write_all_at(
                    &self.file,
                    offset,
                    &bytes,
                    work.map(|work| (work.meter.as_ref(), false)),
                ),
            )
        });
        let result = written.and_then(|()| physical::sync(&self.file, durability, work));
        if result.is_err() {
            self.poisoned = true;
        }
        result?;
        self.published = Some(slot);
        Ok(())
    }
}
