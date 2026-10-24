//! A transaction's byte allowance is independent of a physical segment. Each
//! page still fits wholly in one segment, and leaves room to seal the accepted
//! prefix before a rollover. The private transaction root is never inherited.
use super::*;

impl Store {
    pub(super) fn flush_predecessor(
        &mut self,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<(), LocalStreamError> {
        if self.pending {
            self.seal(work.cloned())
        } else {
            // A private transaction can already reference these pages while
            // Store.state still describes the last accepted publication.
            physical::sync(&self.active.file, self.durability, work)
        }
    }
}

impl Transaction<'_> {
    pub(super) fn make_room(
        &mut self,
        kind: PageKind,
        length: u64,
    ) -> Result<(), LocalStreamError> {
        let reserve = if kind == PageKind::Root {
            0
        } else {
            root::ROOT_BYTES
        };
        let required = length
            .checked_add(reserve)
            .ok_or(LocalStreamError::InvalidLimits)?;
        if required > self.store.maximum - segment::DATA_START {
            return Err(LocalStreamError::InvalidLimits);
        }
        if self.store.active.remaining() < required {
            // Rollover publishes only Store.state, never this transaction's
            // private state. Earlier transaction pages become durable before
            // a later segment's recovery slot can reference them.
            self.store.rollover(self.work.as_ref())?;
        }
        Ok(())
    }
}
