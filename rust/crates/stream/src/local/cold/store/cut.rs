//! A publication cut contains no writer work context and takes no mutation
//! lock. Its shared immutable resources are admitted before acceptance.
use super::*;
use crate::local::cold::{directory::Directory, handle::SharedFile};

#[derive(Clone)]
pub(in crate::local::cold) struct Cut {
    pub(in crate::local::cold) state: Root,
    pub(super) file: SharedFile,
    pub(super) number: u64,
    pub(super) end: u64,
    // Closing the final file precedes releasing either root ownership gate.
    pub(super) root: Arc<Directory>,
}

impl Cut {
    pub(super) fn read(
        &self,
        reference: PageRef,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<Page, LocalStreamError> {
        if reference.segment > self.number || reference.offset < segment::DATA_START {
            return Err(LocalStreamError::Corrupt);
        }
        if reference.segment == self.number {
            if reference
                .offset
                .checked_add(u64::from(reference.bytes))
                .is_none_or(|end| end > self.end)
            {
                return Err(LocalStreamError::Corrupt);
            }
            return page::read(&self.file, reference, work);
        }
        let file = open_segment(&self.root, reference.segment, Access::Read, work)?;
        page::read(&file, reference, work)
    }
}
