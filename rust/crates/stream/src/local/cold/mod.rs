//! Immutable, address-bound facts and indexes for the local provider.
use std::fs::File;
use std::sync::Arc;

use sha2::{Digest as _, Sha256};

use super::LocalStreamError;
use crate::MAX_COMMAND_BYTES;

mod index;
mod page;
mod root;
mod slots;

use page::{Buffer, Page, PageKind, PageRef};
use slots::Slots;

/// The transaction owns file admission and publication. Index traversal never
/// reconstructs the stream state, and only follows older, authenticated pages.
trait PageIo {
    fn work(&self) -> Option<&Arc<crate::memory::Work>>;
    fn read(&mut self, reference: PageRef) -> Result<Page, LocalStreamError>;
    fn append(&mut self, kind: PageKind, body: &[u8]) -> Result<PageRef, LocalStreamError>;
}

#[cfg(test)]
mod tests;
