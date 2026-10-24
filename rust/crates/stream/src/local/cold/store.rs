//! File-backed roots and immutable pages. A fixed locator selects the active
//! segment, so opening never scans a directory or replays earlier publications.
use super::directory::Directory;
use super::*;
use root::{Root, Slot};
use segment::Segment;
use std::fs::OpenOptions;
use std::path::Path;

mod captured;
mod cut;
mod locator;
mod spans;
mod views;
pub(in crate::local::cold) use cut::Cut;
pub(in crate::local::cold) use views::Observer;
#[cfg(test)]
mod tests;

const HEAD: &str = "cold.head";
const HEAD_TEMPORARY: &str = ".cold-head.tmp";

#[derive(Clone, Copy, Eq, PartialEq)]
enum Access {
    Read,
    Active,
    Replace,
}

pub(super) struct Store {
    active: Segment,
    head: File,
    state: Root,
    durable: Root,
    pending: bool,
    maximum: u64,
    durability: super::super::LocalDurability,
    failed: bool,
    views: views::Views,
    // Every file closes before the last directory/root gate is released.
    root: Arc<Directory>,
}

fn filename(number: u64) -> Result<[u8; 16], LocalStreamError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut bytes = [0; 16];
    for (number, [high, low]) in number
        .to_be_bytes()
        .into_iter()
        .zip(bytes.as_chunks_mut::<2>().0)
    {
        *high = *HEX
            .get(usize::from(number >> 4))
            .ok_or(LocalStreamError::Corrupt)?;
        *low = *HEX
            .get(usize::from(number & 15))
            .ok_or(LocalStreamError::Corrupt)?;
    }
    Ok(bytes)
}

fn segment_path(
    root: &Path,
    number: u64,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<physical::LocalPath, LocalStreamError> {
    page::codec(work, 16, 0, true)?;
    let bytes = filename(number)?;
    page::codec(work, 16, 0, false)?;
    let suffix = std::str::from_utf8(&bytes).map_err(|_| LocalStreamError::Corrupt)?;
    physical::path(root, suffix, work)
}

fn open_segment(
    root: &Path,
    number: u64,
    access: Access,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<File, LocalStreamError> {
    let path = segment_path(root, number, work)?;
    let create = access == Access::Replace;
    physical::operation(work, create, || {
        OpenOptions::new()
            .read(true)
            .write(access != Access::Read)
            .create(create)
            .truncate(create)
            .open(&path.value)
    })
}

/// Used only while initializing an unpublished root. Its complete state has
/// no references to another segment, so it cannot depend on recovery state.
struct Initial<'a>(&'a mut Segment);
impl PageRead for Initial<'_> {
    fn work(&self) -> Option<&Arc<crate::memory::Work>> {
        None
    }
    fn read(&mut self, reference: PageRef) -> Result<Page, LocalStreamError> {
        if reference.segment != self.0.number {
            return Err(LocalStreamError::Corrupt);
        }
        page::read(&self.0.file, reference, None)
    }
}
impl PageIo for Initial<'_> {
    fn append(&mut self, kind: PageKind, body: &[u8]) -> Result<PageRef, LocalStreamError> {
        self.0.append(kind, body, None)
    }
}

impl Store {
    pub(super) fn open(
        root: &Path,
        maximum: u64,
        durability: super::super::LocalDurability,
    ) -> Result<Self, LocalStreamError> {
        Self::open_with_anchor(root, maximum, durability, None)
    }

    pub(super) fn open_with_anchor(
        root: &Path,
        maximum: u64,
        durability: super::super::LocalDurability,
        anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
    ) -> Result<Self, LocalStreamError> {
        if maximum < segment::DATA_START + root::ROOT_BYTES {
            return Err(LocalStreamError::InvalidLimits);
        }
        let ownership = super::super::lock_root(
            root,
            super::super::LocalStreamLimits {
                durability,
                ..Default::default()
            },
        )?;
        let root = Directory::new(root.to_path_buf(), ownership, anchor);
        let head_path = physical::path(&root, HEAD, None)?;
        let head = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(&head_path.value)
        {
            Ok(head) => head,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Self::initialize(&root, maximum, durability)?;
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&head_path.value)?
            }
            Err(error) => return Err(error.into()),
        };
        let number = locator::load(&head, maximum, None)?;
        let file = open_segment(&root, number, Access::Active, None)?;
        let active = Segment::open(file, number, maximum, None)?;
        let selected = active.published()?;
        let mut store = Self {
            root,
            active,
            head,
            state: Root::default(),
            durable: Root::default(),
            pending: false,
            maximum,
            durability,
            failed: false,
            views: views::Views::new(),
        };
        let state = Root::load(
            &mut Reader {
                store: &mut store,
                work: None,
            },
            selected.root,
        )?;
        store.active.discard_unpublished(None)?;
        store.state = state;
        store.durable = state;
        store.publish_view();
        Ok(store)
    }

    fn initialize(
        root: &Path,
        maximum: u64,
        durability: super::super::LocalDurability,
    ) -> Result<(), LocalStreamError> {
        // Explicitly reject a legacy provider root. There is no format shim.
        for name in [super::super::JOURNAL_FILE, super::super::SNAPSHOT_FILE] {
            if physical::path(root, name, None)?.value.try_exists()? {
                return Err(LocalStreamError::Corrupt);
            }
        }
        // With no published locator, an interrupted segment-zero initialization
        // has no acknowledged references and may be replaced under this lock.
        let file = open_segment(root, 0, Access::Replace, None)?;
        let mut segment = Segment::create(file, 0, None, maximum, None)?;
        let reference = Root::default().append(&mut Initial(&mut segment))?;
        segment.publish(
            Slot {
                revision: 1,
                root: reference,
            },
            durability,
            None,
        )?;
        let temporary = physical::path(root, HEAD_TEMPORARY, None)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary.value)?;
        locator::create(&file, maximum)?;
        physical::sync(&file, durability, None)?;
        drop(file);
        // The segment's name is durable before the locator can name it.
        super::super::sync_directory(root, durability)?;
        let head = physical::path(root, HEAD, None)?;
        acyclic_native_runtime::rename_file(&temporary.value, &head.value)?;
        super::super::sync_directory(root, durability)?;
        Ok(())
    }

    pub(super) fn state(&self) -> Result<Root, LocalStreamError> {
        if self.failed {
            self.views.poison();
            return Err(LocalStreamError::IncompleteFrame);
        }
        if let Err(error) = self.active.published() {
            self.views.poison();
            return Err(error);
        }
        Ok(self.state)
    }

    fn check_publication<T>(
        &self,
        result: Result<T, LocalStreamError>,
    ) -> Result<T, LocalStreamError> {
        if result.is_err() {
            // A preflight denial leaves the old publication valid. Only a
            // poisoned segment or ambiguous locator invalidates readers.
            let _ = self.state();
        }
        result
    }

    fn publish_view(&self) {
        self.views.publish(self.cut(self.state), self.durable);
    }

    pub(in crate::local::cold) fn observe(&self) -> Observer {
        self.views.observe()
    }

    pub(super) fn durable_state(&self) -> Result<Root, LocalStreamError> {
        self.state()?;
        Ok(self.durable)
    }

    /// Accepted publications append immutable pages only. Exactly one root
    /// slot becomes eligible for recovery at the phase barrier, preserving
    /// both durable fallback slots throughout any number of accepted writes.
    pub(super) fn seal(
        &mut self,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<(), LocalStreamError> {
        let state = self.state()?;
        if !self.pending {
            return Ok(());
        }
        Transaction {
            store: self,
            state,
            remaining: root::ROOT_BYTES,
            work,
        }
        .publish()?;
        Ok(())
    }

    pub(super) fn begin(
        &mut self,
        maximum_bytes: u64,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<Transaction<'_>, LocalStreamError> {
        let state = self.state()?;
        if maximum_bytes < root::ROOT_BYTES {
            return Err(LocalStreamError::InvalidLimits);
        }
        if self.active.remaining() < root::ROOT_BYTES {
            self.rollover(work.as_ref())?;
        }
        Ok(Transaction {
            store: self,
            state,
            remaining: maximum_bytes,
            work,
        })
    }

    fn rollover(
        &mut self,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<(), LocalStreamError> {
        // A new locator must inherit the entire accepted prefix. It cannot
        // abandon pending roots in the predecessor segment.
        self.flush_predecessor(work)?;
        let current = self.active.number;
        let next = current
            .checked_add(1)
            .ok_or(LocalStreamError::InvalidLimits)?;
        let inherited = self.active.published()?;
        // Any segment after the selected locator is unreachable. Replacing
        // this one known abandoned name needs no directory traversal.
        let file = open_segment(&self.root, next, Access::Replace, work)?;
        let segment = Segment::create(file, next, Some(inherited), self.maximum, work)?;
        physical::sync(&segment.file, self.durability, work)?;
        physical::sync_directory(&self.root, self.durability, work)?;
        // Once the locator write starts its result can be ambiguous. Fence
        // further use until a fresh open authenticates the selected root.
        self.failed = true;
        let advanced = locator::advance(&self.head, current, next, work)
            .and_then(|()| physical::sync(&self.head, self.durability, work));
        self.check_publication(advanced)?;
        self.active = segment;
        self.failed = false;
        Ok(())
    }

    fn read(
        &mut self,
        reference: PageRef,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<Page, LocalStreamError> {
        self.state()?;
        if self.failed
            || reference.segment > self.active.number
            || reference.offset < segment::DATA_START
        {
            return Err(LocalStreamError::Corrupt);
        }
        if reference.segment == self.active.number {
            if reference
                .offset
                .checked_add(u64::from(reference.bytes))
                .is_none_or(|end| end > self.active.end)
            {
                return Err(LocalStreamError::Corrupt);
            }
            return page::read(&self.active.file, reference, work);
        }
        let file = open_segment(&self.root, reference.segment, Access::Read, work)?;
        page::read(&file, reference, work)
    }

    pub(super) fn reader(&mut self, work: Option<Arc<crate::memory::Work>>) -> Reader<'_> {
        Reader { store: self, work }
    }
}

pub(super) struct Reader<'a> {
    store: &'a mut Store,
    work: Option<Arc<crate::memory::Work>>,
}
impl PageRead for Reader<'_> {
    fn work(&self) -> Option<&Arc<crate::memory::Work>> {
        self.work.as_ref()
    }
    fn read(&mut self, reference: PageRef) -> Result<Page, LocalStreamError> {
        self.store.read(reference, self.work.as_ref())
    }
}

pub(super) struct Transaction<'a> {
    store: &'a mut Store,
    pub(super) state: Root,
    remaining: u64,
    work: Option<Arc<crate::memory::Work>>,
}
impl PageRead for Transaction<'_> {
    fn work(&self) -> Option<&Arc<crate::memory::Work>> {
        self.work.as_ref()
    }
    fn read(&mut self, reference: PageRef) -> Result<Page, LocalStreamError> {
        self.store.read(reference, self.work.as_ref())
    }
}
impl PageIo for Transaction<'_> {
    fn append(&mut self, kind: PageKind, body: &[u8]) -> Result<PageRef, LocalStreamError> {
        let length = (body.len() as u64)
            .checked_add(page::HEADER_BYTES as u64)
            .ok_or(LocalStreamError::InvalidLimits)?;
        let remaining = self
            .remaining
            .checked_sub(length)
            .ok_or(LocalStreamError::InvalidLimits)?;
        if kind != PageKind::Root && remaining < root::ROOT_BYTES {
            return Err(LocalStreamError::InvalidLimits);
        }
        self.make_room(kind, length)?;
        let appended = self.store.active.append(kind, body, self.work.as_ref());
        let reference = self.store.check_publication(appended)?;
        self.remaining = remaining;
        Ok(reference)
    }
}
impl Transaction<'_> {
    pub(super) fn accept(self) -> Result<(), LocalStreamError> {
        self.store.state()?;
        if self.remaining < root::ROOT_BYTES {
            return Err(LocalStreamError::InvalidLimits);
        }
        self.store.state = self.state;
        self.store.pending = true;
        self.store.publish_view();
        Ok(())
    }

    pub(super) fn publish(mut self) -> Result<Slot, LocalStreamError> {
        let state = self.state;
        let revision = self
            .store
            .active
            .published()?
            .revision
            .checked_add(1)
            .ok_or(LocalStreamError::InvalidLimits)?;
        let reference = state.append(&mut self)?;
        let slot = Slot {
            revision,
            root: reference,
        };
        let published = self
            .store
            .active
            .publish(slot, self.store.durability, self.work.as_ref());
        self.store.check_publication(published)?;
        self.store.state = state;
        self.store.durable = state;
        self.store.pending = false;
        self.store.publish_view();
        Ok(slot)
    }
}
