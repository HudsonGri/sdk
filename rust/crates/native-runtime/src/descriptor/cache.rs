//! The descriptor cache, written once over [`Files`] so that native runs and
//! the model run the same code.

use crate::sync::Mutex;
use crate::sync::atomic::{AtomicBool, Ordering};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, PoisonError, Weak};

/// The operations the cache needs from its platform.
pub(super) trait Files: Send + Sync + Sized + 'static {
    type File: Send + Sync;
    type Identity: Copy + Eq + std::fmt::Debug + Send + Sync;

    /// Opens the existing file at `path`, never creating or truncating it.
    fn reopen(&self, path: &Path, write: bool) -> io::Result<Self::File>;
    fn identity(&self, file: &Self::File) -> io::Result<Self::Identity>;
    /// Makes every write through `file` durable before it closes.
    fn flush(&self, file: &Self::File) -> io::Result<()>;
}

/// Open descriptors of one process, at most `capacity` of them idle.
pub(super) struct Cache<F: Files> {
    files: F,
    capacity: AtomicUsize,
    ring: Mutex<Ring<F>>,
}

/// Files holding a cached descriptor, swept by a clock hand.
struct Ring<F: Files> {
    entries: Vec<Weak<Entry<F>>>,
    hand: usize,
}

impl<F: Files> Cache<F> {
    pub(super) fn new(files: F, capacity: usize) -> Self {
        Self {
            files,
            capacity: AtomicUsize::new(capacity.max(1)),
            ring: Mutex::new(Ring {
                entries: Vec::new(),
                hand: 0,
            }),
        }
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bounds the idle descriptors; the bound applies from the next reopen.
    pub(super) fn set_capacity(&self, capacity: usize) {
        self.capacity
            .store(capacity.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    /// Files whose descriptor the cache holds.
    pub(super) fn cached(&self) -> usize {
        self.ring
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .iter()
            .filter(|entry| entry.strong_count() != 0)
            .count()
    }

    /// Records that `entry`, whose state lock the caller holds, now has a
    /// descriptor, and takes the descriptors of files that went unused
    /// longest until the cache is within its capacity. The caller drops the
    /// taken descriptors after releasing its locks, since closing one may
    /// flush it.
    fn admit(&self, entry: &Arc<Entry<F>>, evicted: &mut Vec<crate::sync::Arc<Handle<F>>>) {
        let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        ring.entries.push(Arc::downgrade(entry));
        let capacity = self.capacity();
        // A file's reference bit clears on the hand's first pass, so the
        // second finds every idle victim; a sweep that finds none leaves the
        // ring over capacity only while every other file is in use.
        let mut budget = ring.entries.len().saturating_mul(3);
        while ring.entries.len() > capacity && budget > 0 {
            budget -= 1;
            if ring.hand >= ring.entries.len() {
                ring.hand = 0;
            }
            let hand = ring.hand;
            let Some(candidate) = ring.entries.get(hand).and_then(Weak::upgrade) else {
                // A file that was dropped closed its own descriptor.
                ring.entries.swap_remove(hand);
                continue;
            };
            if Arc::ptr_eq(&candidate, entry) {
                ring.hand += 1;
                continue;
            }
            // Never waits while holding the ring: a file whose lock is held
            // is in use, which is a reference.
            let Ok(mut state) = candidate.state.try_lock() else {
                ring.hand += 1;
                continue;
            };
            if std::mem::take(&mut state.referenced) {
                ring.hand += 1;
                continue;
            }
            if let Some(handle) = state.open.take() {
                evicted.push(handle);
            }
            state.cached = false;
            drop(state);
            ring.entries.swap_remove(hand);
        }
    }
}

/// One cached file.
pub(super) struct Entry<F: Files> {
    cache: Arc<Cache<F>>,
    path: PathBuf,
    write: bool,
    /// Whether closing a written descriptor flushes it.
    durable: bool,
    state: Mutex<State<F>>,
    /// A write that no flush could make durable before its descriptor
    /// closed. Kept apart from `state`, since a descriptor may close while
    /// `state` is held.
    lost: Arc<std::sync::Mutex<Option<io::ErrorKind>>>,
}

struct State<F: Files> {
    identity: F::Identity,
    open: Option<crate::sync::Arc<Handle<F>>>,
    /// Used since the clock hand last passed.
    referenced: bool,
    /// Listed in the cache's ring.
    cached: bool,
}

/// One open descriptor. It flushes the writes made through it before it
/// closes, whoever closes it last.
pub(super) struct Handle<F: Files> {
    file: F::File,
    cache: Arc<Cache<F>>,
    /// Written since the last flush.
    unsynced: AtomicBool,
    /// Whether closing must flush.
    durable: bool,
    lost: Arc<std::sync::Mutex<Option<io::ErrorKind>>>,
}

impl<F: Files> Drop for Handle<F> {
    fn drop(&mut self) {
        if !self.durable || !self.unsynced.load(Ordering::Acquire) {
            return;
        }
        // On Linux a descriptor opened later never sees a writeback error
        // of this one, so a failure here is kept for the file's next use.
        if let Err(error) = self.cache.files.flush(&self.file) {
            self.lost
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert(error.kind());
        }
    }
}

impl<F: Files> Handle<F> {
    pub(super) fn file(&self) -> &F::File {
        &self.file
    }

    /// Marks the writes `write` makes, before it makes them.
    pub(super) fn write<R>(&self, write: impl FnOnce(&F::File) -> io::Result<R>) -> io::Result<R> {
        self.unsynced.store(true, Ordering::Release);
        write(&self.file)
    }

    /// Runs `flush`, which makes every earlier write durable.
    pub(super) fn flush<R>(&self, flush: impl FnOnce(&F::File) -> io::Result<R>) -> io::Result<R> {
        // Cleared first, so a write racing the flush stays marked.
        let unsynced = self.unsynced.swap(false, Ordering::AcqRel);
        flush(&self.file).inspect_err(|_| {
            if unsynced {
                self.unsynced.store(true, Ordering::Release);
            }
        })
    }
}

impl<F: Files> Entry<F> {
    /// Caches `file`, opened at `path`. `durable` files flush writes before
    /// their descriptor closes.
    pub(super) fn new(
        cache: &Arc<Cache<F>>,
        file: F::File,
        path: &Path,
        write: bool,
        durable: bool,
    ) -> io::Result<Arc<Self>> {
        let identity = cache.files.identity(&file)?;
        let lost = Arc::default();
        let entry = Arc::new(Self {
            cache: Arc::clone(cache),
            path: std::path::absolute(path)?,
            write,
            durable,
            state: Mutex::new(State {
                identity,
                open: None,
                referenced: true,
                cached: false,
            }),
            lost,
        });
        entry.install(file);
        Ok(entry)
    }

    fn handle(&self, file: F::File) -> crate::sync::Arc<Handle<F>> {
        crate::sync::Arc::new(Handle {
            file,
            cache: Arc::clone(&self.cache),
            unsynced: AtomicBool::new(false),
            durable: self.durable,
            lost: Arc::clone(&self.lost),
        })
    }

    fn check_lost(&self) -> io::Result<()> {
        match *self.lost.lock().unwrap_or_else(PoisonError::into_inner) {
            Some(kind) => Err(io::Error::new(
                kind,
                "a write was lost when its file's descriptor closed",
            )),
            None => Ok(()),
        }
    }

    /// The file's descriptor, reopened by path when the cache closed it,
    /// after `admit` admits the reopen. A reopened file must be the very file
    /// cached, never another that took its name.
    pub(super) fn lease(
        self: &Arc<Self>,
        admit: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<crate::sync::Arc<Handle<F>>> {
        self.check_lost()?;
        let mut evicted = Vec::new();
        let leased = (|| {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.referenced = true;
            if let Some(open) = &state.open {
                return Ok(crate::sync::Arc::clone(open));
            }
            admit()?;
            let file = self.cache.files.reopen(&self.path, self.write)?;
            if self.cache.files.identity(&file)? != state.identity {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a cached file was replaced while its descriptor was closed",
                ));
            }
            let handle = self.handle(file);
            state.open = Some(crate::sync::Arc::clone(&handle));
            if !state.cached {
                state.cached = true;
                self.cache.admit(self, &mut evicted);
            }
            Ok(handle)
        })();
        drop(evicted);
        leased
    }

    fn install(self: &Arc<Self>, file: F::File) {
        let mut evicted = Vec::new();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.open = Some(self.handle(file));
        state.referenced = true;
        if !state.cached {
            state.cached = true;
            self.cache.admit(self, &mut evicted);
        }
        drop(state);
        drop(evicted);
    }
}
