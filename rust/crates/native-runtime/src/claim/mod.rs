//! Exclusive store ownership without a descriptor per store.
//!
//! A store names one claim file. Claiming it records this process as the
//! store's owner inside that file, under an OS lock held only for the few
//! syscalls that read and write the record. The record names this process's
//! one process lock, which this process holds for its whole life. Another
//! process finds the owner alive while it cannot take that process lock, so a
//! crashed owner's claims lapse with it. Within this process a sharded table
//! of claimed files excludes a second claim. An idle store therefore holds no
//! descriptor, and the whole process holds one.

use crate::*;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::PathBuf;
use std::sync::OnceLock;

mod process;
use process::{ProcessOwner, Record};

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
type ClaimKey = FileIdentity;
#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
type ClaimKey = PathBuf;

const SHARDS: usize = 64;

/// Claimed files of this process, sharded so unrelated claims never contend.
fn claimed() -> &'static [Shard; SHARDS] {
    static CLAIMED: OnceLock<[Shard; SHARDS]> = OnceLock::new();
    CLAIMED.get_or_init(|| std::array::from_fn(|_| Mutex::new(HashSet::new())))
}

type Shard = Mutex<HashSet<ClaimKey>>;

fn shard(key: &ClaimKey) -> io::Result<&'static Shard> {
    use std::hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher};
    let index = BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % SHARDS as u64;
    usize::try_from(index)
        .ok()
        .and_then(|index| claimed().get(index))
        .ok_or_else(|| io::Error::other("claim shard out of range"))
}

fn contention() -> io::Error {
    io::Error::from(io::ErrorKind::WouldBlock)
}

/// Exclusive ownership of one store, held by this process for as long as the
/// value lives.
///
/// Contention, from this or another live process, fails with an error that
/// [`is_exclusive_lock_contention`] recognizes.
#[derive(Debug)]
pub struct StoreClaim {
    path: PathBuf,
    /// The claim file, kept locked only when this process could not establish
    /// its process lock and so cannot leave a record that lapses with it.
    held: Option<File>,
    // Last: the table admits a reclaim only after the record is cleared.
    _entry: Entry,
}

impl StoreClaim {
    /// Claims the store whose claim file is `path`, creating the file if needed.
    pub fn acquire(path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let entry = Entry::insert(claim_key(&file, path)?)?;
        lock_briefly(&file)?;
        let owner = ProcessOwner::current();
        let recorded = Record::read(&mut file)?;
        if let Some(recorded) = &recorded
            && !owner.is_some_and(|owner| owner.wrote(recorded))
            && recorded.is_alive()
        {
            return Err(contention());
        }
        let held = if let Some(owner) = owner {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&owner.record())?;
            file.unlock()?;
            None
        } else {
            // Without a process lock, the claim file's own lock is the claim:
            // the OS releases it when this process dies.
            if recorded.is_some() {
                file.set_len(0)?;
            }
            Some(file)
        };
        Ok(Self {
            path: path.to_path_buf(),
            held,
            _entry: entry,
        })
    }

    /// Whether the claim file `file` names an owner: a live process, or this
    /// one. The caller holds `file`'s lock, shared or exclusive, so no claim
    /// can begin or end while it reads; holding that lock through a short
    /// operation excludes owners for its duration.
    pub fn is_claimed(file: &mut File) -> io::Result<bool> {
        let owner = ProcessOwner::current();
        Ok(Record::read(file)?.is_some_and(|recorded| {
            owner.is_some_and(|owner| owner.wrote(&recorded)) || recorded.is_alive()
        }))
    }

    fn release(&mut self) -> io::Result<()> {
        if let Some(held) = self.held.take() {
            return held.unlock();
        }
        let Some(owner) = ProcessOwner::current() else {
            return Ok(());
        };
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        lock_briefly(&file)?;
        if Record::read(&mut file)?.is_some_and(|recorded| owner.wrote(&recorded)) {
            file.set_len(0)?;
        }
        file.unlock()
    }
}

impl Drop for StoreClaim {
    fn drop(&mut self) {
        // A record left behind names this live process, so other processes
        // stay excluded until it exits, while this process's table admits a
        // reclaim, which overwrites it.
        let _ = self.release();
    }
}

/// One claimed file in this process's table, removed when dropped.
#[derive(Debug)]
struct Entry {
    key: ClaimKey,
    shard: &'static Shard,
}

impl Entry {
    fn insert(key: ClaimKey) -> io::Result<Self> {
        let shard = shard(&key)?;
        let mut claimed = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if claimed.contains(&key) {
            return Err(contention());
        }
        claimed.insert(key.to_owned());
        Ok(Self { key, shard })
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

/// Takes the claim file's lock for one record exchange. Holders keep it for a
/// few syscalls, or for life when they hold it as the claim itself, so
/// contention means another owner.
fn lock_briefly(file: &File) -> io::Result<()> {
    file.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => contention(),
        std::fs::TryLockError::Error(error) => error,
    })
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn claim_key(file: &File, _: &Path) -> io::Result<ClaimKey> {
    file_identity(file)
}

#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
fn claim_key(_: &File, path: &Path) -> io::Result<ClaimKey> {
    std::fs::canonicalize(path)
}
