//! The claim protocol, written once over [`Host`] so that native runs and the
//! model run the same code.

use super::record::{Contents, Process, Record};
use std::collections::HashSet;
use std::hash::Hash;
use std::io;
use std::path::{Path, PathBuf};

/// What a probe proved about a recorded owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Liveness {
    Alive,
    /// Positive proof that the owner died.
    Dead,
    /// Nothing proven either way. Refuses, as `Alive` does: a false refusal
    /// is an error the caller sees, a false admission is two owners.
    Unknown,
}

/// The operations the protocol needs from its platform.
pub(super) trait Host {
    type File;
    type Key: Clone + Eq + Hash + std::fmt::Debug;

    fn open(&self, path: &Path, create: bool) -> io::Result<Self::File>;
    /// The file's identity, the same for every name of it; `path` is the
    /// name `file` was opened by.
    fn identity(&self, file: &Self::File, path: &Path) -> io::Result<Self::Key>;
    /// Takes the file's exclusive lock without waiting: `Ok(false)` when
    /// another holds it.
    fn try_lock(&self, file: &Self::File) -> io::Result<bool>;
    /// Takes the file's exclusive lock, waiting for its holder.
    fn lock(&self, file: &Self::File) -> io::Result<()>;
    fn unlock(&self, file: &Self::File) -> io::Result<()>;
    /// The whole file, up to [`super::record::RECORD_LIMIT`] bytes.
    fn read(&self, file: &mut Self::File) -> io::Result<Vec<u8>>;
    /// Replaces the whole file with `bytes`.
    fn write(&self, file: &mut Self::File, bytes: &[u8]) -> io::Result<()>;
    /// This process, when it holds a process lock that claims can name.
    fn process(&self) -> Option<&Process>;
    fn liveness(&self, process: &Process) -> Liveness;
    /// A token no other claim of this process has used.
    fn token(&self) -> u64;
    fn table(&self) -> &Table<Self::Key>;
}

pub(super) fn contention(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, reason)
}

#[cfg(not(acyclic_native_runtime_loom))]
const SHARDS: usize = 64;
/// Models keep the table small; sharding is not what they check.
#[cfg(acyclic_native_runtime_loom)]
const SHARDS: usize = 2;

/// Claimed files of one process, sharded so unrelated claims never contend.
pub(super) struct Table<Key> {
    shards: [crate::sync::Mutex<HashSet<Key>>; SHARDS],
}

impl<Key: Clone + Eq + Hash> Table<Key> {
    pub(super) fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| crate::sync::Mutex::new(HashSet::new())),
        }
    }

    fn shard(&self, key: &Key) -> io::Result<&crate::sync::Mutex<HashSet<Key>>> {
        use std::hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher};
        let index = BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % SHARDS as u64;
        usize::try_from(index)
            .ok()
            .and_then(|index| self.shards.get(index))
            .ok_or_else(|| io::Error::other("claim shard out of range"))
    }

    fn insert(&self, key: &Key) -> io::Result<()> {
        let mut claimed = self
            .shard(key)?
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if claimed.contains(key) {
            return Err(contention("store already claimed in this process"));
        }
        claimed.insert(key.to_owned());
        Ok(())
    }

    fn remove(&self, key: &Key) {
        if let Ok(shard) = self.shard(key) {
            shard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(key);
        }
    }
}

/// Whether the recorded owner of a claim file excludes a new one.
fn excludes<H: Host>(host: &H, contents: &Contents) -> Option<io::Error> {
    let Contents::Record(record) = contents else {
        return None;
    };
    // A record of this process that its table does not hold is a release
    // that could not clear it: no owner.
    if host.process() == Some(&record.process) {
        return None;
    }
    match host.liveness(&record.process) {
        Liveness::Dead => None,
        Liveness::Alive => Some(contention("store claimed by a live process")),
        Liveness::Unknown => Some(contention("store claimed by a process of unknown liveness")),
    }
}

/// One held claim.
pub(super) struct Claim<H: Host> {
    host: H,
    path: PathBuf,
    key: H::Key,
    /// The record this claim wrote, which release alone may clear.
    token: Option<u64>,
    /// The claim file, kept locked as the claim itself when this process has
    /// no process lock to name.
    held: Option<H::File>,
}

impl<H: Host> std::fmt::Debug for Claim<H> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreClaim")
            .field("path", &self.path)
            .field("key", &self.key)
            .field("token", &self.token)
            .field("held", &self.held.is_some())
            .finish()
    }
}

/// Removes a table entry unless the claim that owns it is completed.
struct Pending<'a, H: Host> {
    host: &'a H,
    key: Option<H::Key>,
}

impl<H: Host> Drop for Pending<'_, H> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.host.table().remove(&key);
        }
    }
}

pub(super) fn acquire<H: Host>(host: H, path: &Path) -> io::Result<Claim<H>> {
    let mut file = host.open(path, true)?;
    let key = host.identity(&file, path)?;
    host.table().insert(&key)?;
    let mut pending = Pending {
        host: &host,
        key: Some(key),
    };
    if !host.try_lock(&file)? {
        return Err(contention(
            "store claim is being exchanged by another owner",
        ));
    }
    let exchanged = (|| {
        let contents = Contents::decode(&host.read(&mut file)?);
        if let Some(refusal) = excludes(&host, &contents) {
            return Err(refusal);
        }
        let Some(process) = host.process() else {
            // Without a process lock, the claim file's own lock is the claim:
            // the OS releases it when this process dies.
            if contents != Contents::Empty {
                host.write(&mut file, &[])?;
            }
            return Ok(None);
        };
        let token = host.token();
        let record = Record {
            process: process.clone(),
            token,
        };
        host.write(&mut file, &record.encode())?;
        Ok(Some(token))
    })();
    let token = match exchanged {
        Ok(token) => token,
        Err(error) => {
            let _ = host.unlock(&file);
            return Err(error);
        }
    };
    let held = match token {
        Some(_) => {
            // Closing the file releases its lock even if unlocking fails.
            let _ = host.unlock(&file);
            None
        }
        None => Some(file),
    };
    let key = pending
        .key
        .take()
        .ok_or_else(|| io::Error::other("claim key taken"))?;
    drop(pending);
    Ok(Claim {
        host,
        path: path.to_path_buf(),
        key,
        token,
        held,
    })
}

/// Whether the claim file `file`, whose lock the caller holds, names an
/// owner that excludes others: a live process, one of unknown liveness, or
/// this one.
pub(super) fn is_claimed<H: Host>(host: &H, file: &mut H::File) -> io::Result<bool> {
    let contents = Contents::decode(&host.read(file)?);
    Ok(match &contents {
        Contents::Record(record) => {
            host.process() == Some(&record.process) || excludes(host, &contents).is_some()
        }
        Contents::Empty | Contents::Torn => false,
    })
}

impl<H: Host> Claim<H> {
    /// The claimed file's identity.
    #[cfg(all(test, acyclic_native_runtime_loom))]
    pub(super) fn key(&self) -> &H::Key {
        &self.key
    }

    /// Clears this claim's own record: only in the very file it claimed, and
    /// only while the record still carries this claim's token.
    fn release(&mut self) -> io::Result<()> {
        if let Some(held) = self.held.take() {
            return self.host.unlock(&held);
        }
        let Some(token) = self.token else {
            return Ok(());
        };
        let mut file = match self.host.open(&self.path, false) {
            Ok(file) => file,
            // The file is gone, and the record with it.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        // The path may now name another store's file; this claim's record,
        // if it survives, is in the file it claimed and names this live
        // process, which keeps others out until it exits.
        if self.host.identity(&file, &self.path)? != self.key {
            return Ok(());
        }
        // Waits out an exchange in progress: other holders keep this lock for
        // a few syscalls, and none can own the claim while this record
        // stands. Giving up instead would leave a record naming this live
        // process, which locks every other process out until it exits.
        self.host.lock(&file)?;
        let cleared = (|| match Contents::decode(&self.host.read(&mut file)?) {
            Contents::Record(record)
                if Some(&record.process) == self.host.process() && record.token == token =>
            {
                self.host.write(&mut file, &[])
            }
            _ => Ok(()),
        })();
        let unlocked = self.host.unlock(&file);
        cleared.and(unlocked)
    }
}

impl<H: Host> Drop for Claim<H> {
    fn drop(&mut self) {
        let _ = self.release();
        self.host.table().remove(&self.key);
    }
}
