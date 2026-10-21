//! Exclusive store ownership without a descriptor per store.
//!
//! A store names one claim file. Claiming it records this process as the
//! store's owner inside that file, under an OS lock held only for the few
//! syscalls that read and write the record. The record names this process's
//! one process lock, which this process holds for its whole life. Another
//! process admits itself only on positive proof that the recorded owner died:
//! it took that process lock, or the process is provably gone. Within this
//! process a sharded table of claimed files excludes a second claim. An idle
//! store therefore holds no descriptor, and the whole process holds one.

use std::fs::{File, OpenOptions};
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(all(test, acyclic_native_runtime_loom))]
mod model;
mod process;
mod protocol;
mod record;

use process::ProcessOwner;
use protocol::{Host, Liveness, Table};

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
type ClaimKey = crate::FileIdentity;
#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
type ClaimKey = std::path::PathBuf;

/// The claim protocol on this machine's files and processes.
#[derive(Clone, Copy, Debug)]
struct Native;

impl Host for Native {
    type File = File;
    type Key = ClaimKey;

    fn open(&self, path: &Path, create: bool) -> io::Result<File> {
        OpenOptions::new()
            .create(create)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    fn identity(&self, file: &File, _: &Path) -> io::Result<ClaimKey> {
        crate::file_identity(file)
    }

    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    fn identity(&self, _: &File, path: &Path) -> io::Result<ClaimKey> {
        std::fs::canonicalize(path)
    }

    fn try_lock(&self, file: &File) -> io::Result<bool> {
        match file.try_lock() {
            Ok(()) => Ok(true),
            Err(std::fs::TryLockError::WouldBlock) => Ok(false),
            Err(std::fs::TryLockError::Error(error)) => Err(error),
        }
    }

    fn lock(&self, file: &File) -> io::Result<()> {
        file.lock()
    }

    fn unlock(&self, file: &File) -> io::Result<()> {
        file.unlock()
    }

    fn read(&self, file: &mut File) -> io::Result<Vec<u8>> {
        file.seek(SeekFrom::Start(0))?;
        // One byte past the limit, so an oversized file never decodes whole,
        // in the one allocation the check's bound counts.
        let mut bytes = Vec::with_capacity(record::RECORD_LIMIT + 1);
        (&*file)
            .take(record::RECORD_LIMIT as u64 + 1)
            .read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn write(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.set_len(0)?;
        if !bytes.is_empty() {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(bytes)?;
        }
        Ok(())
    }

    fn process(&self) -> Option<&record::Process> {
        ProcessOwner::current().map(|owner| &owner.process)
    }

    fn established(&self) -> Option<&record::Process> {
        ProcessOwner::existing().map(|owner| &owner.process)
    }

    fn liveness(&self, process: &record::Process, probes: &mut u64) -> Liveness {
        process::liveness(process, probes)
    }

    fn token(&self) -> u64 {
        static TOKENS: AtomicU64 = AtomicU64::new(0);
        TOKENS.fetch_add(1, Ordering::Relaxed)
    }

    fn table(&self) -> &Table<ClaimKey> {
        static TABLE: OnceLock<Table<ClaimKey>> = OnceLock::new();
        TABLE.get_or_init(Table::new)
    }
}

#[cfg(target_os = "linux")]
const CHECK_ALLOCATIONS: u64 = 8;
/// Measured on Linux only; elsewhere path conversions may allocate more.
#[cfg(not(target_os = "linux"))]
const CHECK_ALLOCATIONS: u64 = 16;
/// Every buffer the check allocates is bounded by the record, which is at most
/// [`record::RECORD_LIMIT`] bytes.
#[cfg(target_os = "linux")]
const CHECK_ALLOCATION_BYTES: u64 = 5 * (record::RECORD_LIMIT as u64 + 1);
#[cfg(not(target_os = "linux"))]
const CHECK_ALLOCATION_BYTES: u64 = 8 * (record::RECORD_LIMIT as u64 + 1);

/// Exclusive ownership of one store, held by this process for as long as the
/// value lives.
///
/// Contention, from this or another live process, or from a recorded owner
/// whose death cannot be proven, fails with an error that
/// [`crate::is_exclusive_lock_contention`] recognizes.
#[derive(Debug)]
pub struct StoreClaim {
    _claim: protocol::Claim<Native>,
}

impl StoreClaim {
    /// The most backend reads one [`StoreClaim::check`] makes: the record,
    /// then the probes of its owner.
    pub const CHECK_READ_OPERATIONS: u64 = 1 + process::PROBES;

    /// The most allocations one [`StoreClaim::check`] makes: the record's
    /// buffer and its three fields, then its owner's process-lock path, that
    /// path's terminated copy for the open, and the lock's and the process's
    /// identities that its probes read.
    pub const CHECK_ALLOCATIONS: u64 = CHECK_ALLOCATIONS;

    /// The most bytes one [`StoreClaim::check`] allocates in all; its peak
    /// is no larger.
    pub const CHECK_ALLOCATION_BYTES: u64 = CHECK_ALLOCATION_BYTES;

    /// Claims the store whose claim file is `path`, creating the file if needed.
    pub fn acquire(path: &Path) -> io::Result<Self> {
        // Release reopens the file by this path, which must still name it
        // after this process changes its working directory.
        let path = std::path::absolute(path)?;
        protocol::acquire(Native, &path).map(|claim| Self { _claim: claim })
    }

    /// Whether the claim file `file` names an owner that excludes others: a
    /// live process, one whose death cannot be proven, or this one. The
    /// caller holds `file`'s lock, shared or exclusive, so no claim can begin
    /// or end while it reads; holding that lock through a short operation
    /// excludes owners for its duration.
    pub fn is_claimed(file: &mut File) -> io::Result<bool> {
        Self::check(file).map(|check| check.claimed)
    }

    /// The path this claim releases by.
    #[cfg(all(test, unix))]
    pub(crate) fn path(&self) -> &Path {
        self._claim.path()
    }

    /// [`StoreClaim::is_claimed`], with the reads it made, for callers that
    /// account their I/O. At most [`StoreClaim::CHECK_READ_OPERATIONS`].
    pub fn check(file: &mut File) -> io::Result<ClaimCheck> {
        let (claimed, read_operations) = protocol::is_claimed(&Native, file)?;
        Ok(ClaimCheck {
            claimed,
            read_operations,
        })
    }
}

/// What [`StoreClaim::check`] found, and the reads it made to find it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimCheck {
    /// Whether the claim file names an owner that excludes others.
    pub claimed: bool,
    /// Backend reads made: the record, then each probe of its owner.
    pub read_operations: u64,
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) use record::RECORD_LIMIT;

/// The process lock that a claim file's record names, if it holds one.
#[cfg(test)]
pub(crate) fn recorded_process_lock(claim: &[u8]) -> Option<std::path::PathBuf> {
    match record::Contents::decode(claim) {
        record::Contents::Record(record) => process::bytes_path(&record.process.lock),
        record::Contents::Empty | record::Contents::Torn | record::Contents::Oversized => None,
    }
}

/// A record naming process `pid`, started as `start`, by the process lock
/// `lock`, which has no file identity.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn foreign_record(pid: u32, start: &[u8], lock: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;
    record::Record {
        process: record::Process {
            pid,
            start: start.to_vec(),
            lock: lock.as_os_str().as_bytes().to_vec(),
            lock_identity: Vec::new(),
        },
        token: 0,
    }
    .encode()
}
