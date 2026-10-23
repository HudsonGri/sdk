//! Whole-file advisory locks that are released before their file closes.
//!
//! An OS file lock (`flock` on Unix, `LockFileEx` on Windows) belongs to the
//! open file description, not to one descriptor. Closing a descriptor releases
//! it only once every copy of that description is closed, and any process
//! spawned concurrently holds copies of all descriptors until it execs, so a
//! lock "released" by close can outlive its owner by an arbitrary delay. An
//! explicit unlock releases the description for every holder at once.
//!
//! [`FileLock`] is the only way the workspace takes such a lock. It unlocks
//! explicitly when released or dropped, so close-to-release cannot be written.

#![expect(
    clippy::disallowed_methods,
    reason = "the one module that calls the raw file-lock primitives"
)]

use std::fs::File;
use std::io;
use std::sync::Mutex;
use std::time::Instant;

mod wait;
pub use wait::FileLockCancellation;

/// Which whole-file lock to take.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileLockMode {
    /// Any number of shared holders exclude every exclusive holder.
    Shared,
    /// One exclusive holder excludes every other holder.
    Exclusive,
}

/// One open file and the advisory lock this handle holds on it, if any.
///
/// Dropping a held lock unlocks it before the file closes. Contention is
/// reported as [`io::ErrorKind::WouldBlock`] on every platform.
pub struct FileLock {
    file: File,
    held: Mutex<HeldLock>,
}

struct HeldLock {
    active: bool,
    #[cfg(windows)]
    retained: Option<wait::RetainedLock>,
}

impl FileLock {
    /// Wraps `file` without locking it.
    #[must_use]
    pub const fn new(file: File) -> Self {
        Self {
            file,
            held: Mutex::new(HeldLock {
                active: false,
                #[cfg(windows)]
                retained: None,
            }),
        }
    }

    /// Locks `file`, waiting for every conflicting holder to release.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the lock cannot be taken.
    pub fn acquire(file: File, mode: FileLockMode) -> io::Result<Self> {
        let lock = Self::new(file);
        lock.lock(mode)?;
        Ok(lock)
    }

    /// Locks `file` only if no conflicting holder exists.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] under contention, otherwise the
    /// OS error.
    pub fn try_acquire(file: File, mode: FileLockMode) -> io::Result<Self> {
        let lock = Self::new(file);
        lock.try_lock(mode)?;
        Ok(lock)
    }

    /// Takes the lock on this file, waiting for conflicting holders.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the lock cannot be taken.
    pub fn lock(&self, mode: FileLockMode) -> io::Result<()> {
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::ensure_unheld(held.active)?;
        match mode {
            FileLockMode::Shared => self.file.lock_shared(),
            FileLockMode::Exclusive => self.file.lock(),
        }?;
        held.active = true;
        Ok(())
    }

    /// Takes the lock on this file only if no conflicting holder exists.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] under contention, otherwise the
    /// OS error.
    pub fn try_lock(&self, mode: FileLockMode) -> io::Result<()> {
        let mut held = match self.held.try_lock() {
            Ok(held) => held,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(io::ErrorKind::WouldBlock.into());
            }
        };
        Self::ensure_unheld(held.active)?;
        Self::try_lock_file(&self.file, mode)?;
        held.active = true;
        Ok(())
    }

    /// Waits for the kernel lock until the original deadline or cancellation.
    ///
    /// An unsuccessful wait leaves no pending acquisition and no held lock.
    /// The caller's original meter funds admission, notification and cleanup.
    ///
    /// # Errors
    ///
    /// Returns `TimedOut`, `Interrupted`, an admission error or the OS error.
    pub fn lock_until(
        &self,
        mode: FileLockMode,
        deadline: Instant,
        cancellation: Option<&FileLockCancellation>,
        meter: Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
    ) -> io::Result<()> {
        let mut held = match self.held.try_lock() {
            Ok(held) => held,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(io::ErrorKind::WouldBlock.into());
            }
        };
        Self::ensure_unheld(held.active)?;
        wait::check(deadline, cancellation, meter)?;
        let mut rollback = wait::rollback_credit(meter)?;
        let retained = wait::acquire(&self.file, mode, deadline, cancellation, meter)?;
        held.active = true;
        // A cancellation racing the successful kernel completion must release
        // the acquired description before returning that original failure.
        if let Err(error) = wait::check(deadline, cancellation, meter) {
            held.active = false;
            let _ = retained.release_cancelled(&self.file, &mut rollback);
            return Err(error);
        }
        #[cfg(windows)]
        {
            held.retained = Some(retained.accept());
        }
        Ok(())
    }

    /// Takes the lock on `file` only if no conflicting holder exists, and
    /// keeps it, with `file` open, until this process exits. Only the exit
    /// releases it, so it suits a process that holds the lock for its whole
    /// life and shares the description with no process that outlives it.
    ///
    /// It makes only the lock system call and allocates nothing, so a
    /// forked child may call it before exec.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] under contention, otherwise the
    /// OS error; `file` is then closed.
    #[cfg(unix)]
    pub fn try_hold_until_exit(file: File, mode: FileLockMode) -> io::Result<()> {
        Self::try_lock_file(&file, mode)?;
        // The descriptor, and with it the lock, stays until exit.
        std::mem::forget(file);
        Ok(())
    }

    /// Releases the lock for every copy of this open file description.
    ///
    /// One release attempt per acquisition: after this returns, even with an
    /// error, the handle no longer holds the lock and dropping it does not
    /// unlock again. That keeps a later lock taken through another handle on
    /// the same description intact. Unlocking an unheld handle does nothing.
    ///
    /// # Errors
    ///
    /// Returns the OS error from the unlock.
    pub fn unlock(&self) -> io::Result<()> {
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !held.active {
            return Ok(());
        }
        held.active = false;
        #[cfg(windows)]
        if let Some(retained) = held.retained.take() {
            return retained.release(&self.file);
        }
        self.file.unlock()
    }

    /// Whether this handle currently holds its lock.
    #[must_use]
    pub fn is_held(&self) -> bool {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
    }

    fn try_lock_file(file: &File, mode: FileLockMode) -> io::Result<()> {
        match mode {
            FileLockMode::Shared => file.try_lock_shared(),
            FileLockMode::Exclusive => file.try_lock(),
        }
        .map_err(io::Error::from)
    }

    fn ensure_unheld(held: bool) -> io::Result<()> {
        if held {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file lock is already held by this handle",
            ));
        }
        Ok(())
    }
}

impl std::ops::Deref for FileLock {
    type Target = File;

    fn deref(&self) -> &File {
        &self.file
    }
}

impl std::fmt::Debug for FileLock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileLock")
            .field("file", &self.file)
            .field("held", &self.held)
            .finish()
    }
}

impl std::fmt::Debug for HeldLock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.active.fmt(formatter)
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // Close alone would leave the lock to any concurrently forked child.
        let _ = self.unlock();
    }
}

#[cfg(test)]
#[path = "file_lock_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "file_lock_sources_tests.rs"]
mod sources;
