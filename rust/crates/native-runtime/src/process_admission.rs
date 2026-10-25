//! A process host holds admission before fencing and until physical drain.
//!
//! The process that can create custody records also owns the lease. Killing
//! a caller cannot release admission while its still-live host creates a new
//! record after a successor's scan. A killed host cannot create another record;
//! its successor fences the durable records before publishing readiness.

use crate::ProcessCustody;
use std::io;
use std::path::Path;
use std::sync::Arc;

/// Admission and custody must have the same owner and lifetime.
pub struct ProcessAdmission {
    custody: Arc<ProcessCustody>,
    _lease: crate::FileLock,
    lease_wait: std::time::Duration,
    #[cfg(windows)]
    _fleet: Option<platform::Lease>,
}

impl ProcessAdmission {
    /// Acquires the host lease, then fences the preceding owner's trees.
    pub fn acquire(root: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(root)?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("admission.lock"))?;
        let started = std::time::Instant::now();
        let lease = crate::FileLock::acquire(file, crate::FileLockMode::Exclusive)?;
        let lease_wait = started.elapsed();
        let custody = ProcessCustody::acquire(&root.join("trees"))?;
        Ok(Self {
            custody,
            _lease: lease,
            lease_wait,
            #[cfg(windows)]
            _fleet: None,
        })
    }

    /// The Windows runtime fleet additionally shares its established named
    /// mutex with callers of the installed runner. Custody policy stays here.
    #[cfg(windows)]
    pub fn acquire_fleet(root: &Path) -> io::Result<Self> {
        let started = std::time::Instant::now();
        let fleet = platform::Lease::acquire(root)?;
        let fleet_wait = started.elapsed();
        let mut admission = Self::acquire(root)?;
        admission.lease_wait += fleet_wait;
        admission._fleet = Some(fleet);
        Ok(admission)
    }

    /// Custody is available only while this admission remains held.
    pub fn custody(&self) -> &ProcessCustody {
        &self.custody
    }

    /// Time spent acquiring exclusion, excluding startup and physical fencing.
    pub const fn lease_wait(&self) -> std::time::Duration {
        self.lease_wait
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custody_is_never_available_without_its_exclusive_admission() {
        let root = tempfile::tempdir().expect("admission directory");
        let admission = ProcessAdmission::acquire(root.path()).expect("admit");
        let open = || {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(root.path().join("admission.lock"))
                .expect("lease file")
        };
        assert_eq!(
            crate::FileLock::try_acquire(open(), crate::FileLockMode::Exclusive)
                .expect_err("a record creator must exclude every successor")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(admission.custody().fence_stale().expect("empty custody"), 0);
        drop(admission);
        let next = crate::FileLock::try_acquire(open(), crate::FileLockMode::Exclusive)
            .expect("released after custody");
        drop(next);
        ProcessAdmission::acquire(root.path()).expect("successor fences before admission");
    }
}

#[cfg(windows)]
mod platform {
    #![allow(
        unsafe_code,
        reason = "the Windows fleet lease is a named kernel mutex"
    )]
    use super::*;
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
    use windows_sys::Win32::Foundation::{WAIT_ABANDONED, WAIT_OBJECT_0};
    use windows_sys::Win32::System::Threading::{
        CreateMutexW, INFINITE, ReleaseMutex, WaitForSingleObject,
    };
    pub(super) struct Lease {
        handle: OwnedHandle,
        // A Windows mutex must be released by its acquiring thread.
        _thread: std::marker::PhantomData<std::rc::Rc<()>>,
    }
    impl Lease {
        pub(super) fn acquire(_root: &Path) -> io::Result<Self> {
            let name: Vec<u16> = "Global\\AcyclicWindowsTestRun"
                .encode_utf16()
                .chain(Some(0))
                .collect();
            // SAFETY: a terminated name; ownership is transferred immediately.
            let raw = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
            if raw.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the newly created handle belongs to this owner.
            let handle = unsafe { OwnedHandle::from_raw_handle(raw.cast()) };
            // SAFETY: the handle stays owned throughout the blocking wait.
            match unsafe { WaitForSingleObject(raw, INFINITE) } {
                WAIT_OBJECT_0 | WAIT_ABANDONED => Ok(Self {
                    handle,
                    _thread: std::marker::PhantomData,
                }),
                _ => Err(io::Error::last_os_error()),
            }
        }
    }
    impl Drop for Lease {
        fn drop(&mut self) {
            // SAFETY: acquisition and release occur on this host's main thread.
            unsafe { ReleaseMutex(self.handle.as_raw_handle().cast()) };
        }
    }
}
