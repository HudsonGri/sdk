//! One native full flush of a held descriptor, with no internal retry.
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd as _;
use std::os::fd::BorrowedFd;

/// Issues exactly one native full flush of `descriptor`: `fcntl(F_FULLFSYNC)`
/// on Apple, where a bare `fsync` stops at the drive cache, and `fsync` on
/// Linux. `EINTR` and every other failure return to the caller, whose metered
/// loop owns retry, charging and cancellation for each native attempt.
/// Unlike std's `sync_all`, nothing is retried here, and Apple never falls
/// back to a weaker flush.
#[cfg_attr(
    target_os = "linux",
    allow(
        unsafe_code,
        reason = "one flush call on the caller's borrowed descriptor"
    )
)]
pub fn sync_descriptor_once(descriptor: BorrowedFd<'_>) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    return crate::apple::sync_descriptor_once(descriptor);
    #[cfg(target_os = "linux")]
    {
        // SAFETY: the borrowed descriptor stays open for this one flush.
        let result = unsafe { libc::fsync(descriptor.as_raw_fd()) };
        if result == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::File, io::Write as _, os::fd::AsFd as _};

    // Checkout token publication flushes files and each held ancestor. Test
    // both against the real filesystem, and a descriptor that cannot flush.
    // On Apple, ENODEV also distinguishes F_FULLFSYNC from bare fsync's
    // EINVAL. A successful no-op cannot satisfy either host's detector.
    fn full_flush_contract(flush: impl Fn(BorrowedFd<'_>) -> io::Result<()>) -> io::Result<bool> {
        let directory = tempfile::tempdir()?;
        let mut file = File::create(directory.path().join("record"))?;
        file.write_all(b"incarnation")?;
        flush(file.as_fd())?;
        flush(File::open(directory.path())?.as_fd())?;
        let device = File::open("/dev/null")?;
        #[cfg(target_vendor = "apple")]
        let expected = libc::ENODEV;
        #[cfg(target_os = "linux")]
        let expected = libc::EINVAL;
        Ok(matches!(flush(device.as_fd()), Err(error) if error.raw_os_error() == Some(expected)))
    }

    #[test]
    fn full_flush_supports_regular_files_and_directories_and_preserves_errors() {
        assert!(full_flush_contract(sync_descriptor_once).unwrap());
    }

    #[test]
    #[cfg(target_vendor = "apple")]
    fn negative_control_bare_fsync_does_not_meet_the_full_flush_contract() {
        assert!(
            !full_flush_contract(
                |descriptor| rustix::fs::fsync(descriptor).map_err(io::Error::from)
            )
            .unwrap()
        );
    }

    #[test]
    fn negative_control_swallowing_flush_errors_does_not_meet_the_contract() {
        assert!(!full_flush_contract(|_| Ok(())).unwrap());
    }
}
