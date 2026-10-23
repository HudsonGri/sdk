//! Register unlock notifications before closing the nonblocking-acquire race.
pub(super) use super::unix::Signal;
use super::unix::{Descriptor, wait};
use super::*;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

// XNU bsd/sys/event.h: NOTE_FUNLOCK reports a file-lock release.
const NOTE_FUNLOCK: u32 = 0x100;

pub(super) fn acquire(
    file: &File,
    mode: FileLockMode,
    deadline: Instant,
    cancellation: Option<&FileLockCancellation>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    let queue = Descriptor::open(meter, || {
        // SAFETY: kqueue creates one fresh descriptor.
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: ownership of the fresh successful kqueue descriptor transfers here.
        Ok(unsafe { OwnedFd::from_raw_fd(raw) })
    })?;
    let change = libc::kevent {
        ident: libc::uintptr_t::try_from(file.as_raw_fd()).map_err(io::Error::other)?,
        filter: libc::EVFILT_VNODE,
        flags: libc::EV_ADD | libc::EV_CLEAR,
        fflags: NOTE_FUNLOCK,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    attempt(meter, NativeOperationKind::Read, || {
        // SAFETY: one initialized change, an owned file and queue; no output.
        if unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &raw const change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    })?;
    loop {
        check(deadline, cancellation, meter)?;
        // The registration is already live, so an unlock racing this attempt
        // leaves a queued event rather than a missed wake.
        match try_file(file, mode, meter) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        wait(queue.as_raw_fd(), deadline, cancellation, meter)?;
        let mut event = change;
        let zero = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        attempt(meter, NativeOperationKind::Read, || {
            // SAFETY: the retained queue, writable event and zero timeout are live.
            let count = unsafe {
                libc::kevent(
                    queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    &raw mut event,
                    1,
                    &raw const zero,
                )
            };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 0 || event.flags & libc::EV_ERROR != 0 || event.fflags & NOTE_FUNLOCK == 0 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(())
        })?;
    }
}
