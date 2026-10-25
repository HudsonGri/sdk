//! macOS containment includes service launchers which create process groups.
//! The live session leader reserves the session identity until drain completes.
use libc::{c_int, pid_t};
use std::io;

pub(super) fn enter(owner: pid_t) -> io::Result<()> {
    // Command initially creates a process group. Join the inherited owner's
    // group before setsid, which requires the caller not to be a group leader.
    // SAFETY: only this unexeced child changes its own group and session.
    unsafe {
        let inherited = libc::getpgid(owner);
        if inherited < 0 || libc::setpgid(0, inherited) != 0 || libc::setsid() < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Kernel storage used between fork and exec: no allocator or inherited locks.
struct Pids {
    memory: *mut libc::c_void,
    bytes: usize,
    count: usize,
}

impl Drop for Pids {
    fn drop(&mut self) {
        // SAFETY: this mapping is exclusively owned, with its original size.
        unsafe { libc::munmap(self.memory, self.bytes) };
    }
}

impl Pids {
    fn snapshot() -> io::Result<Self> {
        // SAFETY: PROC_ALL_PIDS with no output asks the kernel for byte capacity.
        let required = unsafe { libc::proc_listpids(1, 0, std::ptr::null_mut(), 0) };
        let mut bytes = usize::try_from(required).map_err(|_| io::Error::last_os_error())?;
        loop {
            bytes = bytes
                .checked_mul(2)
                .filter(|bytes| *bytes > 0)
                .ok_or(io::ErrorKind::InvalidData)?;
            let capacity = c_int::try_from(bytes).map_err(|_| io::ErrorKind::InvalidData)?;
            // SAFETY: anonymous private writable kernel-owned mapping.
            let memory = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    bytes,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if memory == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            let mut pids = Self {
                memory,
                bytes,
                count: 0,
            };
            // SAFETY: mapping has exactly the supplied writable capacity.
            let filled = unsafe { libc::proc_listpids(1, 0, memory, capacity) };
            let filled = usize::try_from(filled).map_err(|_| io::Error::last_os_error())?;
            if filled < bytes {
                pids.count = filled / std::mem::size_of::<pid_t>();
                return Ok(pids);
            }
            // Concurrent births filled the snapshot; grow its owned storage.
        }
    }

    fn values(&self) -> &[pid_t] {
        // SAFETY: mmap alignment and the initialized kernel PID prefix.
        unsafe { std::slice::from_raw_parts(self.memory.cast(), self.count) }
    }
}

fn live_member(pid: pid_t, session: pid_t) -> io::Result<bool> {
    // Only inspect owned members: unrelated users' BSD process information
    // can be private even though their public session identity is readable.
    // SAFETY: getsid only reads the named process's session.
    let observed = unsafe { libc::getsid(pid) };
    if observed < 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error)
        };
    }
    if observed != session {
        return Ok(false);
    }
    // SAFETY: read-only process inspection into plain initialized output fields.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size =
        c_int::try_from(std::mem::size_of_val(&info)).map_err(|_| io::ErrorKind::InvalidData)?;
    // SAFETY: output has the stated capacity for PROC_PIDTBSDINFO.
    if unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size) }
        != size
    {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error)
        };
    }
    // XNU SZOMB is 5: exit notification already proved this generation cannot act.
    // SAFETY: getsid only reads the named process's session.
    Ok(info.pbi_status != 5 && unsafe { libc::getsid(pid) } == session)
}

pub(in crate::process_tree) fn drain(queue: c_int, session: pid_t) -> io::Result<()> {
    loop {
        let pids = Pids::snapshot()?;
        let mut pending = false;
        for pid in pids
            .values()
            .iter()
            .copied()
            .filter(|pid| *pid > 0 && *pid != session)
        {
            if live_member(pid, session)?
                && super::watch_exit(queue, pid)?
                && live_member(pid, session)?
            {
                // SAFETY: a live member of the exclusively owned session.
                if unsafe { libc::kill(pid, libc::SIGKILL) } != 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error);
                    }
                }
                pending = true;
            }
        }
        if !pending {
            return Ok(());
        }
        super::next_exit(queue)?;
        // Re-enumerate after kernel exit: a dying member may have forked.
    }
}
