//! A cancellable helper waits on the same open file description, then exits.
//! Only async-signal-safe syscalls run after fork. It inherits no unrelated
//! descriptors and dies with its parent. Reaping always precedes rollback.
pub(super) use super::unix::Signal;
use super::unix::{Descriptor, wait};
use super::*;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicI32, AtomicU32};

#[derive(Default)]
struct Progress {
    reads: AtomicU32,
    writes: AtomicU32,
    result: AtomicI32,
    complete: AtomicBool,
}

struct Shared {
    pointer: *mut Progress,
    cleanup: Option<Credit>,
    _allocation: Option<ReadAllocation>,
}

impl Shared {
    fn new(meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<Self> {
        let mut shared = Self {
            pointer: std::ptr::null_mut(),
            cleanup: credit(meter, NativeOperationKind::Write, 1)?,
            _allocation: meter
                .map(|meter| ReadAllocation::new(meter, size_of::<Progress>()))
                .transpose()?,
        };
        attempt(meter, NativeOperationKind::Read, || {
            // SAFETY: a fresh shared mapping of the exact nonzero control size.
            let pointer = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    size_of::<Progress>(),
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };
            if pointer == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            shared.pointer = pointer.cast();
            // SAFETY: the new mapping is aligned, writable, and exclusively initialized.
            unsafe {
                shared.pointer.write(Progress::default());
            }
            Ok(())
        })?;
        Ok(shared)
    }
    fn get(&self) -> &Progress {
        // SAFETY: initialized before fork and unmapped only after the child is reaped.
        unsafe { &*self.pointer }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        if !self.pointer.is_null() {
            let unmap = || {
                // SAFETY: exactly this owned mapping, with no remaining users.
                unsafe { libc::munmap(self.pointer.cast(), size_of::<Progress>()) };
            };
            match self.cleanup.as_mut() {
                Some(credit) => {
                    let _ = credit.attempt(unmap);
                }
                None => unmap(),
            }
        }
    }
}

struct Helper<'a> {
    pid: libc::pid_t,
    file: &'a File,
    shared: Shared,
    child_reads: Option<Credit>,
    child_writes: Option<Credit>,
    cleanup: Option<Credit>,
    acquired: bool,
}

impl Helper<'_> {
    fn reap(&mut self) {
        if self.pid <= 0 {
            return;
        }
        let kill = || {
            // SAFETY: the unreaped direct child exclusively owns this PID.
            unsafe { libc::kill(self.pid, libc::SIGKILL) };
        };
        match self.cleanup.as_mut() {
            Some(credit) => {
                let _ = credit.attempt(kill);
            }
            None => kill(),
        }
        let reap = || {
            // SAFETY: a direct child; EINTR never transfers reaping custody.
            while unsafe { libc::waitpid(self.pid, std::ptr::null_mut(), 0) } < 0 {
                if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                    break;
                }
            }
        };
        match self.cleanup.as_mut() {
            Some(credit) => {
                let _ = credit.attempt(reap);
            }
            None => reap(),
        }
        self.pid = 0;
        for (credit, count) in [
            (
                &mut self.child_reads,
                self.shared.get().reads.load(Ordering::Acquire),
            ),
            (
                &mut self.child_writes,
                self.shared.get().writes.load(Ordering::Acquire),
            ),
        ] {
            if let Some(credit) = credit {
                for _ in 0..count {
                    let _ = credit.attempt(|| ());
                }
            }
        }
    }
}

impl Drop for Helper<'_> {
    fn drop(&mut self) {
        self.reap();
        if !self.acquired {
            // A cancelled helper may have acquired just before it was killed.
            // Explicit rollback releases the shared description in that race.
            match self.cleanup.as_mut() {
                Some(credit) => {
                    let _ = credit.attempt(|| self.file.unlock());
                }
                None => {
                    let _ = self.file.unlock();
                }
            }
        }
    }
}

pub(super) fn acquire(
    file: &File,
    mode: FileLockMode,
    deadline: Instant,
    cancellation: Option<&FileLockCancellation>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    let mut helper = Helper {
        pid: 0,
        file,
        shared: Shared::new(meter)?,
        child_reads: credit(meter, NativeOperationKind::Read, 2)?,
        child_writes: credit(meter, NativeOperationKind::Write, 5)?,
        cleanup: credit(meter, NativeOperationKind::Write, 3)?,
        acquired: false,
    };
    let parent = std::process::id();
    let spawned = attempt(meter, NativeOperationKind::Write, || {
        // Raw clone with SIGCHLD and no sharing flags has fork semantics:
        // the child has its own copy-on-write stack and address space. The
        // optional TID/TLS arguments are all null, including on architectures
        // that reverse their order. These ABIs include Linux targets with no
        // fork syscall. Neither path runs inherited libc atfork handlers.
        #[cfg(any(
            target_arch = "x86_64",
            target_arch = "x86",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "riscv32",
            target_arch = "loongarch64"
        ))]
        // SAFETY: after the syscall the child runs only async-signal-safe
        // operations and _exit; it never runs inherited Rust destructors.
        let pid = unsafe {
            libc::syscall(
                libc::SYS_clone,
                libc::SIGCHLD,
                0_usize,
                0_usize,
                0_usize,
                0_usize,
            )
        };
        #[cfg(not(any(
            target_arch = "x86_64",
            target_arch = "x86",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "riscv32",
            target_arch = "loongarch64"
        )))]
        // SAFETY: the remaining Linux ABIs expose fork directly; the child
        // follows the same restricted path with its private copied stack.
        let pid = unsafe { libc::syscall(libc::SYS_fork) };
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }
        if pid == 0 {
            child(file.as_raw_fd(), mode, parent, helper.shared.get());
        }
        helper.pid = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
        Ok(())
    });
    spawned?;
    let exit = Descriptor::open(meter, || {
        // SAFETY: the retained, unreaped direct child owns this PID.
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, helper.pid, 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: pidfd_open returned this fresh owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(i32::try_from(raw).map_err(io::Error::other)?) })
    })?;
    wait(exit.as_raw_fd(), deadline, cancellation, meter)?;
    helper.reap();
    check(deadline, cancellation, meter)?;
    let progress = helper.shared.get();
    if !progress.complete.load(Ordering::Acquire) {
        return Err(io::ErrorKind::Interrupted.into());
    }
    let error = progress.result.load(Ordering::Acquire);
    if error != 0 {
        return Err(io::Error::from_raw_os_error(error));
    }
    helper.acquired = true;
    Ok(())
}

fn child(fd: i32, mode: FileLockMode, parent: u32, progress: &Progress) -> ! {
    // SAFETY: after raw fork only lock-free atomics and async-signal-safe raw
    // syscalls execute; no allocator, Rust destructor or inherited mutex runs.
    unsafe {
        progress.writes.fetch_add(1, Ordering::Release);
        let mut error = if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == 0 {
            0
        } else {
            *libc::__errno_location()
        };
        progress.reads.fetch_add(1, Ordering::Release);
        if libc::getppid() != parent as libc::pid_t {
            libc::_exit(1);
        }
        for (first, last) in [
            (0, fd.cast_unsigned().wrapping_sub(1)),
            (fd.cast_unsigned() + 1, u32::MAX),
        ] {
            if first > last || (fd == 0 && first == 0) {
                continue;
            }
            progress.writes.fetch_add(1, Ordering::Release);
            if libc::syscall(libc::SYS_close_range, first, last, 0) != 0 && error == 0 {
                error = *libc::__errno_location();
            }
        }
        if error == 0 {
            progress.reads.fetch_add(1, Ordering::Release);
            let flags = match mode {
                FileLockMode::Shared => libc::LOCK_SH,
                FileLockMode::Exclusive => libc::LOCK_EX,
            };
            if libc::flock(fd, flags) != 0 {
                error = *libc::__errno_location();
            }
        }
        progress.result.store(error, Ordering::Release);
        progress.complete.store(true, Ordering::Release);
        progress.writes.fetch_add(1, Ordering::Release);
        libc::_exit(0);
    }
}
