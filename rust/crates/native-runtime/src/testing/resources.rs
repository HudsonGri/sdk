//! What this process holds from the OS, read from the OS itself on every
//! platform, so per-agent cost is measured, not assumed, wherever it runs.
use std::io;
use std::path::Path;

/// Process-wide OS resources at one instant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProcessResources {
    /// Memory the process holds: resident bytes on Linux and macOS, private
    /// committed bytes on Windows.
    pub memory_bytes: u64,
    /// Threads of this process.
    pub threads: u64,
    /// Open descriptors on Unix; open kernel handles of every kind on
    /// Windows.
    pub descriptors: u64,
    /// Mounted filesystems this process sees: its mount namespace's mounts
    /// on Linux, the mounted filesystems on macOS, and the drive volumes on
    /// Windows.
    pub mounts: u64,
    /// Kernel watches beyond their descriptors: inotify watches on Linux.
    /// macOS `kqueue` and `FSEvents` and Windows directory watches each hold a
    /// descriptor or handle, which `descriptors` already counts.
    pub watches: u64,
    /// CPU time used by the process, user and system, in hundredths of a
    /// second.
    pub cpu_centiseconds: u64,
}

/// Reads this process's resources from one platform's OS.
trait Sampler {
    /// Every counter but `watches`, cheaply enough to poll.
    fn sample() -> io::Result<ProcessResources>;
    /// Watches beyond their descriptors.
    fn watches() -> io::Result<u64>;
    /// This process's open descriptors or handles on the file at `path`.
    fn descriptors_naming(path: &Path) -> io::Result<usize>;
}

#[cfg(target_os = "linux")]
type Native = linux::Procfs;
#[cfg(target_vendor = "apple")]
type Native = apple::Libproc;
#[cfg(windows)]
type Native = windows::NtProcess;

/// Every counter but `watches`, cheaply enough to poll.
pub fn sample_process_resources() -> io::Result<ProcessResources> {
    Native::sample()
}

/// Every counter, `watches` included.
pub fn process_resources() -> io::Result<ProcessResources> {
    Ok(ProcessResources {
        watches: Native::watches()?,
        ..Native::sample()?
    })
}

/// How many of this process's descriptors or handles are open on the file at
/// `path`.
pub fn descriptors_naming(path: &Path) -> io::Result<usize> {
    Native::descriptors_naming(path)
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{ProcessResources, Sampler};
    use std::io;
    use std::path::Path;

    pub(super) struct Procfs;

    impl Sampler for Procfs {
        fn sample() -> io::Result<ProcessResources> {
            let status = std::fs::read_to_string("/proc/self/status")?;
            let field = |name: &str| {
                status
                    .lines()
                    .find_map(|line| line.strip_prefix(name))
                    .and_then(|rest| rest.split_whitespace().next()?.parse::<u64>().ok())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, name.to_owned()))
            };
            let stat = std::fs::read_to_string("/proc/self/stat")?;
            // utime and stime, in clock ticks of 1/100 s.
            let cpu = stat.rsplit(')').next().map_or(0, |rest| {
                rest.split_whitespace()
                    .skip(11)
                    .take(2)
                    .filter_map(|tick| tick.parse::<u64>().ok())
                    .sum()
            });
            Ok(ProcessResources {
                memory_bytes: field("VmRSS:")? * 1024,
                threads: field("Threads:")?,
                descriptors: std::fs::read_dir("/proc/self/fd")?.count() as u64,
                mounts: std::fs::read_to_string("/proc/self/mountinfo")?
                    .lines()
                    .count() as u64,
                watches: 0,
                cpu_centiseconds: cpu,
            })
        }

        fn watches() -> io::Result<u64> {
            Ok(std::fs::read_dir("/proc/self/fdinfo")?
                .filter_map(|entry| std::fs::read_to_string(entry.ok()?.path()).ok())
                .map(|info| {
                    info.lines()
                        .filter(|line| line.starts_with("inotify"))
                        .count() as u64
                })
                .sum())
        }

        fn descriptors_naming(path: &Path) -> io::Result<usize> {
            let mut count = 0;
            for entry in std::fs::read_dir("/proc/self/fd")? {
                if std::fs::read_link(entry?.path()).is_ok_and(|target| target == path) {
                    count += 1;
                }
            }
            Ok(count)
        }
    }
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "FFI: proc_pidinfo, fcntl(F_GETPATH), getfsstat and getrusage"
)]
mod apple {
    use super::{ProcessResources, Sampler};
    use std::ffi::CStr;
    use std::io;
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::Path;

    pub(super) struct Libproc;

    /// `MAXPATHLEN` in `<sys/param.h>`: the most `F_GETPATH` writes.
    const MAXPATHLEN: usize = 1024;

    fn pid() -> io::Result<libc::c_int> {
        libc::c_int::try_from(std::process::id())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
    }

    fn size_of<T>() -> io::Result<libc::c_int> {
        libc::c_int::try_from(std::mem::size_of::<T>())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
    }

    /// This process's open descriptors.
    fn descriptors() -> io::Result<Vec<libc::proc_fdinfo>> {
        let pid = pid()?;
        let entry = size_of::<libc::proc_fdinfo>()?;
        // SAFETY: a null buffer of size 0 only asks for the size needed.
        let needed =
            unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDLISTFDS, 0, std::ptr::null_mut(), 0) };
        if needed < 0 {
            return Err(io::Error::last_os_error());
        }
        // Room for descriptors opened since, so the list is never cut short.
        let capacity = usize::try_from(needed / entry).unwrap_or(0) + 64;
        let mut entries = Vec::<libc::proc_fdinfo>::with_capacity(capacity);
        let bytes = libc::c_int::try_from(capacity * std::mem::size_of::<libc::proc_fdinfo>())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // SAFETY: proc_pidinfo writes at most `bytes` bytes, the capacity.
        let written = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDLISTFDS,
                0,
                entries.as_mut_ptr().cast(),
                bytes,
            )
        };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = usize::try_from(written / entry).unwrap_or(0).min(capacity);
        // SAFETY: the kernel initialized `count` whole entries, all plain
        // integers.
        unsafe { entries.set_len(count) };
        Ok(entries)
    }

    impl Sampler for Libproc {
        fn sample() -> io::Result<ProcessResources> {
            let pid = pid()?;
            let size = size_of::<libc::proc_taskinfo>()?;
            let mut task = std::mem::MaybeUninit::<libc::proc_taskinfo>::zeroed();
            // SAFETY: proc_pidinfo writes at most `size` bytes, the size of
            // `task`.
            let written = unsafe {
                libc::proc_pidinfo(
                    pid,
                    libc::PROC_PIDTASKINFO,
                    0,
                    task.as_mut_ptr().cast(),
                    size,
                )
            };
            if written != size {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: zeroed, then wholly written; every field is an integer.
            let task = unsafe { task.assume_init() };
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
            // SAFETY: getrusage writes one rusage into `usage`.
            if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: zeroed, then written; every field is an integer.
            let usage = unsafe { usage.assume_init() };
            let centiseconds = |time: libc::timeval| {
                u64::try_from(time.tv_sec).unwrap_or(0) * 100
                    + u64::try_from(time.tv_usec).unwrap_or(0) / 10_000
            };
            // SAFETY: a null buffer only asks for the number of mounts.
            let mounts = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
            if mounts < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(ProcessResources {
                memory_bytes: task.pti_resident_size,
                threads: u64::try_from(task.pti_threadnum).unwrap_or(0),
                descriptors: descriptors()?.len() as u64,
                mounts: u64::try_from(mounts).unwrap_or(0),
                watches: 0,
                cpu_centiseconds: centiseconds(usage.ru_utime) + centiseconds(usage.ru_stime),
            })
        }

        fn watches() -> io::Result<u64> {
            Ok(0)
        }

        fn descriptors_naming(path: &Path) -> io::Result<usize> {
            let path = std::fs::canonicalize(path)?;
            let mut count = 0;
            for descriptor in descriptors()? {
                if u32::try_from(libc::PROX_FDTYPE_VNODE).ok() != Some(descriptor.proc_fdtype) {
                    continue;
                }
                let mut buffer: [libc::c_char; MAXPATHLEN] = [0; MAXPATHLEN];
                // SAFETY: F_GETPATH writes a terminated path of at most
                // MAXPATHLEN bytes into `buffer`.
                let got = unsafe {
                    libc::fcntl(descriptor.proc_fd, libc::F_GETPATH, buffer.as_mut_ptr())
                };
                if got == -1 {
                    // Closed since the list was read.
                    continue;
                }
                // SAFETY: F_GETPATH terminated the path within `buffer`.
                let named = unsafe { CStr::from_ptr(buffer.as_ptr()) };
                if Path::new(std::ffi::OsStr::from_bytes(named.to_bytes())) == path {
                    count += 1;
                }
            }
            Ok(count)
        }
    }
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "FFI: process counters, the handle snapshot and handles' final paths"
)]
mod windows {
    use super::{ProcessResources, Sampler};
    use std::io;
    use std::os::windows::ffi::OsStringExt as _;
    use std::path::{Path, PathBuf};
    use windows_sys::Wdk::System::Threading::{
        NtQueryInformationProcess, ProcessHandleInformation,
    };
    use windows_sys::Win32::Foundation::{
        CloseHandle, FILETIME, HANDLE, INVALID_HANDLE_VALUE, STATUS_INFO_LENGTH_MISMATCH,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_TYPE_DISK, GetFileType, GetFinalPathNameByHandleW, GetLogicalDrives, VOLUME_NAME_DOS,
    };
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetCurrentProcessId, GetProcessHandleCount, GetProcessTimes,
    };

    pub(super) struct NtProcess;

    /// `PROCESS_HANDLE_SNAPSHOT_INFORMATION`'s header, before its entries.
    #[repr(C)]
    struct SnapshotHeader {
        count: usize,
        _reserved: usize,
    }

    /// `PROCESS_HANDLE_TABLE_ENTRY_INFO`.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct SnapshotEntry {
        handle: HANDLE,
        _handle_count: usize,
        _pointer_count: usize,
        _granted_access: u32,
        _object_type_index: u32,
        _handle_attributes: u32,
        _reserved: u32,
    }

    /// Every handle this process holds.
    fn handles() -> io::Result<Vec<HANDLE>> {
        // Whole words keep the entries aligned.
        let mut buffer = vec![0_u64; 8 * 1024];
        loop {
            let length = u32::try_from(buffer.len() * 8)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
            let mut needed = 0_u32;
            // SAFETY: the kernel writes at most `length` bytes into `buffer`.
            let status = unsafe {
                NtQueryInformationProcess(
                    GetCurrentProcess(),
                    ProcessHandleInformation,
                    buffer.as_mut_ptr().cast(),
                    length,
                    &mut needed,
                )
            };
            if status == STATUS_INFO_LENGTH_MISMATCH {
                let words = (needed as usize).div_ceil(8).max(buffer.len() * 2);
                buffer = vec![0_u64; words];
                continue;
            }
            if status < 0 {
                return Err(io::Error::other(format!(
                    "handle snapshot failed: {status:#x}"
                )));
            }
            let header_bytes = std::mem::size_of::<SnapshotHeader>();
            let entry_bytes = std::mem::size_of::<SnapshotEntry>();
            // SAFETY: the buffer is word-aligned and begins with the header.
            let count = unsafe { buffer.as_ptr().cast::<SnapshotHeader>().read() }.count;
            let fits = (buffer.len() * 8).saturating_sub(header_bytes) / entry_bytes;
            let base = buffer.as_ptr().cast::<u8>();
            return Ok((0..count.min(fits))
                .map(|index| {
                    // SAFETY: entry `index` lies wholly within the buffer, at
                    // an offset aligned for it.
                    unsafe {
                        base.add(header_bytes + index * entry_bytes)
                            .cast::<SnapshotEntry>()
                            .read()
                    }
                    .handle
                })
                .collect());
        }
    }

    /// The final path of the file `handle` opens, if it opens a disk file.
    fn final_path(handle: HANDLE) -> Option<PathBuf> {
        // SAFETY: GetFileType only queries the handle; it answers unknown for
        // handles that are not files and never blocks on pipes.
        if unsafe { GetFileType(handle) } != FILE_TYPE_DISK {
            return None;
        }
        let mut buffer = vec![0_u16; 1024];
        loop {
            let capacity = u32::try_from(buffer.len()).ok()?;
            // SAFETY: writes at most `capacity` UTF-16 units into `buffer`.
            let written = unsafe {
                GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), capacity, VOLUME_NAME_DOS)
            };
            let written = written as usize;
            if written == 0 {
                return None;
            }
            if written < buffer.len() {
                buffer.truncate(written);
                return Some(PathBuf::from(std::ffi::OsString::from_wide(&buffer)));
            }
            buffer = vec![0; written + 1];
        }
    }

    fn threads() -> io::Result<u64> {
        // SAFETY: a snapshot of every thread, closed below.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: plain data; dwSize names its size, as Thread32First needs.
        let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
        entry.dwSize = u32::try_from(std::mem::size_of::<THREADENTRY32>()).unwrap_or(u32::MAX);
        // SAFETY: GetCurrentProcessId has no preconditions.
        let process = unsafe { GetCurrentProcessId() };
        let mut threads = 0;
        // SAFETY: each call writes one entry into `entry` from the open
        // snapshot.
        let mut more = unsafe { Thread32First(snapshot, &mut entry) } != 0;
        while more {
            if entry.th32OwnerProcessID == process {
                threads += 1;
            }
            // SAFETY: as above.
            more = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
        }
        // SAFETY: the snapshot is open and closed once.
        unsafe { CloseHandle(snapshot) };
        Ok(threads)
    }

    impl Sampler for NtProcess {
        fn sample() -> io::Result<ProcessResources> {
            // SAFETY: the pseudo-handle for this process needs no closing.
            let process = unsafe { GetCurrentProcess() };
            let mut handles = 0_u32;
            // SAFETY: writes the count into `handles`.
            if unsafe { GetProcessHandleCount(process, &mut handles) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: plain data, valid when zeroed.
            let mut memory: PROCESS_MEMORY_COUNTERS_EX = unsafe { std::mem::zeroed() };
            memory.cb = u32::try_from(std::mem::size_of::<PROCESS_MEMORY_COUNTERS_EX>())
                .unwrap_or(u32::MAX);
            // SAFETY: `cb` tells the call it may write the extended counters.
            let read = unsafe {
                GetProcessMemoryInfo(
                    process,
                    (&raw mut memory).cast::<PROCESS_MEMORY_COUNTERS>(),
                    memory.cb,
                )
            };
            if read == 0 {
                return Err(io::Error::last_os_error());
            }
            let zero = FILETIME {
                dwLowDateTime: 0,
                dwHighDateTime: 0,
            };
            let (mut created, mut exited, mut kernel, mut user) = (zero, zero, zero, zero);
            // SAFETY: writes the four times.
            if unsafe {
                GetProcessTimes(process, &mut created, &mut exited, &mut kernel, &mut user)
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            // 100 ns units; 100,000 of them are a hundredth of a second.
            let units = |time: FILETIME| {
                ((u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime)) / 100_000
            };
            // SAFETY: no preconditions.
            let drives = unsafe { GetLogicalDrives() };
            Ok(ProcessResources {
                memory_bytes: memory.PrivateUsage as u64,
                threads: threads()?,
                descriptors: u64::from(handles),
                mounts: u64::from(drives.count_ones()),
                watches: 0,
                cpu_centiseconds: units(kernel) + units(user),
            })
        }

        fn watches() -> io::Result<u64> {
            Ok(0)
        }

        fn descriptors_naming(path: &Path) -> io::Result<usize> {
            let path = std::fs::canonicalize(path)?;
            Ok(handles()?
                .into_iter()
                .filter_map(final_path)
                .filter(|named| *named == path)
                .count())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each platform's counts follow real opens and closes, so a sampler that
    /// answered a constant would fail here.
    #[test]
    fn the_sampler_counts_the_descriptors_this_process_opens() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("counted");
        std::fs::write(&path, b"counted")?;
        assert_eq!(descriptors_naming(&path)?, 0);
        let before = sample_process_resources()?;
        let files = (0..64)
            .map(|_| std::fs::File::open(&path))
            .collect::<io::Result<Vec<_>>>()?;
        let open = sample_process_resources()?;
        assert_eq!(descriptors_naming(&path)?, files.len());
        // Other tests open and close descriptors meanwhile, but not 64 more
        // than they close.
        assert!(
            open.descriptors >= before.descriptors + 32,
            "{before:?} -> {open:?}"
        );
        drop(files);
        assert_eq!(descriptors_naming(&path)?, 0);
        assert!(open.memory_bytes > 0 && open.threads > 0, "{open:?}");
        assert!(process_resources()?.mounts > 0);
        Ok(())
    }
}
