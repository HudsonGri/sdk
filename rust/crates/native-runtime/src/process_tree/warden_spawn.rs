//! Starts custody with `posix_spawn`'s fresh address space, never a copy of the
//! multithreaded host. The fresh executable's initializer receives the held
//! directory over a private socket, prepares exec, then enters the warden.
//! Only async-signal-safe operations follow the warden's own fork.

use super::{ProcessCustody, ProcessTree, ProcessTreeCommand, custody, warden};
use libc::{c_char, c_int};
use std::ffi::{CStr, CString, OsStr};
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::net::{SocketAddr, UnixListener, UnixStream};
use std::process::Command;

const SWITCH: &[u8] = b"--acyclic-process-warden-v1";

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[used]
#[unsafe(link_section = ".init_array.00000")]
static ENTRY: extern "C" fn(c_int, *const *const c_char, *const *const c_char) = entry;

// glibc passes argc/argv to initializers. Other Linux libcs use void(void);
// obtain the same argument bytes from procfs, already required by the warden.
#[cfg(all(target_os = "linux", not(target_env = "gnu")))]
#[used]
#[unsafe(link_section = ".init_array.00000")]
static ENTRY: extern "C" fn() = proc_entry;

#[cfg(all(target_os = "linux", not(target_env = "gnu")))]
extern "C" fn proc_entry() {
    let Ok(bytes) = std::fs::read("/proc/self/cmdline") else {
        return;
    };
    let Ok(arguments) = bytes
        .strip_suffix(&[0])
        .unwrap_or(bytes.as_slice())
        .split(|byte| *byte == 0)
        .map(CString::new)
        .collect::<Result<Vec<_>, _>>()
    else {
        return;
    };
    let Ok(count) = c_int::try_from(arguments.len()) else {
        return;
    };
    let pointers = arguments
        .iter()
        .map(|argument| argument.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect::<Vec<_>>();
    entry(count, pointers.as_ptr(), std::ptr::null());
}

#[cfg(target_vendor = "apple")]
#[used]
#[unsafe(link_section = "__DATA,__mod_init_func")]
static ENTRY: extern "C" fn() = apple_entry;

#[cfg(target_vendor = "apple")]
extern "C" fn apple_entry() {
    unsafe extern "C" {
        fn _NSGetArgc() -> *const c_int;
        fn _NSGetArgv() -> *const *const *const c_char;
    }
    // SAFETY: dyld's process argument and environment storage lives until exit.
    unsafe { entry(*_NSGetArgc(), *_NSGetArgv(), std::ptr::null()) };
}

extern "C" fn entry(argc: c_int, argv: *const *const c_char, _env: *const *const c_char) {
    if argc < 2 || argv.is_null() {
        return;
    }
    // SAFETY: startup supplied argc arguments, each NUL-terminated.
    if unsafe { CStr::from_ptr(*argv.add(1)) }.to_bytes() != SWITCH {
        return;
    }
    if argc >= 5 {
        // SAFETY: the arguments and environment are startup's live arrays.
        let _ = unsafe { serve(argv) };
    }
    // A malformed or failed internal invocation must never enter host main.
    // SAFETY: this fresh helper has no host cleanup to run.
    unsafe { libc::_exit(127) }
}

pub(super) fn spawn(
    target: &mut ProcessTreeCommand,
    custody: &ProcessCustody,
) -> io::Result<ProcessTree> {
    // Retain the initializer in every executable which can start a warden.
    std::hint::black_box(&ENTRY);
    let directory = custody.directory()?;
    let descriptor = directory.as_raw_fd();
    let environment = target.environment()?;
    let cwd = target
        .as_command()
        .get_current_dir()
        .map(|path| path.as_os_str().as_bytes().to_vec());
    let endpoint = Endpoint::new()?;
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg(OsStr::from_bytes(SWITCH))
        .arg(&endpoint.name)
        .arg(std::process::id().to_string())
        .arg(target.as_command().get_program());
    target.configure_warden(&mut command);
    // No pre_exec hook: std uses posix_spawn, including its stdio actions.
    let mut tree = ProcessTree::spawn(&mut command)?;
    let pid = libc::pid_t::try_from(tree.owned_child_id()?)
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    tree.custody = Some(custody::Record::new(directory, pid));
    let admitted = (|| {
        let mut channel = endpoint.accept(&mut tree, pid)?;
        send_directory(&channel, descriptor)?;
        write_bytes(&mut channel, &environment)?;
        channel.write_all(&[u8::from(cwd.is_some())])?;
        if let Some(cwd) = &cwd {
            write_bytes(&mut channel, cwd)?;
        }
        // The warden closes its copy; the program's copy is CLOEXEC. EOF
        // therefore acknowledges exec, exactly like a spawn error pipe.
        let mut error = [0; size_of::<c_int>()];
        let mut read = 0;
        while read < error.len() {
            let remaining = error.get_mut(read..).ok_or(io::ErrorKind::InvalidData)?;
            match channel.read(remaining) {
                Ok(0) if read == 0 => return Ok(()),
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(count) => read += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::from_raw_os_error(c_int::from_ne_bytes(error)))
    })();
    if let Err(error) = admitted {
        tree.terminate(None)?;
        return Err(error);
    }
    Ok(tree)
}

struct Endpoint {
    listener: UnixListener,
    name: std::ffi::OsString,
    #[cfg(target_vendor = "apple")]
    _directory: tempfile::TempDir,
}

impl Endpoint {
    fn new() -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        let name: std::ffi::OsString = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            format!(
                "acyclic-warden-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )
            .into()
        };
        #[cfg(target_vendor = "apple")]
        let directory = tempfile::Builder::new()
            .prefix("acyclic-warden-")
            .tempdir_in("/tmp")?;
        #[cfg(target_vendor = "apple")]
        let name = directory.path().join("socket").into_os_string();
        let listener = UnixListener::bind_addr(&address(&name)?)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            name,
            #[cfg(target_vendor = "apple")]
            _directory: directory,
        })
    }

    fn accept(&self, tree: &mut ProcessTree, pid: libc::pid_t) -> io::Result<UnixStream> {
        loop {
            match self.listener.accept() {
                Ok((channel, _)) => {
                    verify_peer(&channel, pid)?;
                    return Ok(channel);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if tree.try_wait(None)?.is_some() {
                        return Err(io::Error::other("process warden exited before handoff"));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn address(name: &OsStr) -> io::Result<SocketAddr> {
    #[cfg(target_os = "linux")]
    {
        use std::os::linux::net::SocketAddrExt as _;
        SocketAddr::from_abstract_name(name.as_bytes())
    }
    #[cfg(target_vendor = "apple")]
    SocketAddr::from_pathname(name)
}

fn verify_peer(channel: &UnixStream, pid: libc::pid_t) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: all-zero is a valid credentials buffer.
        let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
        let mut length = libc::socklen_t::try_from(size_of_val(&credentials))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        // SAFETY: an owned socket and correctly sized writable buffer.
        if unsafe {
            libc::getsockopt(
                channel.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&raw mut credentials).cast(),
                &raw mut length,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if credentials.pid != pid {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
    }
    #[cfg(target_vendor = "apple")]
    {
        let mut peer = 0;
        let mut length = libc::socklen_t::try_from(size_of_val(&peer))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        // SAFETY: an owned socket and a writable pid_t buffer.
        if unsafe {
            libc::getsockopt(
                channel.as_raw_fd(),
                0,
                libc::LOCAL_PEERPID,
                (&raw mut peer as *mut libc::pid_t).cast(),
                &raw mut length,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        if peer != pid {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
    }
    Ok(())
}

/// One descriptor, transferred atomically without making any host descriptor
/// inheritable. Other threads may spawn concurrently throughout the handoff.
fn send_directory(channel: &UnixStream, directory: RawFd) -> io::Result<()> {
    let (space, length) = directory_message_lengths()?;
    let mut byte = [0_u8];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut control = [0_usize; 8];
    // SAFETY: all-zero is an empty msghdr.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &raw mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = space;
    // SAFETY: msg_control is aligned and has CMSG_SPACE bytes.
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&raw const message);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = length;
        libc::CMSG_DATA(header)
            .cast::<RawFd>()
            .write_unaligned(directory);
    }
    loop {
        // SAFETY: all message storage is live, including the borrowed fd.
        let sent =
            unsafe { libc::sendmsg(channel.as_raw_fd(), &raw const message, libc::MSG_NOSIGNAL) };
        if sent == 1 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if sent >= 0 {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn receive_directory(channel: &UnixStream) -> io::Result<OwnedFd> {
    let (space, length) = directory_message_lengths()?;
    let mut byte = [0_u8];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut control = [0_usize; 8];
    // SAFETY: all-zero is an empty msghdr.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &raw mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = space;
    loop {
        // SAFETY: the message points to writable byte/control storage.
        let read = unsafe { libc::recvmsg(channel.as_raw_fd(), &raw mut message, 0) };
        if read == 1 {
            break;
        }
        if read >= 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    // SAFETY: recvmsg initialized the control buffer; check its header before use.
    let fd = unsafe {
        let header = libc::CMSG_FIRSTHDR(&raw const message);
        if header.is_null()
            || (*header).cmsg_level != libc::SOL_SOCKET
            || (*header).cmsg_type != libc::SCM_RIGHTS
            || message.msg_flags & libc::MSG_CTRUNC != 0
            || (*header).cmsg_len != length
        {
            return Err(io::ErrorKind::InvalidData.into());
        }
        libc::CMSG_DATA(header).cast::<RawFd>().read_unaligned()
    };
    // SAFETY: recvmsg gave this helper sole ownership of the received descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: setting CLOEXEC on this owned descriptor before any fork.
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

#[cfg(all(target_os = "linux", not(target_env = "musl")))]
type ControlLength = usize;
#[cfg(any(target_vendor = "apple", target_env = "musl"))]
type ControlLength = libc::socklen_t;

fn directory_message_lengths() -> io::Result<(ControlLength, ControlLength)> {
    let bytes = size_of::<RawFd>()
        .try_into()
        .map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: the fixed descriptor payload fits both CMSG calculations.
    let (space, length) = unsafe { (libc::CMSG_SPACE(bytes), libc::CMSG_LEN(bytes)) };
    #[cfg(all(target_os = "linux", not(target_env = "musl")))]
    let (space, length) = (
        space.try_into().map_err(|_| io::ErrorKind::InvalidInput)?,
        length.try_into().map_err(|_| io::ErrorKind::InvalidInput)?,
    );
    Ok((space, length))
}

fn write_bytes(channel: &mut UnixStream, bytes: &[u8]) -> io::Result<()> {
    channel.write_all(
        &u64::try_from(bytes.len())
            .map_err(|_| io::ErrorKind::InvalidInput)?
            .to_ne_bytes(),
    )?;
    channel.write_all(bytes)
}

fn read_bytes(channel: &mut UnixStream) -> io::Result<Vec<u8>> {
    let mut length = [0; size_of::<u64>()];
    channel.read_exact(&mut length)?;
    let length =
        usize::try_from(u64::from_ne_bytes(length)).map_err(|_| io::ErrorKind::InvalidInput)?;
    let mut bytes = vec![0; length];
    channel.read_exact(&mut bytes)?;
    Ok(bytes)
}

/// All allocation and path search preparation happens in the fresh helper,
/// before enter forks. Neither branch copies a multithreaded host's locks.
unsafe fn serve(argv: *const *const c_char) -> io::Result<()> {
    // SAFETY: entry checked the number of startup arguments.
    let endpoint = unsafe { CStr::from_ptr(*argv.add(2)) };
    let mut channel = UnixStream::connect_addr(&address(OsStr::from_bytes(endpoint.to_bytes()))?)?;
    let prepared = (|| {
        // SAFETY: entry checked the number of startup arguments.
        let owner = unsafe { CStr::from_ptr(*argv.add(3)) }
            .to_str()
            .map_err(|_| io::ErrorKind::InvalidInput)?
            .parse::<libc::pid_t>()
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        let directory = receive_directory(&channel)?;
        let bytes = read_bytes(&mut channel)?;
        let environment = bytes
            .split_inclusive(|byte| *byte == 0)
            .map(|entry| {
                CString::from_vec_with_nul(entry.to_vec())
                    .map_err(|_| io::ErrorKind::InvalidInput.into())
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut has_cwd = [0];
        channel.read_exact(&mut has_cwd)?;
        let cwd = match has_cwd {
            [0] => None,
            [1] => Some(CString::new(read_bytes(&mut channel)?)
                .map_err(|_| io::ErrorKind::InvalidInput)?),
            _ => return Err(io::ErrorKind::InvalidData.into()),
        };
        // SAFETY: argv[4..] is the NUL-terminated program argument array.
        let exec = unsafe { Exec::prepare(argv.add(4), environment, cwd) }?;
        Ok::<_, io::Error>((owner, directory, exec))
    })();
    let (owner, directory, exec) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => fail(
            channel.as_raw_fd(),
            error.raw_os_error().unwrap_or(libc::EINVAL),
        ),
    };
    match warden::enter(directory.as_raw_fd(), owner) {
        Ok(()) => exec.run(channel.as_raw_fd()),
        Err(error) => fail(
            channel.as_raw_fd(),
            error.raw_os_error().unwrap_or(libc::EIO),
        ),
    }
}

struct Exec {
    paths: Vec<CString>,
    args: *const *const c_char,
    _environment: Vec<CString>,
    env: Vec<*const c_char>,
    shell_args: Vec<*const c_char>,
    cwd: Option<CString>,
}

impl Exec {
    unsafe fn prepare(args: *const *const c_char, environment: Vec<CString>, cwd: Option<CString>) -> io::Result<Self> {
        // SAFETY: serve supplies the live program and environment arrays.
        let program = unsafe { CStr::from_ptr(*args) }.to_bytes();
        if program.is_empty() {
            return Err(io::Error::from_raw_os_error(libc::ENOENT));
        }
        let paths = if program.contains(&b'/') {
            vec![CString::new(program).map_err(|_| io::ErrorKind::InvalidInput)?]
        } else {
            let path = environment
                .iter()
                .find_map(|entry| entry.to_bytes().strip_prefix(b"PATH="))
                .unwrap_or(b"/bin:/usr/bin");
            path.split(|byte| *byte == b':')
                .map(|directory| {
                    let mut name = directory.to_vec();
                    if !name.is_empty() {
                        name.push(b'/');
                    }
                    name.extend_from_slice(program);
                    CString::new(name).map_err(|_| io::ErrorKind::InvalidInput.into())
                })
                .collect::<io::Result<Vec<_>>>()?
        };
        let mut shell_args = vec![c"/bin/sh".as_ptr(), std::ptr::null()];
        // SAFETY: argv[1..] and its final null pointer are live startup storage.
        let mut at = unsafe { args.add(1) };
        loop {
            // SAFETY: stop at the startup array's sentinel.
            let arg = unsafe { *at };
            shell_args.push(arg);
            if arg.is_null() {
                break;
            }
            // SAFETY: the next entry or sentinel.
            at = unsafe { at.add(1) };
        }
        let env = environment
            .iter()
            .map(|entry| entry.as_ptr())
            .chain(std::iter::once(std::ptr::null()))
            .collect();
        Ok(Self {
            paths,
            args,
            env,
            _environment: environment,
            shell_args,
            cwd,
        })
    }

    fn run(mut self, channel: RawFd) -> ! {
        if let Some(cwd) = &self.cwd {
            // SAFETY: only the program changes cwd, after the warden's fork.
            // chdir is async-signal-safe; its CString was prepared before fork.
            if unsafe { libc::chdir(cwd.as_ptr()) } != 0 {
                fail(channel, io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO));
            }
        }
        let mut denied = false;
        for path in &self.paths {
            // SAFETY: all strings and pointer arrays were prepared before fork.
            unsafe { libc::execve(path.as_ptr(), self.args, self.env.as_ptr()) };
            let mut error = io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
            if error == libc::ENOEXEC {
                // Preserve execvp's executable-text fallback, without allocation.
                let Some(script) = self.shell_args.get_mut(1) else {
                    fail(channel, libc::EINVAL);
                };
                *script = path.as_ptr();
                // SAFETY: a prebuilt argv with the selected path filled in.
                unsafe {
                    libc::execve(
                        c"/bin/sh".as_ptr(),
                        self.shell_args.as_ptr(),
                        self.env.as_ptr(),
                    )
                };
                error = io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO);
                fail(channel, error);
            }
            match error {
                libc::EACCES => denied = true,
                libc::ENOENT | libc::ENOTDIR => {}
                _ => fail(channel, error),
            }
        }
        fail(channel, if denied { libc::EACCES } else { libc::ENOENT })
    }
}

/// No allocation, destructors or unwinding after enter's fork, even on error.
fn fail(channel: RawFd, error: c_int) -> ! {
    let bytes = error.to_ne_bytes();
    let mut written = 0;
    while written < bytes.len() {
        let Some(remaining) = bytes.get(written..) else {
            break;
        };
        // SAFETY: a live socket and the unwritten portion of a stack buffer.
        let count = unsafe { libc::write(channel, remaining.as_ptr().cast(), remaining.len()) };
        if count > 0 {
            written += count.unsigned_abs();
        } else if count == 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            break;
        }
    }
    // SAFETY: no Rust cleanup is safe or necessary in the post-fork child.
    unsafe { libc::_exit(127) }
}
