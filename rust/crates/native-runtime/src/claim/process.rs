//! This process's one process lock, and the records that name it.

use super::*;

const RECORD_HEADER: &str = "acyclic-claim-v1";
/// A record is a few short lines; anything longer is not one.
const RECORD_LIMIT: u64 = 4096;

/// The lock this process holds for its whole life. A claim record names it, so
/// the record lapses when the process dies.
#[derive(Debug)]
pub(super) struct ProcessOwner {
    pid: u32,
    start: String,
    path: String,
    _lock: File,
}

impl ProcessOwner {
    /// This process's owner, created once. `None` when no process lock could be
    /// established, in which case each claim holds its own file lock instead.
    pub(super) fn current() -> Option<&'static Self> {
        static OWNER: OnceLock<Option<ProcessOwner>> = OnceLock::new();
        OWNER.get_or_init(|| Self::create().ok()).as_ref()
    }

    fn create() -> io::Result<Self> {
        let directory = owner_directory()?;
        sweep(&directory);
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let path = directory.join(format!("{pid}-{nanos}.lock"));
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(windows)]
        {
            // No FILE_SHARE_DELETE: the name stays while this process lives, so
            // a missing lock file proves its owner died.
            use std::os::windows::fs::OpenOptionsExt as _;
            options.share_mode(0x1 | 0x2);
        }
        let lock = options.open(&path)?;
        lock_briefly(&lock)?;
        Ok(Self {
            pid,
            start: start_identity(pid).unwrap_or_default(),
            path: path
                .into_os_string()
                .into_string()
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidFilename))?,
            _lock: lock,
        })
    }

    pub(super) fn record(&self) -> Vec<u8> {
        format!(
            "{RECORD_HEADER}\n{}\n{}\n{}\n",
            self.pid, self.start, self.path
        )
        .into_bytes()
    }

    /// Whether `record` names this process.
    pub(super) fn wrote(&self, record: &Record) -> bool {
        record.pid == self.pid && record.path == self.path
    }
}

/// The owner a claim file names.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct Record {
    pid: u32,
    start: String,
    path: String,
}

impl Record {
    /// The claim file's record. `None` when it names no owner: empty, or not a
    /// whole record, as after a crash that tore its write.
    pub(super) fn read(file: &mut File) -> io::Result<Option<Self>> {
        file.seek(SeekFrom::Start(0))?;
        let mut text = String::new();
        if (&*file)
            .take(RECORD_LIMIT)
            .read_to_string(&mut text)
            .is_err()
        {
            return Ok(None);
        }
        Ok(Self::parse(&text))
    }

    fn parse(text: &str) -> Option<Self> {
        let mut lines = text.strip_suffix('\n')?.split('\n');
        if lines.next()? != RECORD_HEADER {
            return None;
        }
        let pid = lines.next()?.parse().ok()?;
        let start = lines.next()?.to_owned();
        let path = lines.next()?.to_owned();
        (lines.next().is_none() && !path.is_empty()).then_some(Self { pid, start, path })
    }

    /// Whether the process this record names still lives. Unknown answers
    /// count as alive: a false "alive" refuses an open, a false "dead" would
    /// admit two owners.
    pub(super) fn is_alive(&self) -> bool {
        match File::open(&self.path) {
            Ok(lock) => !matches!(lock.try_lock_shared(), Ok(())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => self.started_process_lives(),
            Err(_) => true,
        }
    }

    /// For a process lock that is gone: whether the process that held it lives.
    /// Something other than its owner can remove it, such as a temporary-file
    /// cleaner, so a missing lock alone proves nothing on Unix.
    fn started_process_lives(&self) -> bool {
        #[cfg(target_os = "linux")]
        return match start_identity(self.pid) {
            Some(start) => self.start.is_empty() || start == self.start,
            None => false,
        };
        #[cfg(target_vendor = "apple")]
        return match rustix::process::Pid::from_raw(i32::try_from(self.pid).unwrap_or(0)) {
            Some(pid) => !matches!(
                rustix::process::test_kill_process(pid),
                Err(rustix::io::Errno::SRCH)
            ),
            None => false,
        };
        // The owner's lock file denies deletion while it is open, and only a
        // sweep that took its lock removes it.
        #[cfg(windows)]
        return false;
        #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
        true
    }
}

/// Where this user's process locks live: private to the user, and outside any
/// store so that every process finds the lock a record names.
fn owner_directory() -> io::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .unwrap_or_else(std::env::temp_dir);
    #[cfg(not(target_os = "linux"))]
    let base = std::env::temp_dir();
    #[cfg(unix)]
    let directory = base.join(format!("acyclic-owners-{}", user_id()?));
    #[cfg(not(unix))]
    let directory = base.join("acyclic-owners");
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
        builder.mode(0o700);
        builder.create(&directory)?;
        // A shared temporary directory lets another user create this name
        // first; only a private directory of this user's is trusted.
        let metadata = std::fs::symlink_metadata(&directory)?;
        if !metadata.is_dir() || metadata.uid() != user_id()? || metadata.mode() & 0o077 != 0 {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
    }
    #[cfg(not(unix))]
    builder.create(&directory)?;
    Ok(directory)
}

#[cfg(unix)]
fn user_id() -> io::Result<u32> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok(std::fs::metadata("/proc/self")?.uid())
    }
    #[cfg(target_vendor = "apple")]
    return Ok(rustix::process::getuid().as_raw());
    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

/// Removes the process locks of processes that have died. A lock is removed
/// only while held here, so a removed lock always belonged to a dead process.
fn sweep(directory: &Path) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "lock") {
            continue;
        }
        if let Ok(lock) = OpenOptions::new().read(true).write(true).open(&path)
            && lock.try_lock().is_ok()
        {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// A Linux process's identity across PID reuse: the boot and its start tick.
#[cfg(target_os = "linux")]
fn start_identity(pid: u32) -> Option<String> {
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Field 22, counted after the parenthesized command name, which may
    // itself contain spaces.
    let (_, fields) = stat.rsplit_once(')')?;
    let start = fields.split_whitespace().nth(19)?;
    Some(format!("{}:{start}", boot.trim()))
}

#[cfg(not(target_os = "linux"))]
fn start_identity(_: u32) -> Option<String> {
    None
}
