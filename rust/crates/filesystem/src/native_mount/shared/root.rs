//! The process's root for views of its shared native mount instance.
//!
//! Every process gets a fresh root under the user's native-mount directory,
//! so one instance serves all of the process's views however many owners
//! (harnesses, sessions) open them. A process holds its root's owner lock
//! for its lifetime; a process that dies releases it with its descriptors,
//! leaving its root mounted with a fence nobody holds. The first process to
//! ask for its own root afterwards detaches and removes every root whose
//! owner lock it can take, so a crash never leaves a mount behind past the
//! next start. One directory lock orders creation against reaping, so a
//! root is never reaped between its creation and its owner's lock.
//!
//! The owner lock belongs to its open file description, which a forked
//! child shares until it executes (descriptors are close-on-exec). A root
//! therefore reads live while its owner, or a child it is starting, can
//! still reach the lock: liveness errs only toward keeping a root, which
//! the next start reaps.

use super::super::{NativeMountError, registry};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const ROOTS: &str = "views";
const OWNER: &str = "owner";
const REAP_LOCK: &str = ".reap.lock";

struct Root {
    path: PathBuf,
    // Held for the process's lifetime: the root's liveness.
    _owner: File,
}

static ROOT: OnceLock<Result<Root, std::sync::Arc<std::io::Error>>> = OnceLock::new();

/// This process's root for shared views: every view
/// [`Workspace::mount_view`](crate::Workspace::mount_view) mounts is a
/// namespace `root.join(name)` of the one instance mounted at the root.
///
/// The first call reaps the roots of dead processes.
pub fn shared_view_root() -> Result<PathBuf, NativeMountError> {
    let root = ROOT.get_or_init(|| {
        let roots = registry::path()
            .map_err(|error| std::io::Error::other(error.to_string()))?
            .parent()
            .ok_or_else(|| std::io::Error::other("native-mount directory has no parent"))?
            .join(ROOTS);
        create_root(
            &roots,
            &format!(
                "{:032x}",
                u128::from_be_bytes(crate::MountId::new().into_bytes())
            ),
        )
        .map_err(std::sync::Arc::new)
    });
    match root {
        Ok(root) => Ok(root.path.clone()),
        Err(error) => Err(NativeMountError::Io(std::io::Error::new(
            error.kind(),
            error.to_string(),
        ))),
    }
}

/// The last serial [`shared_view_destination`] issued.
static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Names issued and not yet listed, kept only where a view's listing
/// claims its name (Windows). A name leaves the set when its view is
/// listed, so no name is ever listed twice and the set holds only names in
/// flight; elsewhere nothing would ever take a name out.
#[cfg(windows)]
static UNLISTED: std::sync::Mutex<Option<std::collections::HashSet<u64>>> =
    std::sync::Mutex::new(None);

const ISSUED_PREFIX: &str = "v";

/// A fresh destination for a shared view: a name in this process's root
/// that no view of the process has had or will have. A Windows view must
/// be mounted at one: ProjFS names a view's entries by path alone, so a
/// late callback from a handle opened in a retired view would reach any
/// later view of the same name.
pub fn shared_view_destination() -> Result<PathBuf, NativeMountError> {
    let root = shared_view_root()?;
    #[cfg(windows)]
    let mut unlisted = UNLISTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let serial = SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    #[cfg(windows)]
    unlisted.get_or_insert_default().insert(serial);
    Ok(root.join(format!("{ISSUED_PREFIX}{serial:016x}")))
}

/// Claims an issued, never listed name for its view; false for any other.
#[cfg(windows)]
pub(in crate::native_mount) fn claim_issued_name(name: &std::ffi::OsStr) -> bool {
    let Some(serial) = name
        .to_str()
        .and_then(|name| name.strip_prefix(ISSUED_PREFIX))
        .filter(|digits| digits.len() == 16)
        .and_then(|digits| u64::from_str_radix(digits, 16).ok())
    else {
        return false;
    };
    UNLISTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_mut()
        .is_some_and(|unlisted| unlisted.remove(&serial))
}

fn create_root(roots: &Path, name: &str) -> std::io::Result<Root> {
    std::fs::create_dir_all(roots)?;
    let reaping = File::create(roots.join(REAP_LOCK))?;
    reaping.lock()?;
    reap_dead_roots(roots);
    let owner = File::create(roots.join(format!("{name}.{OWNER}")))?;
    owner.try_lock().map_err(std::io::Error::from)?;
    let path = roots.join(name);
    std::fs::create_dir(&path)?;
    drop(reaping);
    Ok(Root {
        path,
        _owner: owner,
    })
}

/// Detaches and removes every root under `roots` whose owner is gone; the
/// caller holds the directory's reap lock. Returns how many were removed.
fn reap_dead_roots(roots: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(roots) else {
        return 0;
    };
    let suffix = format!(".{OWNER}");
    let mut reaped = 0;
    for entry in entries.flatten() {
        let file = entry.file_name();
        let Some(name) = file
            .to_str()
            .and_then(|file| file.strip_suffix(suffix.as_str()))
        else {
            continue;
        };
        let owner_path = entry.path();
        let Ok(owner) = File::options().write(true).open(&owner_path) else {
            continue;
        };
        if owner.try_lock().is_err() {
            // Held: a live process owns this root.
            continue;
        }
        if reap_root(&roots.join(name)) {
            // Only a fully removed root gives up its owner file, so a failed
            // reap is retried by the next start.
            if std::fs::remove_file(&owner_path).is_ok() {
                reaped += 1;
            }
        }
    }
    reaped
}

/// Detaches and removes a dead process's root; false leaves it for the
/// next start. Detaching is the adapter's: only it knows how its OS binds a
/// root.
fn reap_root(root: &Path) -> bool {
    match std::fs::symlink_metadata(root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        _ => detach_dead_root(root),
    }
}

#[cfg(windows)]
use super::super::projfs::detach_dead_shared_root as detach_dead_root;

/// A FUSE or NFS root is a mount fenced like any native mount: a fence
/// left by the dead owner names the mount to detach.
#[cfg(not(windows))]
fn detach_dead_root(root: &Path) -> bool {
    let [fence, ..] = match super::super::native_mount_control_paths(root) {
        Ok(paths) => paths,
        Err(_) => return false,
    };
    if fence.exists()
        && let Err(error) = super::super::detach_native_mount_destination_after_crash(root)
    {
        tracing::warn!(%error, root = %root.display(), "a dead shared root was not detached");
        return false;
    }
    match std::fs::remove_dir(root) {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(%error, root = %root.display(), "a dead shared root was not removed");
            false
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(super) mod tests {
    use super::*;

    /// Set to run [`only_roots_without_a_live_owner_are_reaped`]'s probe in
    /// a fresh process.
    const REAP_PROBE: &str = "ACYCLIC_SHARED_ROOT_REAP_PROBE";

    /// A root whose owner lock is held is never reaped; once released it is.
    ///
    /// The probe runs in a process of its own: in the shared test process
    /// another test's fork can share a dropped owner's lock until its child
    /// executes, which keeps that root live, as the module describes.
    #[test]
    fn only_roots_without_a_live_owner_are_reaped() -> std::io::Result<()> {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "native_mount::shared::root::tests::reap_probe",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(REAP_PROBE, "1")
            .output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    #[test]
    fn reap_probe() -> std::io::Result<()> {
        if std::env::var_os(REAP_PROBE).is_none() {
            return Ok(());
        }
        let roots = tempfile::tempdir()?;
        let live = create_root(roots.path(), "live")?;
        let dead = create_root(roots.path(), "dead")?;
        std::fs::write(roots.path().join("stray"), b"")?;
        let held = File::open(roots.path().join(REAP_LOCK))?;
        held.lock()?;
        drop(dead);
        assert_eq!(reap_dead_roots(roots.path()), 1);
        drop(held);
        assert!(live.path.is_dir(), "a live owner keeps its root");
        assert!(!roots.path().join("dead").exists());
        assert!(!roots.path().join("dead.owner").exists());
        assert!(
            roots.path().join("stray").exists(),
            "only owned roots are touched"
        );
        // Negative control: the same root, its owner lock dropped, is reaped.
        drop(live);
        let next = create_root(roots.path(), "next")?;
        assert!(!roots.path().join("live").exists());
        assert!(next.path.is_dir());
        Ok(())
    }
}

#[cfg(all(test, windows))]
mod issued_name_tests {
    use super::*;

    /// An issued name is claimed once; a reused, forged or foreign name is
    /// never claimed, so no two shared ProjFS views ever share a name.
    #[test]
    fn issued_names_are_claimed_exactly_once() -> Result<(), NativeMountError> {
        let destination = shared_view_destination()?;
        let name = destination.file_name().unwrap_or_default();
        assert!(claim_issued_name(name));
        assert!(!claim_issued_name(name), "a listed name was claimed again");
        let unissued = format!("{ISSUED_PREFIX}{:016x}", u64::MAX);
        for forged in [unissued.as_str(), "v1", "task", ""] {
            assert!(
                !claim_issued_name(std::ffi::OsStr::new(forged)),
                "{forged:?}"
            );
        }
        Ok(())
    }
}
