//! The process's root for views of its shared native mount instance.
//!
//! Each process gets a fresh root in the default native-mount directory or
//! an admitted host workspace. One instance serves all views in that admission,
//! however many owners (harnesses, sessions) open them. A process holds its root's owner lock
//! for its lifetime; a process that dies releases it with its descriptors,
//! leaving its root mounted with a fence nobody holds. The first process to
//! ask for its own root afterwards detaches and removes every root whose
//! owner lock it can take, so a crash never leaves a mount behind past the
//! next start. One directory lock orders creation against reaping, so a
//! root is never reaped between its creation and its owner's lock.

use super::{NativeMountError, detach_native_mount_destination_after_crash, registry};
use acyclic_native_runtime::{FileLock, FileLockMode};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

const ROOTS: &str = "views";
const OWNER: &str = "owner";
const REAP_LOCK: &str = ".reap.lock";

struct Root {
    path: PathBuf,
    // Held for the process's lifetime: the root's liveness.
    _owner: FileLock,
}

type Admission = (PathBuf, [u8; 16]);
type Roots = std::collections::BTreeMap<Admission, Root>;
static ROOTS_BY_ADMISSION: OnceLock<Mutex<Roots>> = OnceLock::new();

/// This process's root for shared views: a view at `root.join(name)`,
/// mounted with [`MountOptions::shared`](crate::MountOptions::shared), is a
/// namespace of the one instance mounted at the root.
///
/// The first call reaps the roots of dead processes.
pub fn shared_view_root() -> Result<PathBuf, NativeMountError> {
    let roots = registry::path()?
        .parent()
        .ok_or_else(|| std::io::Error::other("native-mount directory has no parent"))?
        .join(ROOTS);
    root_in(&roots)
}

/// Uses an already admitted writable namespace for the process's shared
/// instance. Native hosts use this to keep every view inside their existing
/// sandbox roots. Root ownership, crash reaping and instance allocation are
/// identical to [`shared_view_root`]; placement grants no additional access.
pub fn shared_view_root_in(admitted: &crate::HeldRoot) -> Result<PathBuf, NativeMountError> {
    if crate::capture_root_identity(admitted.path())
        .map_err(|error| std::io::Error::other(error.to_string()))?
        != admitted.identity()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "shared-view admission no longer names the held root",
        )
        .into());
    }
    root_in(admitted.path())
}

fn root_in(roots: &Path) -> Result<PathBuf, NativeMountError> {
    std::fs::create_dir_all(roots)?;
    let identity = crate::capture_root_identity(roots)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let key = (roots.to_path_buf(), identity.to_bytes());
    let mut index = ROOTS_BY_ADMISSION
        .get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .map_err(|_| std::io::Error::other("shared mount root index poisoned"))?;
    if let Some(root) = index.get(&key) {
        return Ok(root.path.clone());
    }
    let root = create_root(
        roots,
        &format!(
            "{:032x}",
            u128::from_be_bytes(crate::MountId::new().into_bytes())
        ),
    )?;
    let path = root.path.clone();
    index.insert(key, root);
    Ok(path)
}

fn create_root(roots: &Path, name: &str) -> std::io::Result<Root> {
    std::fs::create_dir_all(roots)?;
    let reaping = FileLock::acquire(File::create(roots.join(REAP_LOCK))?, FileLockMode::Exclusive)?;
    reap_dead_roots(roots);
    let owner = FileLock::try_acquire(
        File::create(roots.join(format!("{name}.{OWNER}")))?,
        FileLockMode::Exclusive,
    )?;
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
        let Ok(_owner) = FileLock::try_acquire(owner, FileLockMode::Exclusive) else {
            // Held: a live process owns this root.
            continue;
        };
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

fn reap_root(root: &Path) -> bool {
    match std::fs::symlink_metadata(root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        _ => {}
    }
    let Ok([fence, ..]) = super::native_mount_control_paths(root) else {
        return false;
    };
    if fence.exists()
        && let Err(error) = detach_native_mount_destination_after_crash(root)
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

    #[test]
    fn admitted_shared_root_is_reused_and_replaced_admission_is_refused() -> std::io::Result<()> {
        let fixture = tempfile::tempdir()?;
        let path = fixture.path().join("admitted");
        std::fs::create_dir(&path)?;
        let admitted = crate::hold_root(&path).map_err(std::io::Error::other)?;
        let first = shared_view_root_in(&admitted).map_err(std::io::Error::other)?;
        assert!(first.starts_with(&path));
        assert_eq!(
            first,
            shared_view_root_in(&admitted).map_err(std::io::Error::other)?
        );
        let replacement = fixture.path().join("replacement");
        std::fs::create_dir(&replacement)?;
        std::fs::rename(&path, fixture.path().join("held"))?;
        std::fs::rename(&replacement, &path)?;
        // The same path, now naming another directory, grants no mount root.
        assert!(shared_view_root_in(&admitted).is_err());
        assert!(std::fs::read_dir(&path)?.next().is_none());
        let replacement = crate::hold_root(&path).map_err(std::io::Error::other)?;
        let next = shared_view_root_in(&replacement).map_err(std::io::Error::other)?;
        assert_ne!(
            first, next,
            "a new admitted identity cannot adopt the old cached root"
        );
        assert!(next.is_dir());
        Ok(())
    }

    /// A root whose owner lock is held is never reaped; once released it is.
    #[test]
    fn only_roots_without_a_live_owner_are_reaped() -> std::io::Result<()> {
        let roots = tempfile::tempdir()?;
        let live = create_root(roots.path(), "live")?;
        let dead = create_root(roots.path(), "dead")?;
        std::fs::write(roots.path().join("stray"), b"")?;
        let held = FileLock::acquire(
            File::open(roots.path().join(REAP_LOCK))?,
            FileLockMode::Exclusive,
        )?;
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
