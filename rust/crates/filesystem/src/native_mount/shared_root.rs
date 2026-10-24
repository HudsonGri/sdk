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

use super::{NativeMountError, registry};
use acyclic_native_runtime::{FileLock, FileLockMode};
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
    std::fs::create_dir_all(&roots)?;
    let roots = crate::hold_root(&roots)
        .map_err(crate::NativeTreeMaterializationError::from)?;
    root_in(&roots)
}

/// Uses an already admitted writable namespace for the process's shared
/// instance. Native hosts use this to keep every view inside their existing
/// sandbox roots. Root ownership, crash reaping and instance allocation are
/// identical to [`shared_view_root`]; placement grants no additional access.
pub fn shared_view_root_in(admitted: &crate::HeldRoot) -> Result<PathBuf, NativeMountError> {
    root_in(admitted)
}

fn verify_placement(admitted: &crate::HeldRoot) -> Result<(), NativeMountError> {
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
    Ok(())
}

fn root_in(roots: &crate::HeldRoot) -> Result<PathBuf, NativeMountError> {
    verify_placement(roots)?;
    let key = (roots.path().to_path_buf(), roots.identity().to_bytes());
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
    verify_placement(roots)?;
    let path = root.path.clone();
    index.insert(key, root);
    Ok(path)
}

fn create_root(roots: &crate::HeldRoot, name: &str) -> Result<Root, NativeMountError> {
    crate::native_host::require_leaf(Path::new(name))?;
    let reaping = FileLock::acquire(
        roots.open_file(Path::new(REAP_LOCK), true)
            .map_err(crate::NativeTreeMaterializationError::from)?,
        FileLockMode::Exclusive,
    )?;
    reap_dead_roots(roots)?;
    let owner = FileLock::try_acquire(
        roots.open_file(Path::new(&format!("{name}.{OWNER}")), true)
            .map_err(crate::NativeTreeMaterializationError::from)?,
        FileLockMode::Exclusive,
    )?;
    roots.root.directory.create_dir(name)?;
    let path = roots.path().join(name);
    drop(reaping);
    Ok(Root {
        path,
        _owner: owner,
    })
}

/// Detaches and removes every root under `roots` whose owner is gone; the
/// caller holds the directory's reap lock. Returns how many were removed.
fn reap_dead_roots(roots: &crate::HeldRoot) -> Result<usize, NativeMountError> {
    let entries = roots.root.directory.entries()?;
    let suffix = format!(".{OWNER}");
    let mut reaped = 0;
    for entry in entries {
        let file = entry?.file_name();
        let Some(name) = file
            .to_str()
            .and_then(|file| file.strip_suffix(suffix.as_str()))
        else {
            continue;
        };
        let owner = roots.open_file(Path::new(&file), false)
            .map_err(crate::NativeTreeMaterializationError::from)?;
        let _owner = match FileLock::try_acquire(owner, FileLockMode::Exclusive) {
            Ok(owner) => owner,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error.into()),
        };
        reap_root(roots, Path::new(name))?;
        roots.remove_file(Path::new(&file))
            .map_err(crate::NativeTreeMaterializationError::from)?;
        reaped += 1;
    }
    Ok(reaped)
}

fn reap_root(roots: &crate::HeldRoot, name: &Path) -> Result<(), NativeMountError> {
    // The shared backend currently exists on Linux. Its common pinned
    // recovery operation cannot detach an alias or a replacement mount.
    #[cfg(target_os = "linux")]
    super::recover_disconnected_native_mount_destination_in(
        roots,
        name.as_os_str(),
    )?;
    match roots.root.directory.remove_dir(name) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
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
    fn only_roots_without_a_live_owner_are_reaped() -> Result<(), Box<dyn std::error::Error>> {
        let roots = tempfile::tempdir()?;
        let admission = crate::hold_root(roots.path())?;
        let live = create_root(&admission, "live")?;
        let dead = create_root(&admission, "dead")?;
        std::fs::write(roots.path().join("stray"), b"")?;
        let held = FileLock::acquire(
            admission.open_file(Path::new(REAP_LOCK), true)?,
            FileLockMode::Exclusive,
        )?;
        drop(dead);
        assert_eq!(reap_dead_roots(&admission)?, 1);
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
        let next = create_root(&admission, "next")?;
        assert!(!roots.path().join("live").exists());
        assert!(next.path.is_dir());
        Ok(())
    }

    #[test]
    fn root_setup_and_reaping_never_reopen_the_admitted_name()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = tempfile::tempdir()?;
        let path = fixture.path().join("admitted");
        let moved = fixture.path().join("held");
        let outside = fixture.path().join("outside");
        std::fs::create_dir(&path)?;
        std::fs::create_dir(&outside)?;
        let admission = crate::hold_root(&path)?;
        drop(create_root(&admission, "dead")?);
        std::fs::rename(&path, &moved)?;
        std::os::unix::fs::symlink(&outside, &path)?;
        // Replacement after admission cannot redirect creation, owner locks,
        // enumeration or removal. The public boundary also refuses its cwd.
        let next = create_root(&admission, "next")?;
        assert!(moved.join("next").is_dir());
        assert!(!moved.join("dead").exists());
        assert!(!moved.join("dead.owner").exists());
        assert_eq!(std::fs::read_dir(&outside)?.count(), 0);
        assert!(shared_view_root_in(&admission).is_err());
        // Negative control: the former path-based creation writes outside.
        std::fs::create_dir(path.join("path-based-control"))?;
        assert!(outside.join("path-based-control").is_dir());
        drop(next);
        Ok(())
    }

    #[test]
    fn failed_reap_refuses_a_new_root_without_removing_its_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = tempfile::tempdir()?;
        let admission = crate::hold_root(fixture.path())?;
        let dead = create_root(&admission, "dead")?;
        std::fs::write(fixture.path().join("dead/authored"), b"retained")?;
        drop(dead);
        assert!(create_root(&admission, "next").is_err());
        assert!(!fixture.path().join("next").exists());
        assert!(fixture.path().join("dead.owner").exists());
        assert_eq!(std::fs::read(fixture.path().join("dead/authored"))?, b"retained");
        Ok(())
    }
}
