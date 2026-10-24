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

use super::{NativeMountError, detach_native_mount_destination_after_crash, registry};
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

/// This process's root for shared views: a view at `root.join(name)`,
/// mounted with [`MountOptions::shared`](crate::MountOptions::shared), is a
/// namespace of the one instance mounted at the root.
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

fn reap_root(root: &Path) -> bool {
    match std::fs::symlink_metadata(root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        _ => {}
    }
    let [fence, ..] = match super::native_mount_control_paths(root) {
        Ok(paths) => paths,
        Err(_) => return false,
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

    /// A root whose owner lock is held is never reaped; once released it is.
    #[test]
    fn only_roots_without_a_live_owner_are_reaped() -> std::io::Result<()> {
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
