//! Every immutable cut retains the same directory owner, including an external
//! root gate. A path alone can never release the gate while a reader survives.
use super::*;
use acyclic_native_runtime::{FileLock, OwnershipAnchor};
use std::path::{Path, PathBuf};

pub(super) struct Directory {
    path: PathBuf,
    // Files and their ownership gates are destroyed before the external root.
    _ownership: FileLock,
    _anchor: Option<OwnershipAnchor>,
}

impl Directory {
    pub(super) fn new(
        path: PathBuf,
        ownership: FileLock,
        anchor: Option<OwnershipAnchor>,
    ) -> Arc<Self> {
        Arc::new(Self {
            path,
            _ownership: ownership,
            _anchor: anchor,
        })
    }
}

impl std::ops::Deref for Directory {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::{LocalDurability, LocalStreamLimits};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct External(Arc<AtomicBool>);
    impl Drop for External {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_shared_directory_retains_both_root_gates_until_its_final_read_cut()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let limits = LocalStreamLimits {
            durability: LocalDurability::FullFlush,
            ..Default::default()
        };
        let released = Arc::new(AtomicBool::new(false));
        let anchor = OwnershipAnchor::new(External(Arc::clone(&released)));
        let ownership = crate::local::lock_root(temporary.path(), limits)?;
        let directory = Directory::new(temporary.path().to_path_buf(), ownership, Some(anchor));
        let cut = Arc::clone(&directory);
        drop(directory);
        assert!(
            matches!(
                crate::local::lock_root(temporary.path(), limits),
                Err(LocalStreamError::AlreadyOpen)
            ),
            "captured directory released its exclusive root gate"
        );
        assert!(
            !released.load(Ordering::SeqCst),
            "captured directory released its external root gate"
        );
        drop(cut);
        assert!(released.load(Ordering::SeqCst));
        let ownership = crate::local::lock_root(temporary.path(), limits)?;
        drop(ownership);
        Ok(())
    }
}
