//! Immutable bytes and permissions only; provider/host caches are never copied.
use std::{
    io,
    path::{Path, PathBuf},
};

#[cfg(test)]
mod tests;
#[cfg(all(test, windows))]
mod windows_tests;

#[cfg(all(test, windows))]
#[derive(Debug, PartialEq)]
struct PermissionWrite;

/// Closed fixture bytes and permissions, independent of live provider state.
/// Symlinks and restoring over an existing root are rejected.
#[derive(Debug, PartialEq)]
pub struct Snapshot(Vec<(PathBuf, Option<Vec<u8>>, std::fs::Permissions)>);

impl Snapshot {
    /// Captures a quiescent owned fixture after every writer has closed.
    pub fn capture(root: &Path) -> io::Result<Self> {
        let mut pending = vec![(PathBuf::new(), std::fs::symlink_metadata(root)?)];
        let mut entries = Vec::new();
        while let Some((relative, metadata)) = pending.pop() {
            let path = root.join(&relative);
            if metadata.is_symlink() || !(metadata.is_dir() || metadata.is_file()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fixture snapshot accepts only regular files and directories",
                ));
            }
            let bytes = if metadata.is_dir() {
                let mut children = std::fs::read_dir(&path)?
                    .map(|entry| {
                        let entry = entry?;
                        // Enumeration already supplies the no-follow metadata on
                        // Windows. These independent fixtures have no concurrent writer.
                        Ok((relative.join(entry.file_name()), entry.metadata()?))
                    })
                    .collect::<std::io::Result<Vec<_>>>()?;
                children.sort_by(|(left, _), (right, _)| left.cmp(right));
                pending.extend(children.into_iter().rev());
                None
            } else {
                Some(std::fs::read(path)?)
            };
            entries.push((relative, bytes, metadata.permissions()));
        }
        Ok(Self(entries))
    }

    /// Creates an independent physical tree and verifies its complete image.
    pub fn restore(&self, root: &Path) -> io::Result<()> {
        assert!(!root.exists(), "snapshot destination must be independent");
        for (relative, bytes, _) in &self.0 {
            let path = root.join(relative);
            match bytes {
                Some(bytes) => std::fs::write(&path, bytes)?,
                None => std::fs::create_dir(&path)?,
            }
        }
        // Install permissions after descendants exist, so a readonly source
        // directory cannot prevent constructing its independent copy.
        for (relative, _, permissions) in self.0.iter().rev() {
            let path = root.join(relative);
            // Windows creates these independent entries writable. Only the
            // readonly bit needs restoring; avoid opening every writable entry
            // again just to set the attribute it already has.
            #[cfg(windows)]
            if !permissions.readonly() {
                continue;
            }
            #[cfg(all(test, windows))]
            crate::differential::record(PermissionWrite);
            std::fs::set_permissions(path, permissions.clone())?;
        }
        assert_eq!(&Self::capture(root)?, self, "snapshot bytes changed");
        Ok(())
    }
}
