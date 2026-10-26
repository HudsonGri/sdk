//! Provider-independent workspace transfer commands.

use crate::ProviderError;
use async_trait::async_trait;

/// Transport operations needed by the shared workspace transfer mechanism.
#[async_trait]
pub trait WorkspaceIo: Send + Sync {
    /// Runs one POSIX command and returns its terminal status.
    async fn status(&self, resource: &str, command: String) -> Result<Option<i64>, ProviderError>;
    /// Downloads one file without text conversion.
    async fn download(&self, resource: &str, path: &str) -> Result<Vec<u8>, ProviderError>;
    /// Uploads one file without text conversion.
    async fn upload(&self, resource: &str, path: &str, data: &[u8]) -> Result<(), ProviderError>;
}

/// A quiescent copy of the provider-declared persistent workspace.
#[derive(Clone)]
pub struct WorkspaceArchive {
    path: String,
    data: Vec<u8>,
}

impl WorkspaceArchive {
    /// Captures a workspace once, before any destination resources exist.
    /// The temporary source archive is removed even when its download fails.
    ///
    /// # Errors
    /// Returns `Conflict` when the workspace keeps changing, or a transport failure.
    pub async fn capture(
        io: &impl WorkspaceIo,
        resource: &str,
        workspace: &str,
        tar_path: &str,
        attempts: u32,
    ) -> Result<Self, ProviderError> {
        validate_workspace(workspace)?;
        if attempts == 0 || !tar_path.starts_with("/tmp/") || tar_path.contains("..") {
            return Err(ProviderError::Invalid("workspace capture admission failed; recovery: use a unique archive under /tmp and at least one attempt".into()));
        }
        match io
            .status(
                resource,
                quiescent_archive_script(workspace, tar_path, attempts),
            )
            .await?
        {
            Some(0) => {}
            Some(NOT_QUIESCENT) => {
                return Err(ProviderError::Conflict(format!(
                    "workspace of {resource} changed across {attempts} archive attempts; recovery: quiesce the workload and retry the fork"
                )));
            }
            status => return Err(transfer_failure("capture", resource, status)),
        }
        let path = format!("{tar_path}.gz");
        let data = io.download(resource, &path).await;
        // Attempt cleanup even after a failed download, retaining its original error.
        let cleanup = io
            .status(resource, format!("rm -f -- {}", shell_quote(&path)))
            .await;
        let data = data?;
        match cleanup? {
            Some(0) => Ok(Self { path, data }),
            status => Err(transfer_failure("capture cleanup", resource, status)),
        }
    }

    /// Installs the same bytes into a ready destination. Callers publish the
    /// destination's ready marker only after this method succeeds.
    ///
    /// # Errors
    /// Returns upload failures or `Failed` for a failed extraction.
    pub async fn restore(
        &self,
        io: &impl WorkspaceIo,
        resource: &str,
        workspace: &str,
    ) -> Result<(), ProviderError> {
        validate_workspace(workspace)?;
        io.upload(resource, &self.path, &self.data).await?;
        match io
            .status(resource, restore_script(workspace, &self.path))
            .await?
        {
            Some(0) => Ok(()),
            status => Err(transfer_failure("restore", resource, status)),
        }
    }
}

/// Validates a portable, independently replaceable workspace directory.
pub fn validate_workspace(workspace: &str) -> Result<(), ProviderError> {
    if !workspace.starts_with('/')
        || workspace == "/"
        || workspace.contains('\0')
        || workspace
            .split('/')
            .skip(1)
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(ProviderError::Invalid("workspace admission failed; recovery: configure a normalized absolute directory below /".into()));
    }
    Ok(())
}

fn transfer_failure(step: &str, resource: &str, status: Option<i64>) -> ProviderError {
    ProviderError::Rejected(format!(
        "workspace {step} failed on {resource} (exit {status:?}); recovery: inspect the machine's workspace and retry with a new idempotency key"
    ))
}

fn restore_script(workspace: &str, path: &str) -> String {
    let workspace = shell_quote(workspace);
    let archive = shell_quote(path);
    // Replace directory contents, including dotfiles: overlay extraction would retain
    // files deleted by the source workload. Refuse a symlink at the workspace root.
    format!(
        "test ! -L {workspace} && mkdir -p {workspace} && find {workspace} -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} + && tar -C {workspace} -xzf {archive} && rm -f -- {archive}"
    )
}

/// Exit status of [`quiescent_archive_script`] when the workspace never held still.
pub const NOT_QUIESCENT: i64 = 75;

/// POSIX `sh` script that archives `workspace` into `tar_path.gz` only at a quiescent moment.
///
/// `tar` reads files one after another, so a workspace written during the read yields files
/// from different moments. The script archives, then re-reads the tree the same way (both
/// reads stream to stdout, so `tar` pads them identically) and compares checksums. It keeps
/// the archive only when both reads agree and neither `tar` saw a file change under it (a
/// failed re-read appends a marker to its stream, so its checksum can never match), which
/// means nothing changed across the whole read. It retries up to `attempts` times, one second
/// apart, and exits [`NOT_QUIESCENT`] otherwise.
pub fn quiescent_archive_script(workspace: &str, tar_path: &str, attempts: u32) -> String {
    let (workspace, tar) = (shell_quote(workspace), shell_quote(tar_path));
    format!(
        "mkdir -p {workspace} || exit 1; i=0; \
         while :; do \
           if tar -C {workspace} -cf - . > {tar}; then \
             a=$(cksum < {tar}) && b=$({{ tar -C {workspace} -cf - . || echo changed; }} | cksum) && [ \"$a\" = \"$b\" ] && break; \
           else r=$?; [ $r -eq 1 ] || {{ rm -f {tar}; exit $r; }}; fi; \
           i=$((i+1)); if [ $i -ge {attempts} ]; then rm -f {tar}; exit {NOT_QUIESCENT}; fi; \
           sleep 1; \
         done; \
         gzip -n -f {tar}"
    )
}

/// POSIX single-quoted shell word.
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{path::PathBuf, time::Duration};

    struct LocalIo;
    #[async_trait]
    impl WorkspaceIo for LocalIo {
        async fn status(&self, _: &str, command: String) -> Result<Option<i64>, ProviderError> {
            acyclic_native_runtime::process_status(
                std::process::Command::new("sh").args(["-c", &command]),
                Duration::from_secs(10),
            )
            .map(|status| status.code().map(i64::from))
            .map_err(|_| ProviderError::Unavailable)
        }
        async fn download(&self, _: &str, path: &str) -> Result<Vec<u8>, ProviderError> {
            std::fs::read(path).map_err(|_| ProviderError::Unavailable)
        }
        async fn upload(&self, _: &str, path: &str, data: &[u8]) -> Result<(), ProviderError> {
            std::fs::write(path, data).map_err(|_| ProviderError::Unavailable)
        }
    }

    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn workspace_restore_replaces_contents_and_preserves_binary_and_dotfiles()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = Directory(PathBuf::from(format!(
            "/tmp/acyclic-transfer-{}",
            uuid::Uuid::new_v4()
        )));
        let (source, destination) = (dir.0.join("source"), dir.0.join("destination"));
        std::fs::create_dir_all(&source)?;
        std::fs::create_dir(&destination)?;
        std::fs::write(source.join("binary"), [0, 255, 1, 0])?;
        std::fs::write(source.join(".kept"), b"kept")?;
        std::fs::write(destination.join("deleted"), b"stale")?;
        std::fs::write(destination.join(".deleted"), b"stale")?;
        let archive = dir.0.join("archive.tar");
        let captured = WorkspaceArchive::capture(
            &LocalIo,
            "local",
            source.to_str().ok_or("source path is not UTF-8")?,
            archive.to_str().ok_or("archive path is not UTF-8")?,
            1,
        )
        .await?;
        assert!(!archive.with_extension("tar.gz").exists());
        captured
            .restore(
                &LocalIo,
                "local",
                destination
                    .to_str()
                    .ok_or("destination path is not UTF-8")?,
            )
            .await?;
        assert_eq!(std::fs::read(destination.join("binary"))?, [0, 255, 1, 0]);
        assert_eq!(std::fs::read(destination.join(".kept"))?, b"kept");
        assert!(!destination.join("deleted").exists());
        assert!(!destination.join(".deleted").exists());
        Ok(())
    }

    #[test]
    fn workspace_root_must_be_independently_replaceable() {
        for path in ["/", ".", "/a/..", "/a/../b", "//a", "/a/", "/a\0b"] {
            assert!(validate_workspace(path).is_err());
        }
        assert!(validate_workspace("/home/user/workspace").is_ok());
    }
}
