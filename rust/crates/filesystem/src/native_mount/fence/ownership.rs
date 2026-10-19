use super::*;
use crate::materializer::NativeResourceCredit::WriteOperations;
use crate::native_host::NativeHostWork as _;
use std::os::windows::fs::FileExt as _;

const MAGIC: &[u8; 16] = b"ACYPFSROOT000002";
const RECEIPT_BYTES: usize = 64;

impl MountDestinationGuard {
    /// Commit ownership before ProjFS can expose authored state. The receipt
    /// lives in the very file whose exclusive lock outlives that provider.
    pub(in crate::native_mount) fn publish_root_ownership(
        &self,
        identity: crate::NativeRootIdentity,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let file = self.file.as_ref().ok_or(NativeMountError::ControlPending)?;
        let parent = self
            .parent_directory
            .as_ref()
            .ok_or(NativeMountError::InvalidDestination)?;
        let fence_identity = crate::native_host::file_identity_and_length(file, &mut &*original)?.0;
        let parent_identity = crate::native_host::directory_identity(parent, &mut &*original)?;
        let encoded = crate::WorkCounters {
            bytes_encoded: RECEIPT_BYTES as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        original.admit(encoded)?;
        let mut bytes = [0; RECEIPT_BYTES];
        bytes[..16].copy_from_slice(MAGIC);
        bytes[16..32].copy_from_slice(&identity.to_bytes());
        bytes[32..48].copy_from_slice(&fence_identity.to_bytes());
        bytes[48..].copy_from_slice(&parent_identity.to_bytes());
        original.record_admitted(encoded)?;
        let mut offset = 0;
        while offset != bytes.len() {
            let written = (&mut &*original).attempt_io(WriteOperations.delta(1), || {
                file.seek_write(&bytes[offset..], offset as u64)
            })?;
            if written == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
            }
            offset += written;
        }
        (&mut &*original)
            .attempt_io(WriteOperations.delta(1), || file.set_len(RECEIPT_BYTES as u64))?;
        crate::native_host::sync_file_held(file, &mut &*original)?;
        crate::native_host::sync_directory_held(parent, &mut &*original)?;
        Ok(())
    }

    pub(in crate::native_mount) fn owned_root(
        &self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Option<crate::NativeRootIdentity>, NativeMountError> {
        let file = self.file.as_ref().ok_or(NativeMountError::ControlPending)?;
        let (fence_identity, length) =
            crate::native_host::file_identity_and_length(file, &mut &*original)?;
        if length != RECEIPT_BYTES as u64 {
            return Ok(None);
        }
        let parent = self
            .parent_directory
            .as_ref()
            .ok_or(NativeMountError::InvalidDestination)?;
        let parent_identity = crate::native_host::directory_identity(parent, &mut &*original)?;
        let mut bytes = [0; RECEIPT_BYTES];
        let mut offset = 0;
        while offset != bytes.len() {
            let read = (&mut &*original).attempt_io(
                crate::WorkCounters {
                    backend_read_operations: 1,
                    source_bytes_read: (bytes.len() - offset) as u64,
                    ..crate::WorkCounters::UNCHARGED
                },
                || file.seek_read(&mut bytes[offset..], offset as u64),
            )?;
            if read == 0 {
                return Ok(None);
            }
            offset += read;
        }
        // Copying receipt bytes cannot substitute another physical lock file
        // or parent for the one held before provider activation.
        if &bytes[..16] != MAGIC
            || bytes[32..48] != fence_identity.to_bytes()
            || bytes[48..] != parent_identity.to_bytes()
        {
            return Ok(None);
        }
        let mut identity = [0; 16];
        identity.copy_from_slice(&bytes[16..32]);
        Ok(Some(crate::NativeRootIdentity::from_bytes(identity)))
    }
}
