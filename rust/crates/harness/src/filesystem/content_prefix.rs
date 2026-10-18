//! Authenticated prefixes use the same creation lease and directory grant as full reads.
use super::*;

pub(super) async fn read<A, O>(
    reader: &FilesystemContentVerifier<A, O>,
    volume: &VolumeRef,
    granted_prefix: &str,
    path: &str,
    expected: Option<&GenerationRef>,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<(GenerationRef, u64, acyclic_stream::OwnedBytes)>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    reader.verify_creation(volume, None, io).await?;
    let local;
    let io = match io {
        Some(io) => io,
        None => {
            local = PreparationIo::for_unmeasured(&CancellationToken::new())?;
            &local
        }
    };
    let grant = crate::executor::with_history_io(Some(io), |work| {
        ContentGrant::verify_directory_read(
            &reader.verifier,
            &reader.scope,
            volume,
            granted_prefix,
            work,
        )
    })?;
    let grant_hold = io.guard_owned(grant.logical_owned_bytes()?)?;
    reader
        .host
        .require_private_directory(volume, &grant, path, Some(io))?;
    let workspace_ref =
        fork_preparer_io::content_workspace(&reader.host.provider, volume, Some(io))?;
    let workspace_hold = io.guard_owned(workspace_ref.logical_owned_bytes()?)?;
    let workspace = reader.host.open(&workspace_ref, Some(io)).await?;
    let generation = match expected {
        Some(expected) => {
            reader
                .host
                .generation(&workspace, expected, Some(io))
                .await?
        }
        None => io.sdk(workspace.head(io, io.cancellation()).await)?,
    };
    if expected.is_none() {
        fork_preparer_io::retain(&generation, Some(io)).await?;
    }
    let path = crate::executor::with_history_io(Some(io), |work| {
        crate::executor::preparation_format(format_args!("/{path}"), work)
    })?;
    let path_hold = io.guard_owned(path.capacity() as u64)?;
    let stat = io.sdk(generation.stat(&path, io, io.cancellation()).await)?;
    if stat.kind != FileKind::Regular {
        return Err(acyclic_fs::WorkspaceError::NotRegularFile.into());
    }
    let length = stat
        .logical_bytes
        .ok_or_else(|| io.invalid("prefix file has no length"))?;
    let end = length.min(maximum);
    if end > reader.maximum_bytes {
        return Err(acyclic_fs::WorkspaceError::ReadLimitExceeded.into());
    }
    let chunk = io.sdk(
        generation
            .read_range(&path, 0, end, io, io.cancellation())
            .await,
    )?;
    let reference = reader.host.generation_ref_id(generation.id(), Some(io))?;
    drop(path);
    drop(path_hold);
    drop(grant);
    drop(grant_hold);
    drop(workspace_ref);
    drop(workspace_hold);
    Ok((reference, length, chunk.bytes))
}

use acyclic_fs::CancellationToken;
