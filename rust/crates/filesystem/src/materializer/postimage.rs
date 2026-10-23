//! Durable attribution of exact staged postimages; observed live files never mint proofs.
use super::*;
use crate::kernel::{FileRecord, NamespacePath};

fn key(
    root: crate::NativeRootIdentity,
    path: &NamespacePath,
    work: &NativeMaterializationWork,
) -> Result<[u8; 32], NativeTreeMaterializationError> {
    work.charge(crate::WorkCounters {
        bytes_hashed: 16
            + path
                .components()
                .iter()
                .map(|name| 9 + name.as_bytes().len() as u64)
                .sum::<u64>(),
        ..crate::WorkCounters::UNCHARGED
    })?;
    let mut hash = blake3::Hasher::new_derive_key("acyclic.native.postimage/v1");
    hash.update(&root.to_bytes());
    for component in path.components() {
        hash.update(&[match component.encoding() {
            crate::kernel::NameEncoding::Utf8 => 1,
            crate::kernel::NameEncoding::PosixBytes => 2,
            crate::kernel::NameEncoding::WindowsUtf16Le => 3,
        }]);
        hash.update(&(component.as_bytes().len() as u64).to_le_bytes());
        hash.update(component.as_bytes());
    }
    Ok(*hash.finalize().as_bytes())
}

fn canonical(
    record: FileRecord,
    work: &NativeMaterializationWork,
) -> Result<[u8; 32], crate::FsError> {
    if record.kind == crate::kernel::FileKind::Directory {
        work.charge(crate::WorkCounters {
            bytes_hashed: 57,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let mut hash = blake3::Hasher::new_derive_key("acyclic.native.postimage.directory/v1");
        hash.update(&record.file_id.into_bytes());
        hash.update(&record.metadata.to_bytes());
        hash.update(&record.link_count.to_le_bytes());
        return Ok(*hash.finalize().as_bytes());
    }
    let mut accounting = crate::kernel::KernelAccounting::native(work, work.cancellation());
    let (bytes, _guard) = crate::kernel::codec::encode_owned(&mut accounting, |observe, error| {
        crate::kernel::encode_file_record(record, observe, error)
    })?;
    work.charge(crate::WorkCounters {
        bytes_hashed: bytes.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

pub(crate) async fn matches(
    state: &crate::LocalCoreStateStore,
    root: &crate::native_host::HostRoot,
    path: &NamespacePath,
    record: FileRecord,
    observation: &crate::native_host::HostStat,
    work: &NativeMaterializationWork,
) -> Result<bool, crate::CaptureError> {
    if !matches!(
        record.kind,
        crate::kernel::FileKind::Regular | crate::kernel::FileKind::Directory
    ) {
        return Ok(false);
    }
    let state = state.materialization(work.clone());
    let Some(image) = state
        .native_postimage(key(root.identity(), path, work)?)
        .await?
    else {
        return Ok(false);
    };
    if canonical(record, work)? != image.canonical {
        return Ok(false);
    }
    let mut observed = crate::WorkspaceMetadata::from_engine(
        crate::native_capture::physical_metadata(observation),
    );
    let mut expected = image.metadata;
    // Reads and host bookkeeping advance these stamps. An explicitly moved
    // backwards stamp still invalidates attribution, as in ordinary capture.
    for (now, before) in [
        (observed.accessed_ns, expected.accessed_ns),
        (observed.changed_ns, expected.changed_ns),
    ] {
        if now.zip(before).is_some_and(|(now, before)| now < before) {
            return Ok(false);
        }
    }
    observed.accessed_ns = None;
    observed.changed_ns = None;
    expected.accessed_ns = None;
    expected.changed_ns = None;
    if record.kind == crate::kernel::FileKind::Directory {
        // Child installs alter directory timestamps. Children are reconciled
        // individually; the directory proof binds its inode and permissions.
        observed.modified_ns = None;
        expected.modified_ns = None;
    }
    if observed.created_ns.is_none() || expected.created_ns.is_none() {
        observed.created_ns = None;
        expected.created_ns = None;
    }
    if observed != expected {
        return Ok(false);
    }
    let (text, _guard) = path.unicode_text_with_accounting(
        &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
        work.cancellation(),
    )?;
    let relative = text
        .strip_prefix('/')
        .ok_or(crate::CaptureError::InvalidOptions)?;
    Ok(fingerprint(root, Path::new(relative), record.kind, work)? == Some(image.native))
}

fn fingerprint(
    root: &crate::native_host::HostRoot,
    path: &Path,
    kind: crate::kernel::FileKind,
    work: &NativeMaterializationWork,
) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
    if kind == crate::kernel::FileKind::Directory {
        super::metadata::native_metadata_fingerprint(&(root, path), None, work)
    } else {
        super::metadata::native_entry_fingerprint(&(root, path), work)
    }
}

#[cfg(feature = "native-mount")]
pub(super) async fn remember<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    state: &crate::LocalCoreStateStore,
    to: &crate::Generation<A, O>,
    journal: &MaterializationJournal,
    staged: &crate::native_host::HostRoot,
    work: &NativeMaterializationWork,
) -> Result<(), NativeWorkspacePublicationError> {
    // All proofs are durable before replay can perform the first live effect.
    // Replaying an older completed operation must never overwrite newer proofs.
    if journal.phase != MaterializationPhase::Prepared {
        return Ok(());
    }
    let root = crate::NativeRootIdentity::from_bytes(
        journal
            .root_identity
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
    );
    let mut checkout = to
        .workspace()
        .checkout(
            crate::model::GenerationSelector::Exact(to.id()),
            crate::model::CheckoutMode::read_only_pinned(),
            work,
            work.cancellation(),
        )
        .await?;
    let state = state.materialization(work.clone());
    for (edit, preimage) in journal.plan.edits.iter().zip(&journal.preimages) {
        let MaterializationEdit::Install { path, .. } = edit else {
            continue;
        };
        let [_, Some(native)] = super::witness::decode_native_witness(&preimage.image)? else {
            continue;
        };
        let Some(staged_fingerprint) =
            super::metadata::native_entry_fingerprint(&(staged, Path::new(path)), work)?
        else {
            // A crash may follow the rename but precede its progress record.
            // That rename was reachable only after every proof was durable.
            continue;
        };
        if staged_fingerprint != native {
            return Err(NativeTreeMaterializationError::ExternalMutation.into());
        }
        let (absolute, _guard) = work.display(&format_args!("/{path}"))?;
        let config = checkout.volume_config();
        let (namespace, _namespace_guard) = NamespacePath::parse_in_profile(
            &absolute,
            config.profile,
            config.limits,
            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
            work.cancellation(),
        )
        .map_err(crate::WorkspaceError::from)?;
        let entry = checkout
            .lookup_no_follow(&namespace, work, work.cancellation())
            .await
            .map_err(crate::WorkspaceError::from)?;
        let Some(record) = entry.record else {
            continue;
        };
        remember_tree(
            &state,
            &mut checkout,
            root,
            staged,
            (namespace, _namespace_guard, record),
            work,
        )
        .await?;
    }
    Ok(())
}

#[cfg(feature = "native-mount")]
async fn remember_tree<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    state: &crate::LocalCoreStateStore,
    checkout: &mut crate::Checkout<A, O>,
    root: crate::NativeRootIdentity,
    staged: &crate::native_host::HostRoot,
    first: (
        NamespacePath,
        crate::kernel::KernelOwnedAllocation,
        FileRecord,
    ),
    work: &NativeMaterializationWork,
) -> Result<(), NativeWorkspacePublicationError> {
    let config = checkout.volume_config();
    let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut pending = Vec::new();
    push_buffer(&mut pending, first, &mut storage)?;
    while let Some((path, _path_guard, record)) = pending.pop() {
        work.charge(NativeResourceCredit::Items.delta(1))?;
        remember_one(state, root, staged, &path, record, work).await?;
        if record.kind != crate::kernel::FileKind::Directory {
            continue;
        }
        let mut after: Option<(
            crate::kernel::LogicalName,
            crate::kernel::KernelOwnedAllocation,
        )> = None;
        loop {
            let (page, _page_guard) = checkout
                .list_directory_records(
                    &path,
                    after
                        .as_ref()
                        .map_or(std::ops::Bound::Unbounded, |(name, _)| {
                            std::ops::Bound::Excluded(name)
                        }),
                    256,
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(crate::WorkspaceError::from)?;
            for entry in &page.entries {
                let (child, guard) = path
                    .child_with_accounting(
                        &entry.name,
                        config.limits,
                        &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                        work.cancellation(),
                    )
                    .map_err(crate::WorkspaceError::from)?;
                push_buffer(&mut pending, (child, guard, entry.record), &mut storage)?;
            }
            if !page.has_more {
                break;
            }
            let last = page
                .entries
                .last()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
            after = Some(
                last.name
                    .clone_with_accounting(
                        &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
                        work.cancellation(),
                    )
                    .map_err(crate::WorkspaceError::from)?,
            );
        }
    }
    Ok(())
}

#[cfg(feature = "native-mount")]
async fn remember_one(
    state: &crate::LocalCoreStateStore,
    root: crate::NativeRootIdentity,
    staged: &crate::native_host::HostRoot,
    path: &NamespacePath,
    record: FileRecord,
    work: &NativeMaterializationWork,
) -> Result<(), NativeWorkspacePublicationError> {
    if !matches!(
        record.kind,
        crate::kernel::FileKind::Regular | crate::kernel::FileKind::Directory
    ) {
        return Ok(());
    }
    let (text, _guard) = path
        .unicode_text_with_accounting(
            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
            work.cancellation(),
        )
        .map_err(crate::WorkspaceError::from)?;
    let relative = Path::new(
        text.strip_prefix('/')
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
    );
    let stat = super::backend::host_stat(staged, relative, work)?
        .ok_or(NativeTreeMaterializationError::MissingEntry)?;
    let native = fingerprint(staged, relative, record.kind, work)?
        .ok_or(NativeTreeMaterializationError::MissingEntry)?;
    state
        .record_native_postimage(
            key(root, path, work)?,
            crate::core_state::native_postimages::NativePostimage {
                canonical: canonical(record, work).map_err(crate::WorkspaceError::from)?,
                native,
                metadata: crate::WorkspaceMetadata::from_engine(
                    crate::native_capture::physical_metadata(&stat),
                ),
            },
        )
        .await?;
    Ok(())
}

#[cfg(all(test, feature = "native-mount"))]
#[path = "postimage_tests.rs"]
mod tests;
