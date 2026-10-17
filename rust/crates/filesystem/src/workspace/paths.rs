use super::*;

pub(crate) fn customer_path(
    path: &str,
    config: crate::model::VolumeConfig,
) -> Result<NamespacePath, WorkspaceError> {
    let portable = PortablePath::parse(path, config.limits).map_err(WorkspaceError::path)?;
    NamespacePath::from_portable_in_profile(&portable, config.profile, config.limits)
        .map_err(WorkspaceError::path)
}

pub(super) fn path_conflict_kind(
    base: Option<crate::kernel::FileRecord>,
    current: Option<crate::kernel::FileRecord>,
    source: Option<crate::kernel::FileRecord>,
) -> ConflictKind {
    let (Some(base), Some(current), Some(source)) = (base, current, source) else {
        return ConflictKind::Binding;
    };
    if base.kind != current.kind
        || base.kind != source.kind
        || base.file_id != current.file_id
        || base.file_id != source.file_id
    {
        return ConflictKind::Binding;
    }
    if base.link_count != current.link_count || base.link_count != source.link_count {
        return ConflictKind::HardLink;
    }
    if base.metadata != current.metadata || base.metadata != source.metadata {
        return ConflictKind::Metadata;
    }
    match base.kind {
        FileKind::Regular => ConflictKind::Binary,
        FileKind::Directory => ConflictKind::Directory,
        FileKind::SymbolicLink => ConflictKind::SymbolicLink,
        FileKind::Fifo
        | FileKind::Socket
        | FileKind::CharacterDevice
        | FileKind::BlockDevice
        | FileKind::ReparsePoint
        | FileKind::MountBoundary => ConflictKind::Special,
    }
}

pub(super) fn regular_file_bytes(record: crate::kernel::FileRecord) -> Result<u64, WorkspaceError> {
    match record.payload {
        FilePayload::InlineRegular(bytes) => {
            Ok(u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX))
        }
        FilePayload::Regular { logical_bytes, .. } => Ok(logical_bytes),
        _ => Err(WorkspaceError::NotRegularFile),
    }
}

pub(crate) fn workspace_vec_bytes<T>(values: &Vec<T>) -> u64 {
    (values.capacity() * std::mem::size_of::<T>()) as u64
}

pub(crate) fn workspace_owned_strings_bytes(values: &Vec<String>) -> Result<u64, WorkspaceError> {
    let bytes = values
        .capacity()
        .checked_mul(std::mem::size_of::<String>())
        .ok_or(crate::WorkError::Overflow)?;
    values.iter().try_fold(
        u64::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?,
        |bytes, value| {
            bytes
                .checked_add(
                    u64::try_from(value.capacity()).map_err(|_| crate::WorkError::Overflow)?,
                )
                .ok_or_else(|| crate::WorkError::Overflow.into())
        },
    )
}

pub(crate) fn customer_path_with_accounting(
    path: &str,
    config: crate::model::VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(NamespacePath, KernelOwnedAllocation), WorkspaceError> {
    NamespacePath::parse_in_profile(
        path,
        config.profile,
        config.limits,
        accounting,
        cancellation,
    )
    .map_err(WorkspaceError::from)
}

pub(super) fn copy_workspace_label(
    identity: &WorkspaceName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    copy_owned_text("", identity.as_str(), accounting)
}

/// Copies `prefix` and `text` into one exactly sized accounted string.
///
/// On a native ledger one leaf pass reserves the copy across every scope and
/// then claims its storage, so copy limits fail ahead of storage limits and
/// concurrent owners cannot spend the same credit. The reservation settles
/// once the copy fills the new capacity, before the claim is reconciled; if
/// the allocator fails first, the guard returns it. Standalone contexts have
/// a single owner and keep check, copy, record.
fn copy_owned_text(
    prefix: &str,
    text: &str,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    let length = prefix
        .len()
        .checked_add(text.len())
        .ok_or(crate::WorkError::Overflow)?;
    let copied = copy_work(length as u64);
    let fill = |bytes: &mut Vec<u8>| {
        bytes.extend_from_slice(prefix.as_bytes());
        bytes.extend_from_slice(text.as_bytes());
        #[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
        model::completed(bytes.len() as u64);
    };
    #[cfg(not(target_arch = "wasm32"))]
    let native = match accounting {
        KernelAccounting::NativeOriginal { work, cancellation } => Some((*work, *cancellation)),
        _ => None,
    };
    #[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
    let native = native.filter(|_| !model::check_only());
    #[cfg(not(target_arch = "wasm32"))]
    if let Some((work, cancellation)) = native {
        #[cfg(all(test, not(acyclic_fs_loom)))]
        if tests::redundant_preflight() {
            accounting.admit(copied)?;
        }
        let reserved = Reserved(work, std::cell::Cell::new(crate::WorkCounters::UNCHARGED));
        let bytes = crate::kernel::Owned::<Vec<u8>>::allocate_with(
            length,
            accounting,
            |_, requested, operations| {
                let (allocation, credit) =
                    work.reserve_and_claim(cancellation, requested, operations, copied)?;
                reserved.1.set(credit);
                Ok(KernelOwnedAllocation::NativeOriginal(allocation))
            },
            |bytes| {
                fill(bytes);
                let mut pending = reserved.1.get();
                let completed = work.settle_reserved(cancellation, &mut pending, copied);
                reserved.1.set(pending);
                completed
            },
        )?;
        return owned_text(bytes);
    }
    accounting.admit(copied)?;
    let mut bytes = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
    fill(&mut bytes.value);
    if let Err(error) = accounting.record_completed(copied) {
        bytes.retire(accounting)?;
        return Err(error.into());
    }
    owned_text(bytes)
}

/// Copy credit reserved for a fill that has not completed; dropping it
/// returns whatever is still pending.
#[cfg(not(target_arch = "wasm32"))]
struct Reserved<'a>(
    &'a crate::NativeMaterializationWork,
    std::cell::Cell<crate::WorkCounters>,
);

#[cfg(not(target_arch = "wasm32"))]
impl Drop for Reserved<'_> {
    fn drop(&mut self) {
        #[cfg(all(test, not(acyclic_fs_loom)))]
        if tests::skip_refund() {
            return;
        }
        crate::kernel::allocation::effects::refund(self.0, self.1.get_mut());
    }
}

fn owned_text(
    bytes: crate::kernel::Owned<Vec<u8>>,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    Ok(crate::kernel::Owned::new(
        String::from_utf8(bytes.value).map_err(WorkspaceError::engine)?,
        bytes.allocation,
        bytes.bytes,
    ))
}

pub(super) fn merge_workspace_backend<T, E: Into<WorkspaceError>>(
    result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    work: &mut crate::WorkCounters,
    live: u64,
) -> Result<crate::OperationReceipt<T>, WorkspaceError> {
    let backend = match &result {
        Ok(receipt) => receipt.work,
        Err(failure) => *failure.work,
    };
    *work = work
        .with_backend(backend, live)
        .map_err(|error| WorkspaceError::WorkMerge {
            prior: Box::new(*work),
            backend: Box::new(backend),
            error,
        })?;
    result.map_err(|failure| failure.error.into())
}

pub(super) fn workspace_allocation_error(
    error: crate::kernel::allocation::AllocationError,
) -> WorkspaceError {
    error
        .into_work_or(crate::FsError::HandleAllocationFailed)
        .into()
}

pub(super) fn restore_absolute_path(
    relative: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    cancellation.check()?;
    copy_owned_text("/", relative, accounting)
}

pub(super) fn workspace_allocation(
    allocation: KernelOwnedAllocation,
    accounting: &KernelAccounting<'_>,
) -> Result<crate::AuthorityAllocation, WorkspaceError> {
    let _ = accounting;
    match allocation {
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => Ok(Some(allocation)),
        KernelOwnedAllocation::Standalone => {
            #[cfg(not(target_arch = "wasm32"))]
            if accounting.native_work().is_some() {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
            Ok(None)
        }
    }
}

#[cfg(all(test, not(acyclic_fs_loom), not(target_arch = "wasm32")))]
#[path = "paths_tests.rs"]
mod tests;

#[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
#[path = "paths_loom.rs"]
mod model;
