//! Native attempts retain the original caller's failure, cancellation and work.
use super::*;
use acyclic_native_runtime::{NativeOperationKind, NativeOperationReservation, ReadAllocation};
use std::path::{Path, PathBuf};

pub(super) fn operation<T>(
    work: Option<&Arc<crate::memory::Work>>,
    write: bool,
    attempt: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, LocalStreamError> {
    let kind = if write {
        NativeOperationKind::Write
    } else {
        NativeOperationKind::Read
    };
    operation_kind(work, kind, attempt)
}

fn operation_kind<T>(
    work: Option<&Arc<crate::memory::Work>>,
    kind: NativeOperationKind,
    attempt: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, LocalStreamError> {
    let Some(work) = work else {
        return attempt().map_err(Into::into);
    };
    page::observe(Some(work), work.meter.check())?;
    let mut ticket = page::observe(
        Some(work),
        NativeOperationReservation::new(Arc::clone(&work.meter), kind, 1),
    )?;
    let (physical, observed) = ticket.attempt(attempt);
    // Record both outcomes before propagating either one.
    let physical = page::observe(Some(work), physical);
    let observed = page::observe(Some(work), observed);
    observed?;
    let output = physical?;
    page::observe(Some(work), work.meter.check())?;
    Ok(output)
}

pub(super) fn sync_directory(
    path: &Path,
    durability: super::super::LocalDurability,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<(), LocalStreamError> {
    operation_kind(work, NativeOperationKind::Durability, || {
        acyclic_native_runtime::sync_parent(path, super::super::native_durability(durability))
    })
}

pub(super) fn sync(
    file: &File,
    durability: super::super::LocalDurability,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<(), LocalStreamError> {
    operation_kind(work, NativeOperationKind::Durability, || {
        acyclic_native_runtime::sync_file(file, super::super::native_durability(durability), None)
    })
}

pub(super) struct LocalPath {
    pub(super) value: PathBuf,
    allocation: Option<ReadAllocation>,
}

/// Construct the bounded derived path under one owner before fallible work.
/// Values are destroyed before their allocation funding on every exit.
pub(super) fn path(
    root: &Path,
    suffix: &str,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<LocalPath, LocalStreamError> {
    let capacity = root
        .as_os_str()
        .len()
        .checked_add(suffix.len())
        .and_then(|length| length.checked_add(1))
        .ok_or(LocalStreamError::InvalidLimits)?;
    let allocation = work
        .map(|work| page::observe(Some(work), ReadAllocation::new(&work.meter, capacity)))
        .transpose()?;
    let mut path = LocalPath {
        value: PathBuf::new(),
        allocation,
    };
    if let Err(error) = path.value.try_reserve_exact(capacity) {
        return match work {
            Some(work) => page::observe(Some(work), Err(work.meter.allocation_failed(error))),
            None => Err(LocalStreamError::Io(std::io::ErrorKind::OutOfMemory.into())),
        };
    }
    if let Some(allocation) = &mut path.allocation {
        page::observe(work, allocation.reconcile_capacity(path.value.capacity()))?;
    }
    page::codec(work, capacity as u64, 0, true)?;
    path.value.push(root);
    path.value.push(suffix);
    page::codec(work, path.value.as_os_str().len() as u64, 0, false)?;
    Ok(path)
}
