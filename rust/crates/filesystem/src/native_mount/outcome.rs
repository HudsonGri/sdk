//! One semantic classification for every native namespace adapter.
use super::{MountSourceError, NativeMountError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeFailure {
    AccessDenied,
    ReadOnly,
    NotFound,
    AlreadyExists,
    NotDirectory,
    IsDirectory,
    DirectoryNotEmpty,
    Invalid,
    Unsupported,
    Stale,
    Cancelled,
    Allocation,
    TooLarge,
    Os(i32),
    Io,
}

impl NativeFailure {
    fn io(error: &std::io::Error) -> Self {
        error.raw_os_error().map_or(Self::Io, Self::Os)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(super) fn errno(self) -> i32 {
        match self {
            Self::AccessDenied => libc::EACCES,
            Self::ReadOnly => libc::EROFS,
            Self::NotFound => libc::ENOENT,
            Self::AlreadyExists => libc::EEXIST,
            Self::NotDirectory => libc::ENOTDIR,
            Self::IsDirectory => libc::EISDIR,
            Self::DirectoryNotEmpty => libc::ENOTEMPTY,
            Self::Invalid => libc::EINVAL,
            Self::Unsupported => libc::EOPNOTSUPP,
            Self::Stale => libc::ESTALE,
            Self::Cancelled => libc::ECANCELED,
            Self::Allocation => libc::ENOMEM,
            Self::TooLarge => libc::EFBIG,
            Self::Os(code) => code,
            Self::Io => libc::EIO,
        }
    }
}

pub(super) fn source_failure(error: &MountSourceError) -> NativeFailure {
    use NativeFailure as F;
    match error.semantic_error() {
        MountSourceError::AccessDenied | MountSourceError::NativeCallbackDenied { .. } => {
            F::AccessDenied
        }
        MountSourceError::ReadOnly => F::ReadOnly,
        MountSourceError::NotFound => F::NotFound,
        MountSourceError::AlreadyExists => F::AlreadyExists,
        MountSourceError::NotDirectory => F::NotDirectory,
        MountSourceError::IsDirectory => F::IsDirectory,
        MountSourceError::DirectoryNotEmpty => F::DirectoryNotEmpty,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => F::Invalid,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => F::Unsupported,
        MountSourceError::Stale => F::Stale,
        MountSourceError::Cancelled => F::Cancelled,
        MountSourceError::Allocation(_) => F::Allocation,
        MountSourceError::Io(error, _) => F::io(error),
        MountSourceError::NativeWork(error) => materialization_failure(error),
        MountSourceError::WorkspaceCause(error) => workspace_failure(error),
        MountSourceError::Filesystem(error) => filesystem_failure(error),
        _ => F::Io,
    }
}

fn filesystem_failure(error: &crate::FsError) -> NativeFailure {
    use NativeFailure as F;
    match error {
        crate::FsError::Cancelled(_) => F::Cancelled,
        crate::FsError::SourceSnapshotStale => F::Stale,
        crate::FsError::NativeKernel(error) => materialization_failure(error),
        crate::FsError::NotFound => F::NotFound,
        crate::FsError::NotDirectory => F::NotDirectory,
        crate::FsError::HandleAllocationFailed => F::Allocation,
        _ => F::Io,
    }
}

pub(super) fn materialization_failure(
    error: &crate::NativeTreeMaterializationError,
) -> NativeFailure {
    use NativeFailure as F;
    match error {
        crate::NativeTreeMaterializationError::Io(error) => F::io(error),
        crate::NativeTreeMaterializationError::Cancelled => F::Cancelled,
        crate::NativeTreeMaterializationError::AllocationFailed(_) => F::Allocation,
        crate::NativeTreeMaterializationError::PayloadTooLarge { .. } => F::TooLarge,
        _ => F::Io,
    }
}

pub(super) fn workspace_failure(error: &crate::WorkspaceError) -> NativeFailure {
    use crate::WorkspaceError;
    use NativeFailure as F;
    match error {
        WorkspaceError::AccessDenied => F::AccessDenied,
        WorkspaceError::Cancelled(_)
        | WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => {
            F::Cancelled
        }
        WorkspaceError::Filesystem(error) => filesystem_failure(error),
        WorkspaceError::NotFound => F::NotFound,
        WorkspaceError::NotDirectory => F::NotDirectory,
        WorkspaceError::IsDirectory => F::IsDirectory,
        WorkspaceError::DirectoryNotEmpty => F::DirectoryNotEmpty,
        WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Work(_)) => F::Io,
        WorkspaceError::NamespacePath(crate::kernel::NamespacePathError::AllocationFailed) => {
            F::Allocation
        }
        WorkspaceError::Name(_)
        | WorkspaceError::Path(_)
        | WorkspaceError::NotRegularFile
        | WorkspaceError::NamespacePath(_) => F::Invalid,
        WorkspaceError::ReadLimitExceeded => F::TooLarge,
        WorkspaceError::StaleGeneration
        | WorkspaceError::StaleSource
        | WorkspaceError::StaleIdentity
        | WorkspaceError::StaleTarget => F::Stale,
        _ => F::Io,
    }
}

pub(super) fn callback_failure(error: &NativeMountError) -> NativeFailure {
    use NativeFailure as F;
    match error {
        NativeMountError::Source(error) => source_failure(error),
        NativeMountError::Demand(crate::DemandError::Mount(error)) => source_failure(error),
        NativeMountError::Materialization(error) => materialization_failure(error),
        NativeMountError::Io(error) => F::io(error),
        NativeMountError::Allocation(_) => F::Allocation,
        NativeMountError::CallbackDenied { .. } => F::AccessDenied,
        NativeMountError::Demand(crate::DemandError::Cancelled) => F::Cancelled,
        #[cfg(target_os = "linux")]
        NativeMountError::FuseUnsupportedCapabilities(_) => F::Unsupported,
        _ => F::Io,
    }
}

#[cfg(test)]
mod tests;
