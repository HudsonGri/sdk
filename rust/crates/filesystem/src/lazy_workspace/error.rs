use super::*;

/// The original consumer failure and the source publication still needing rollback.
/// Directory admission funds this complete payload before requesting a source page.
#[derive(Debug, Error)]
#[error("{primary}; source rollback failed: {cleanup}")]
pub struct SourcePageCleanupFailure {
    /// Original consumer failure, including its semantic classification.
    #[source]
    pub primary: LazyWorkspaceError,
    /// Original rollback failure.
    pub cleanup: DemandError,
    /// Producer state still requiring cleanup.
    pub publication: Option<Box<crate::demand::SourceDirectoryPublication>>,
}

pub(super) fn reserve_source_page_failure(
    ctx: &mut KernelAccounting<'_>,
) -> Result<Owned<Box<std::mem::MaybeUninit<SourcePageCleanupFailure>>>, LazyWorkspaceError> {
    let bytes = std::mem::size_of::<SourcePageCleanupFailure>() as u64;
    let guard = ctx.claim_owned(bytes, 1)?;
    Ok(Owned::new(
        Box::<SourcePageCleanupFailure>::new_uninit(),
        guard,
        bytes,
    ))
}

/// Fail-closed lazy workspace errors.
#[derive(Debug, Error)]
pub enum LazyWorkspaceError {
    /// Original workspace failure.
    #[error(transparent)]
    WorkspaceCause(#[from] WorkspaceError),
    /// Original filesystem failure.
    #[error(transparent)]
    Filesystem(#[from] crate::FsError),
    /// Original path validation failure.
    #[error(transparent)]
    NamespacePath(#[from] crate::kernel::NamespacePathError),
    /// Original provider failure.
    #[error(transparent)]
    Demand(#[from] DemandError),
    /// Companion-store failure.
    #[error("lazy state failed: {0}")]
    Store(String),
    /// Durable state exhausted its bounded retries.
    #[error("lazy state changed concurrently")]
    Concurrent,
    /// The provider no longer matches the frozen source.
    #[error("lazy source binding is stale")]
    StaleSource,
    /// Authored state and tombstones leave no path.
    #[error("path is absent")]
    NotFound,
    /// Traversal requires a directory.
    #[error("path is not a directory")]
    NotDirectory,
    /// The read requires a regular file.
    #[error("path is not a regular file")]
    NotRegularFile,
    /// The requested byte bound is too small.
    #[error("file exceeds the requested read bound")]
    TooLarge,
    /// A page must request at least one entry.
    #[error("directory page bound must be positive")]
    InvalidPageBound,
    /// The directory view changed since the cursor.
    #[error("lazy directory cursor is stale")]
    StaleCursor,
    /// The resolved identity has changed.
    #[error("lazy filesystem object identity is stale")]
    StaleIdentity,
    /// The workspace profile cannot represent the source node.
    #[error("source node cannot be represented by the authored workspace")]
    UnsupportedNode,
    /// The operation was cancelled.
    #[error("lazy exact scan was cancelled")]
    Cancelled,
    /// The original logical cut cannot be authenticated.
    #[error("original snapshot is unavailable")]
    OriginalSnapshotUnavailable,
    /// Retains the original consumer and producer rollback failures.
    #[error("{failure}")]
    SourcePageCleanup {
        /// Complete rollback payload, admitted on the original ledger.
        #[source]
        failure: Box<SourcePageCleanupFailure>,
        /// Retained diagnostic funding.
        #[cfg(not(target_arch = "wasm32"))]
        allocation: Option<crate::NativeAllocation>,
    },
    /// Original native denial.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeWork(#[from] crate::NativeTreeMaterializationError),
    /// Funding belongs to another invocation.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("native ledger mismatch")]
    NativeLedgerMismatch,
    /// Original record IO failure.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("{0}")]
    NativeRecordIo(#[source] std::io::Error, Option<crate::NativeAllocation>),
    /// Original record codec failure.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("{0}")]
    NativeRecordJson(#[source] serde_json::Error, Option<crate::NativeAllocation>),
    /// The record lock timed out.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("record lock timed out: {}", " .0.display()")]
    NativeRecordLockTimeout(std::path::PathBuf, Option<crate::NativeAllocation>),
    /// Another owner controls the record namespace.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("record ownership conflict: {}", " .0.display()")]
    NativeRecordOwnershipConflict(std::path::PathBuf, Option<crate::NativeAllocation>),
    /// The context transaction is incompatible.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("context transaction is incompatible: {0}")]
    NativeRecordContextTransaction(String, Option<crate::NativeAllocation>),
    /// An intact log frame is undecodable.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("corrupt record log: {}", " .0.display()")]
    NativeRecordCorrupt(std::path::PathBuf, Option<crate::NativeAllocation>),
    /// A nonterminal materialization cannot be discarded.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("materialization is in progress")]
    NativeRecordMaterializationInProgress,
    /// Stored node differs from its content address.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("record integrity failure")]
    NativeRecordIntegrity,
}
impl LazyWorkspaceError {
    /// Original semantic cause.
    pub fn semantic_error(&self) -> &Self {
        match self {
            Self::SourcePageCleanup { failure, .. } => failure.primary.semantic_error(),
            error => error,
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn rollback_payload_uses_one_original_funded_allocation_and_retains_both_causes() {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let before = original.live_allocation_bytes();
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &original,
            cancellation: &cancellation,
        };
        let slot = reserve_source_page_failure(&mut ctx).unwrap();
        assert_eq!(
            original.live_allocation_bytes() - before,
            std::mem::size_of::<SourcePageCleanupFailure>() as u64
        );
        let allocation = match slot.allocation {
            KernelOwnedAllocation::NativeOriginal(allocation) => Some(allocation),
            KernelOwnedAllocation::Standalone => panic!("original allocation lost"),
        };
        let error = LazyWorkspaceError::SourcePageCleanup {
            failure: Box::write(
                slot.value,
                SourcePageCleanupFailure {
                    primary: LazyWorkspaceError::StaleSource,
                    cleanup: DemandError::WorkerUnavailable,
                    publication: None,
                },
            ),
            allocation,
        };
        assert!(matches!(
            error.semantic_error(),
            LazyWorkspaceError::StaleSource
        ));
        assert!(
            matches!(&error, LazyWorkspaceError::SourcePageCleanup { failure, .. } if matches!(failure.cleanup, DemandError::WorkerUnavailable))
        );
        assert_eq!(
            original.live_allocation_bytes() - before,
            std::mem::size_of::<SourcePageCleanupFailure>() as u64
        );
        drop(error);
        assert_eq!(original.live_allocation_bytes(), before);
    }

    #[test]
    fn rollback_payload_is_admitted_before_the_box_is_allocated() {
        let cancellation = CancellationToken::new();
        // The constructor consumes the only admitted operation for its own Arc.
        let budget = WorkBudget {
            allocation_operations: 1,
            ..WorkBudget::UNBOUNDED
        };
        let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let before = original.live_allocation_bytes();
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &original,
            cancellation: &cancellation,
        };
        assert!(reserve_source_page_failure(&mut ctx).is_err());
        assert_eq!(original.live_allocation_bytes(), before);
    }
}
