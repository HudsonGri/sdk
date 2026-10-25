//! Backend selection only; the graph owns every lineage transition.
use super::*;
use std::sync::Arc;

/// A shared lineage namespace selected by the embedding service.
/// Reopening a durable filesystem must reopen this same namespace.
#[derive(Clone)]
pub enum WorkspaceLineageStorage {
    /// Process-local state, for an explicitly in-memory deployment.
    Memory(Arc<MemoryWorkspaceLineageStore>),
    /// The same durable companion store used by workspace and task owners.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    Local(crate::LocalCoreStateStore),
    /// The same IndexedDB authority namespace as the browser filesystem.
    #[cfg(all(feature = "browser", target_arch = "wasm32"))]
    Browser(crate::browser::IndexedDbAuthorityStore),
}

/// Storage failure from the selected lineage backend.
#[derive(Debug, Error)]
pub enum WorkspaceLineageStorageError {
    /// The in-memory namespace failed.
    #[error(transparent)]
    Memory(#[from] MemoryWorkspaceLineageStoreError),
    /// The durable companion namespace failed.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[error(transparent)]
    Local(#[from] crate::LocalCoreStateStoreError),
    /// The browser's transactional companion namespace failed.
    #[cfg(all(feature = "browser", target_arch = "wasm32"))]
    #[error(transparent)]
    Browser(#[from] crate::AuthorityStoreError),
}

impl<E: std::error::Error + 'static> WorkspaceLineageError<E> {
    pub(crate) fn map_store<T: std::error::Error + 'static>(
        self,
        map: impl FnOnce(E) -> T,
    ) -> WorkspaceLineageError<T> {
        match self {
            Self::Store(error) => WorkspaceLineageError::Store(map(error)),
            Self::Workspace(error) => WorkspaceLineageError::Workspace(error),
            Self::IncompatibleState => WorkspaceLineageError::IncompatibleState,
            Self::ConflictingRegistration => WorkspaceLineageError::ConflictingRegistration,
            Self::UnauthorizedJoin => WorkspaceLineageError::UnauthorizedJoin,
            Self::TraversalLimit => WorkspaceLineageError::TraversalLimit,
        }
    }
}

impl WorkspaceLineageStore for WorkspaceLineageStorage {
    type Error = WorkspaceLineageStorageError;
    async fn load(&self, id: WorkspaceId) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        match self {
            Self::Memory(store) => store.load(id).await.map_err(Into::into),
            #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
            Self::Local(store) => WorkspaceLineageStore::load(store, id)
                .await
                .map_err(Into::into),
            #[cfg(all(feature = "browser", target_arch = "wasm32"))]
            Self::Browser(store) => WorkspaceLineageStore::load(store, id)
                .await
                .map_err(Into::into),
        }
    }
    async fn compare_and_swap(
        &self,
        id: WorkspaceId,
        revision: u64,
        record: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        match self {
            Self::Memory(store) => store
                .compare_and_swap(id, revision, record)
                .await
                .map_err(Into::into),
            #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
            Self::Local(store) => {
                WorkspaceLineageStore::compare_and_swap(store, id, revision, record)
                    .await
                    .map_err(Into::into)
            }
            #[cfg(all(feature = "browser", target_arch = "wasm32"))]
            Self::Browser(store) => {
                WorkspaceLineageStore::compare_and_swap(store, id, revision, record)
                    .await
                    .map_err(Into::into)
            }
        }
    }
    async fn load_accounted(
        &self,
        id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Owned<WorkspaceLineageRecord>>, WorkspaceLineageError<Self::Error>> {
        match self {
            Self::Memory(store) => store
                .load_accounted(id, accounting)
                .await
                .map_err(|error| error.map_store(Into::into)),
            #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
            Self::Local(store) => store
                .load_accounted(id, accounting)
                .await
                .map_err(|error| error.map_store(Into::into)),
            #[cfg(all(feature = "browser", target_arch = "wasm32"))]
            Self::Browser(store) => store
                .load_accounted(id, accounting)
                .await
                .map_err(|error| error.map_store(Into::into)),
        }
    }
    async fn compare_accounted(
        &self,
        id: WorkspaceId,
        revision: u64,
        record: &WorkspaceLineageRecord,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, WorkspaceLineageError<Self::Error>> {
        match self {
            Self::Memory(store) => store
                .compare_accounted(id, revision, record, accounting)
                .await
                .map_err(|error| error.map_store(Into::into)),
            #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
            Self::Local(store) => store
                .compare_accounted(id, revision, record, accounting)
                .await
                .map_err(|error| error.map_store(Into::into)),
            #[cfg(all(feature = "browser", target_arch = "wasm32"))]
            Self::Browser(store) => store
                .compare_accounted(id, revision, record, accounting)
                .await
                .map_err(|error| error.map_store(Into::into)),
        }
    }
}
