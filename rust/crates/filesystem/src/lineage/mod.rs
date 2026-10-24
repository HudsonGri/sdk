//! Durable direct-parent workspace lineage.

use crate::record_store::MAXIMUM_CAS_ATTEMPTS;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, GenerationId, IdempotencyKey,
    WorkBudget, Workspace, WorkspaceError, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::future::Future;
use thiserror::Error;

const LINEAGE_VERSION: u32 = 2;

/// Durable identity and direct parent of one SDK workspace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceLineageRecord {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Whether the planned child authority and its initial generation exist.
    pub ready: bool,
    /// Child workspace described by this record.
    pub workspace_id: WorkspaceId,
    /// Canonical workspace name needed to reopen it after process restart.
    pub workspace_name: String,
    /// Direct parent allowed to receive publication from the child.
    pub parent_workspace_id: Option<WorkspaceId>,
    /// Canonical direct-parent name needed for restart recovery.
    pub parent_workspace_name: Option<String>,
    /// Immutable SDK creation operation; absent only for root registrations.
    pub creation_operation: Option<crate::OperationId>,
    /// Exact parent generation from which the child was created.
    pub fork_generation: GenerationId,
    /// Exact first generation in the child workspace, used for local diffs.
    pub initial_generation: GenerationId,
}

fn lineage_shape_is_valid(record: &WorkspaceLineageRecord) -> bool {
    let parent_shape = match (
        record.parent_workspace_id,
        record.parent_workspace_name.as_deref(),
    ) {
        (None, None) => {
            record.creation_operation.is_none()
                && record.fork_generation == record.initial_generation
        }
        (Some(parent_id), Some(parent_name)) => {
            record.creation_operation.is_some()
                && parent_id != record.workspace_id
                && crate::WorkspaceName::new(
                    parent_name,
                    &WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .is_ok()
        }
        _ => false,
    };
    record.version == LINEAGE_VERSION
        && record.revision > 0
        && crate::WorkspaceName::new(
            &record.workspace_name,
            &WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .is_ok()
        && parent_shape
}

/// Durable optimistic-concurrency adapter for workspace lineage.
pub trait WorkspaceLineageStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads the record for one workspace, if registered.
    fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<WorkspaceLineageRecord>, Self::Error>> + Send;

    /// Atomically replaces `expected_revision`; zero creates a record.
    fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Failure while recording or authenticating workspace lineage.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum WorkspaceLineageError<E: std::error::Error + 'static> {
    /// The durable adapter failed.
    #[error("workspace-lineage store failed: {0}")]
    Store(E),
    /// The workspace operation failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Persisted state uses an unsupported version or mismatched identity.
    #[error("workspace-lineage state is incompatible")]
    IncompatibleState,
    /// An existing workspace is already bound to different lineage.
    #[error("workspace lineage conflicts with its durable registration")]
    ConflictingRegistration,
    /// Optimistic registration contention exceeded the bounded retry policy.
    #[error("workspace lineage registration remained contended")]
    Contended,
    /// Publication was requested by a workspace other than the direct parent.
    #[error("only the direct parent may receive workspace publication")]
    UnauthorizedJoin,
    /// A lineage walk encountered a cycle or exceeded the caller's bound.
    #[error("workspace lineage traversal exceeded its safe bound")]
    TraversalLimit,
}

/// Core recursive-workspace graph over one durable lineage adapter.
pub struct WorkspaceGraph<S> {
    store: S,
}

impl<S> WorkspaceGraph<S> {
    /// Creates a graph over a durable state adapter.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Borrows the underlying adapter.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }
}

impl<S: WorkspaceLineageStore> WorkspaceGraph<S> {
    /// Registers a root workspace with no publication parent.
    pub async fn register_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if let Some(existing) = self
            .store
            .load(workspace.id())
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            if lineage_shape_is_valid(&existing)
                && existing.ready
                && existing.workspace_id == workspace.id()
                && existing.workspace_name == workspace.name().as_str()
                && existing.parent_workspace_id.is_none()
                && existing.parent_workspace_name.is_none()
            {
                return Ok(existing);
            }
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        let generation = workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        self.register(WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: workspace.id(),
            workspace_name: workspace.name().as_str().to_owned(),
            parent_workspace_id: None,
            parent_workspace_name: None,
            creation_operation: None,
            fork_generation: generation.id(),
            initial_generation: generation.id(),
        })
        .await
    }

    /// Executes an SDK-owned creator between durable intent and lineage publication.
    /// The creator receives the original authenticated parent generation on every
    /// recovery. Its result is not returned until the child has been authenticated
    /// against that intent and registered as ready.
    pub async fn fork<A, O, T, E, F, Fut>(
        &self,
        parent: &Workspace<A, O>,
        destination: &str,
        idempotency_key: IdempotencyKey,
        selected: Option<GenerationId>,
        create: F,
        workspace: for<'a> fn(&'a T) -> &'a Workspace<A, O>,
    ) -> Result<T, E>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        E: From<WorkspaceLineageError<S::Error>>,
        F: FnOnce(crate::Generation<A, O>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let planned = self
            .prepare_fork(parent, destination, idempotency_key, selected)
            .await
            .map_err(E::from)?;
        let generation = parent
            .generation(
                planned.fork_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| E::from(WorkspaceLineageError::Workspace(failure.error)))?
            .value;
        let output = create(generation).await?;
        self.finish_fork(parent, workspace(&output), planned, idempotency_key)
            .await
            .map_err(E::from)?;
        Ok(output)
    }

    async fn prepare_fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        parent: &Workspace<A, O>,
        destination: &str,
        idempotency_key: IdempotencyKey,
        selected: Option<GenerationId>,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let destination = crate::WorkspaceName::new(
            destination,
            &WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .map_err(|failure| failure.error)?
        .value;
        let child_id = parent
            .volume
            .fs
            .workspace_id(destination.as_str())
            .map_err(WorkspaceError::from)?;
        // First durable intent wins. Recovery uses that immutable cut,
        // even if the parent has advanced since this operation began.
        let planned = match self
            .store
            .load(child_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            Some(record) => record,
            None => {
                let generation = match selected {
                    Some(id) => {
                        parent
                            .generation(id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                            .await
                    }
                    None => {
                        parent
                            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                            .await
                    }
                }
                .map_err(|failure| failure.error)?
                .value;
                let record = WorkspaceLineageRecord {
                    version: LINEAGE_VERSION,
                    revision: 1,
                    ready: false,
                    workspace_id: child_id,
                    workspace_name: destination.as_str().to_owned(),
                    parent_workspace_id: Some(parent.id()),
                    parent_workspace_name: Some(parent.name().as_str().to_owned()),
                    creation_operation: Some(idempotency_key.operation_id()),
                    fork_generation: generation.id(),
                    initial_generation: generation.id(),
                };
                if self
                    .store
                    .compare_and_swap(child_id, 0, record.clone())
                    .await
                    .map_err(WorkspaceLineageError::Store)?
                {
                    record
                } else {
                    self.store
                        .load(child_id)
                        .await
                        .map_err(WorkspaceLineageError::Store)?
                        .ok_or(WorkspaceLineageError::IncompatibleState)?
                }
            }
        };
        if !lineage_shape_is_valid(&planned)
            || planned.workspace_id != child_id
            || planned.workspace_name != destination.as_str()
            || planned.parent_workspace_id != Some(parent.id())
            || planned.parent_workspace_name.as_deref() != Some(parent.name().as_str())
            || planned.creation_operation != Some(idempotency_key.operation_id())
            || selected.is_some_and(|cut| cut != planned.fork_generation)
        {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        Ok(planned)
    }

    async fn finish_fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        planned: WorkspaceLineageRecord,
        idempotency_key: IdempotencyKey,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if !parent.volume.fs.same_deployment(&child.volume.fs)
            || child.volume.config != parent.volume.config
            || child.name().as_str() != planned.workspace_name
            || child.id() != planned.workspace_id
        {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        // The current child head is not its creation identity. A
        // recovered child can already contain subsequent commits.
        let initial_generation = parent
            .volume
            .fs
            .workspace_for_creation_operation(
                planned.workspace_name.as_str(),
                child.volume.config,
                idempotency_key.operation_id(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value
            .ok_or(WorkspaceLineageError::ConflictingRegistration)?;
        if initial_generation.workspace_id() != planned.workspace_id
            || initial_generation
                .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| failure.error)?
                .value
                .as_slice()
                != [planned.fork_generation]
            || (planned.ready && planned.initial_generation != initial_generation.id())
        {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        let mut ready = planned;
        ready.ready = true;
        ready.initial_generation = initial_generation.id();
        self.promote_ready(ready).await
    }

    /// Verifies that `parent` is the child's exact direct publication target.
    pub async fn authorize_join(
        &self,
        child: WorkspaceId,
        parent: WorkspaceId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let record = self.required(child).await?;
        if record.parent_workspace_id != Some(parent) {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        Ok(record)
    }

    /// Resolves one durable workspace identity to its canonical lineage record.
    pub async fn resolve(
        &self,
        workspace: WorkspaceId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        self.required(workspace).await
    }

    /// Returns the direct-parent chain, nearest parent first.
    pub async fn ancestors(
        &self,
        workspace: WorkspaceId,
        maximum: u32,
    ) -> Result<Vec<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        let mut next = Some(workspace);
        let mut seen = BTreeSet::new();
        let mut records = Vec::new();
        while let Some(id) = next {
            if !seen.insert(id) || records.len() >= maximum as usize {
                return Err(WorkspaceLineageError::TraversalLimit);
            }
            let record = self.required(id).await?;
            next = record.parent_workspace_id;
            if id != workspace {
                records.push(record);
            }
        }
        Ok(records)
    }

    async fn required(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        let record = self
            .store
            .load(workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        if record.workspace_id != workspace_id || !lineage_shape_is_valid(&record) {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        if !record.ready {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        Ok(record)
    }

    async fn register(
        &self,
        record: WorkspaceLineageRecord,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if !lineage_shape_is_valid(&record) {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        if let Some(existing) = self
            .store
            .load(record.workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            if lineage_shape_is_valid(&existing) && same_lineage(&existing, &record) {
                if record.ready && !existing.ready {
                    return self.promote_ready(record).await;
                }
                return Ok(existing);
            }
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        if self
            .store
            .compare_and_swap(record.workspace_id, 0, record.clone())
            .await
            .map_err(WorkspaceLineageError::Store)?
        {
            return Ok(record);
        }
        let existing = self
            .store
            .load(record.workspace_id)
            .await
            .map_err(WorkspaceLineageError::Store)?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        if lineage_shape_is_valid(&existing)
            && same_lineage(&existing, &record)
            && existing.ready == record.ready
        {
            Ok(existing)
        } else if lineage_shape_is_valid(&existing)
            && same_lineage(&existing, &record)
            && record.ready
            && !existing.ready
        {
            self.promote_ready(record).await
        } else {
            Err(WorkspaceLineageError::ConflictingRegistration)
        }
    }

    async fn promote_ready(
        &self,
        ready: WorkspaceLineageRecord,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        if !ready.ready || !lineage_shape_is_valid(&ready) {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let existing = self
                .store
                .load(ready.workspace_id)
                .await
                .map_err(WorkspaceLineageError::Store)?
                .ok_or(WorkspaceLineageError::IncompatibleState)?;
            if !lineage_shape_is_valid(&existing) || !same_lineage(&existing, &ready) {
                return Err(WorkspaceLineageError::ConflictingRegistration);
            }
            if existing.ready {
                return Ok(existing);
            }
            let mut replacement = ready.clone();
            replacement.revision = existing
                .revision
                .checked_add(1)
                .ok_or(WorkspaceLineageError::IncompatibleState)?;
            if self
                .store
                .compare_and_swap(ready.workspace_id, existing.revision, replacement.clone())
                .await
                .map_err(WorkspaceLineageError::Store)?
            {
                return Ok(replacement);
            }
        }
        Err(WorkspaceLineageError::Contended)
    }
}

fn same_lineage(left: &WorkspaceLineageRecord, right: &WorkspaceLineageRecord) -> bool {
    left.version == right.version
        && left.workspace_id == right.workspace_id
        && left.workspace_name == right.workspace_name
        && left.parent_workspace_id == right.parent_workspace_id
        && left.parent_workspace_name == right.parent_workspace_name
        && left.creation_operation == right.creation_operation
        && left.fork_generation == right.fork_generation
        && (!left.ready || !right.ready || left.initial_generation == right.initial_generation)
}

mod memory;
pub use memory::{MemoryWorkspaceLineageStore, MemoryWorkspaceLineageStoreError};

#[cfg(test)]
mod tests;
