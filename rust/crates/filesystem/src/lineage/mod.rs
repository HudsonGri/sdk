//! Durable direct-parent workspace lineage.

use crate::kernel::{KernelAccounting, Owned};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, GenerationId, IdempotencyKey,
    WorkBudget, Workspace, WorkspaceError, WorkspaceId,
};
use acyclic_stream::{ProviderPlatform, ProviderTask};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::future::Future;
use thiserror::Error;

mod creators;
pub use creators::{GenerationFork, GuardedGenerationFork, LazyFork, WorkspaceForkCreator};

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
    use crate::facade::FsWork as _;
    let cancellation = CancellationToken::new();
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    lineage_shape_accounted(record, &mut accounting).unwrap_or(false)
}

fn lineage_shape_accounted(
    record: &WorkspaceLineageRecord,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, WorkspaceError> {
    fn name_is_canonical(
        name: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, WorkspaceError> {
        match crate::WorkspaceName::canonical_matches(name, accounting) {
            Err(WorkspaceError::Name(_)) => Ok(false),
            result => result,
        }
    }
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
                && name_is_canonical(parent_name, accounting)?
        }
        _ => false,
    };
    Ok(record.version == LINEAGE_VERSION
        && record.revision > 0
        && name_is_canonical(&record.workspace_name, accounting)?
        && parent_shape)
}

/// Durable optimistic-concurrency adapter for workspace lineage.
pub trait WorkspaceLineageStore: ProviderPlatform {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads the record for one workspace, if registered.
    fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<WorkspaceLineageRecord>, Self::Error>> + ProviderTask;

    /// Atomically replaces `expected_revision`; zero creates a record.
    fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> impl Future<Output = Result<bool, Self::Error>> + ProviderTask;

    /// Loads a record with payload custody on the caller's original ledger.
    fn load_accounted(
        &self,
        workspace_id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<
        Output = Result<Option<Owned<WorkspaceLineageRecord>>, WorkspaceLineageError<Self::Error>>,
    > + ProviderTask;

    /// Copies and persists the exact record using the caller's original ledger.
    fn compare_accounted(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: &WorkspaceLineageRecord,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<bool, WorkspaceLineageError<Self::Error>>> + ProviderTask;
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
    /// Publication was requested by a workspace other than the direct parent.
    #[error("only the direct parent may receive workspace publication")]
    UnauthorizedJoin,
    /// A lineage walk encountered a cycle or exceeded the caller's bound.
    #[error("workspace lineage traversal exceeded its safe bound")]
    TraversalLimit,
}

impl<E: std::error::Error + 'static> From<crate::FsError> for WorkspaceLineageError<E> {
    fn from(error: crate::FsError) -> Self {
        Self::Workspace(error.into())
    }
}

impl<E: std::error::Error + 'static> From<crate::WorkError> for WorkspaceLineageError<E> {
    fn from(error: crate::WorkError) -> Self {
        Self::Workspace(error.into())
    }
}

/// Shares an adapter without introducing another lineage implementation.
impl<S: WorkspaceLineageStore + ?Sized> WorkspaceLineageStore for std::sync::Arc<S> {
    type Error = S::Error;

    fn load(
        &self,
        id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<WorkspaceLineageRecord>, Self::Error>> + ProviderTask
    {
        (**self).load(id)
    }

    fn compare_and_swap(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> impl Future<Output = Result<bool, Self::Error>> + ProviderTask {
        (**self).compare_and_swap(id, revision, replacement)
    }

    fn load_accounted(
        &self,
        id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<
        Output = Result<Option<Owned<WorkspaceLineageRecord>>, WorkspaceLineageError<Self::Error>>,
    > + ProviderTask {
        (**self).load_accounted(id, accounting)
    }

    fn compare_accounted(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: &WorkspaceLineageRecord,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<bool, WorkspaceLineageError<Self::Error>>> + ProviderTask {
        (**self).compare_accounted(id, revision, replacement, accounting)
    }
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
    pub async fn fork<'original, A, O, T, E, F>(
        &self,
        parent: &Workspace<A, O>,
        destination: &str,
        idempotency_key: IdempotencyKey,
        selected: Option<GenerationId>,
        accounting: &mut KernelAccounting<'original>,
        create: F,
        workspace: for<'a> fn(&'a T) -> &'a Workspace<A, O>,
    ) -> Result<T, E>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        E: From<WorkspaceLineageError<S::Error>>,
        F: WorkspaceForkCreator<A, O, T, E>,
    {
        let planned = self
            .prepare_fork(parent, destination, idempotency_key, selected, accounting)
            .await
            .map_err(E::from)?;
        let result = async {
            let generation = crate::accounted_fs!(
                accounting,
                WorkspaceLineageError<S::Error>,
                |work, cancellation| parent.generation(
                    planned.value.fork_generation,
                    work,
                    cancellation
                )
            )
            .map_err(E::from)?;
            let output = create.create(generation, accounting).await?;
            self.finish_fork(
                parent,
                workspace(&output),
                &planned.value,
                idempotency_key,
                accounting,
            )
            .await
            .map_err(E::from)?;
            Ok(output)
        }
        .await;
        planned
            .retire(accounting)
            .map_err(|error| E::from(WorkspaceLineageError::from(error)))?;
        result
    }

    async fn prepare_fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        parent: &Workspace<A, O>,
        destination: &str,
        idempotency_key: IdempotencyKey,
        selected: Option<GenerationId>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        let (destination, child_id) =
            parent
                .volume
                .fs
                .workspace_name(destination, accounting, accounting.cancellation())?;
        let result = self
            .prepare_fork_named(
                parent,
                &destination,
                child_id,
                idempotency_key,
                selected,
                accounting,
            )
            .await;
        destination.retire_unique(accounting)?;
        result
    }

    async fn prepare_fork_named<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        parent: &Workspace<A, O>,
        destination: &crate::WorkspaceName,
        child_id: WorkspaceId,
        idempotency_key: IdempotencyKey,
        selected: Option<GenerationId>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        // First durable intent wins. Recovery uses that immutable cut,
        // even if the parent has advanced since this operation began.
        let planned = match self.store.load_accounted(child_id, accounting).await? {
            Some(record) => record,
            None => {
                let generation = match selected {
                    Some(id) => {
                        crate::accounted_fs!(
                            accounting,
                            WorkspaceLineageError<S::Error>,
                            |work, cancellation| parent.generation(id, work, cancellation)
                        )
                    }
                    None => {
                        crate::accounted_fs!(
                            accounting,
                            WorkspaceLineageError<S::Error>,
                            |work, cancellation| parent.head(work, cancellation)
                        )
                    }
                }?;
                let record = work::new_child::<S::Error, _, _>(
                    parent,
                    destination,
                    child_id,
                    idempotency_key,
                    generation.id(),
                    accounting,
                )?;
                match self
                    .store
                    .compare_accounted(child_id, 0, &record.value, accounting)
                    .await
                {
                    Ok(true) => record,
                    result => {
                        record.retire(accounting)?;
                        result?;
                        self.store
                            .load_accounted(child_id, accounting)
                            .await?
                            .ok_or(WorkspaceLineageError::IncompatibleState)?
                    }
                }
            }
        };
        let valid = lineage_shape_accounted(&planned.value, accounting);
        if !valid.as_ref().is_ok_and(|valid| *valid)
            || planned.value.workspace_id != child_id
            || planned.value.workspace_name != destination.as_str()
            || planned.value.parent_workspace_id != Some(parent.id())
            || planned.value.parent_workspace_name.as_deref() != Some(parent.name().as_str())
            || planned.value.creation_operation != Some(idempotency_key.operation_id())
            || selected.is_some_and(|cut| cut != planned.value.fork_generation)
        {
            planned.retire(accounting)?;
            valid?;
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        Ok(planned)
    }

    async fn finish_fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        parent: &Workspace<A, O>,
        child: &Workspace<A, O>,
        planned: &WorkspaceLineageRecord,
        idempotency_key: IdempotencyKey,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), WorkspaceLineageError<S::Error>> {
        if !parent.volume.fs.same_deployment(&child.volume.fs)
            || child.volume.config != parent.volume.config
            || child.name().as_str() != planned.workspace_name
            || child.id() != planned.workspace_id
        {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        // The current child head is not its creation identity. A
        // recovered child can already contain subsequent commits.
        let initial_generation = crate::accounted_fs!(
            accounting,
            WorkspaceLineageError<S::Error>,
            |work, cancellation| parent.volume.fs.workspace_for_creation_operation(
                planned.workspace_name.as_str(),
                child.volume.config,
                idempotency_key.operation_id(),
                work,
                cancellation,
            )
        )?
        .ok_or(WorkspaceLineageError::ConflictingRegistration)?;
        let (parents, parents_allocation) = crate::accounted_fs!(
            accounting,
            WorkspaceLineageError<S::Error>,
            |work, cancellation| initial_generation.workspace.volume.fs.generation_parents(
                &initial_generation.workspace.volume,
                initial_generation.id(),
                work,
                cancellation
            )
        )?;
        let matches_parent = parents.as_slice() == [planned.fork_generation];
        let bytes = (parents.capacity() * std::mem::size_of::<GenerationId>()) as u64;
        let mut parents_allocation = parents_allocation;
        if matches!(
            parents_allocation,
            crate::kernel::KernelOwnedAllocation::Standalone
        ) {
            parents_allocation = accounting.claim_owned(bytes, 0)?;
        }
        drop(parents);
        accounting.release_owned(&mut parents_allocation, bytes)?;
        if initial_generation.workspace_id() != planned.workspace_id
            || !matches_parent
            || (planned.ready && planned.initial_generation != initial_generation.id())
        {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        work::promote(self, planned, initial_generation.id(), accounting).await
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
        use crate::facade::FsWork as _;
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        work::promote(self, &ready, ready.initial_generation, &mut accounting).await?;
        self.required(ready.workspace_id).await
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
pub(crate) mod work;
pub use memory::{MemoryWorkspaceLineageStore, MemoryWorkspaceLineageStoreError};

#[cfg(test)]
mod tests;
