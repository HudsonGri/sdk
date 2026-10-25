//! Durable direct-parent workspace lineage.

use crate::kernel::{KernelAccounting, Owned};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, GenerationId, IdempotencyKey,
    WorkBudget, Workspace, WorkspaceError, WorkspaceId,
};
use acyclic_stream::{ProviderPlatform, ProviderTask};
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;

mod creators;
pub use creators::{
    GenerationFork, GuardedGenerationFork, LazyFork, WorkspaceForkCreation, WorkspaceForkCreator,
};

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
    /// Registers a root workspace with no publication parent. Reads, copies and
    /// durable publication use the original accounting; returned metadata keeps
    /// its allocation custody until retired by that same accounting context.
    pub async fn register_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        if let Some(existing) = self
            .store
            .load_accounted(workspace.id(), accounting)
            .await?
        {
            let valid = lineage_shape_accounted(&existing.value, accounting).map(|valid| {
                valid
                    && existing.value.ready
                    && existing.value.workspace_id == workspace.id()
                    && existing.value.workspace_name == workspace.name().as_str()
                    && existing.value.parent_workspace_id.is_none()
                    && existing.value.parent_workspace_name.is_none()
            });
            if matches!(valid, Ok(true)) {
                return Ok(existing);
            }
            existing.retire(accounting)?;
            valid?;
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        let generation = crate::accounted_fs!(
            accounting,
            WorkspaceLineageError<S::Error>,
            |work, cancellation| workspace.head(work, cancellation)
        )?;
        let record = work::new_root(workspace, generation.id(), accounting)?;
        let registered = self.register(&record.value, accounting).await;
        if let Err(error) = registered {
            record.retire(accounting)?;
            return Err(error);
        }
        Ok(record)
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
            let output = create
                .create(
                    WorkspaceForkCreation {
                        generation,
                        destination: &planned.value.workspace_name,
                        key: idempotency_key,
                    },
                    accounting,
                )
                .await?;
            self.finish_fork(
                parent,
                F::workspace(&output),
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
        // Physical existence alone cannot turn an unregistered workspace, or
        // an interrupted parent's creation, into an admitted fork parent.
        // Observe its ready registration before creating any child intent.
        let registered = self
            .store
            .load_accounted(parent.id(), accounting)
            .await?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        let valid = lineage_shape_accounted(&registered.value, accounting);
        let exact_parent = registered.value.ready
            && registered.value.workspace_id == parent.id()
            && registered.value.workspace_name == parent.name().as_str();
        registered.retire(accounting)?;
        if !valid? || !exact_parent {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
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
        let planned = if let Some(record) = self.store.load_accounted(child_id, accounting).await? {
            record
        } else {
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

    /// Walks one record at a time on the caller's original allocation ledger.
    /// The starting workspace is included; `maximum` bounds its ancestors.
    /// A zero bound rejects even the starting record.
    pub fn walk(&self, workspace: WorkspaceId, maximum: u32) -> WorkspaceLineageCursor<'_, S> {
        WorkspaceLineageCursor {
            graph: self,
            next: Some(workspace),
            anchor: workspace,
            remaining: if maximum == 0 {
                0
            } else {
                u64::from(maximum) + 1
            },
            power: 1,
            span: 0,
        }
    }

    /// Returns the direct-parent chain, nearest parent first.
    pub async fn ancestors(
        &self,
        workspace: WorkspaceId,
        maximum: u32,
    ) -> Result<Vec<WorkspaceLineageRecord>, WorkspaceLineageError<S::Error>> {
        use crate::facade::FsWork as _;
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        let mut cursor = self.walk(workspace, maximum);
        let mut records = Vec::new();
        while let Some(record) = cursor.next_record(&mut accounting).await? {
            if record.value.workspace_id != workspace {
                // This convenience result has an ordinary owner. Native callers
                // consume each owned cursor record before advancing instead.
                records.push(record.value);
            }
        }
        Ok(records)
    }

    async fn required(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<WorkspaceLineageRecord, WorkspaceLineageError<S::Error>> {
        use crate::facade::FsWork as _;
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        self.walk(workspace_id, 1)
            .next_record(&mut accounting)
            .await?
            .map(|record| record.value)
            .ok_or(WorkspaceLineageError::IncompatibleState)
    }

    async fn register(
        &self,
        record: &WorkspaceLineageRecord,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), WorkspaceLineageError<S::Error>> {
        if !lineage_shape_accounted(record, accounting)? {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        if let Some(existing) = self
            .store
            .load_accounted(record.workspace_id, accounting)
            .await?
        {
            let valid = lineage_shape_accounted(&existing.value, accounting)
                .map(|valid| valid && same_lineage(&existing.value, record));
            let pending = !existing.value.ready;
            existing.retire(accounting)?;
            if valid? {
                if record.ready && pending {
                    return work::promote(self, record, record.initial_generation, accounting)
                        .await;
                }
                return Ok(());
            }
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        if self
            .store
            .compare_accounted(record.workspace_id, 0, record, accounting)
            .await?
        {
            return Ok(());
        }
        let existing = self
            .store
            .load_accounted(record.workspace_id, accounting)
            .await?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        let valid = lineage_shape_accounted(&existing.value, accounting)
            .map(|valid| valid && same_lineage(&existing.value, record));
        let ready = existing.value.ready;
        existing.retire(accounting)?;
        if !valid? {
            Err(WorkspaceLineageError::ConflictingRegistration)
        } else if ready == record.ready {
            Ok(())
        } else if record.ready && !ready {
            work::promote(self, record, record.initial_generation, accounting).await
        } else {
            Err(WorkspaceLineageError::ConflictingRegistration)
        }
    }
}

/// An allocation-free traversal state over immutable direct-parent records.
/// Each returned record retains its own original allocation custody. Retire it
/// on the same ledger before requesting the next record.
pub struct WorkspaceLineageCursor<'a, S> {
    graph: &'a WorkspaceGraph<S>,
    next: Option<WorkspaceId>,
    anchor: WorkspaceId,
    remaining: u64,
    power: u64,
    span: u64,
}

impl<S: WorkspaceLineageStore> WorkspaceLineageCursor<'_, S> {
    /// Reads and validates the next record with the supplied original work.
    /// Brent's cycle detector needs constant state rather than an ancestry set.
    pub async fn next_record(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Owned<WorkspaceLineageRecord>>, WorkspaceLineageError<S::Error>> {
        let Some(id) = self.next else {
            return Ok(None);
        };
        if self.remaining == 0 {
            return Err(WorkspaceLineageError::TraversalLimit);
        }
        accounting.check()?;
        let record = self
            .graph
            .store
            .load_accounted(id, accounting)
            .await?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        let valid = lineage_shape_accounted(&record.value, accounting)
            .map(|valid| valid && record.value.workspace_id == id && record.value.ready);
        let valid = match valid {
            Ok(valid) => valid,
            Err(error) => {
                record.retire(accounting)?;
                return Err(error.into());
            }
        };
        if !valid {
            record.retire(accounting)?;
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        self.remaining -= 1;
        self.next = record.value.parent_workspace_id;
        self.span += 1;
        if self.next == Some(self.anchor) {
            record.retire(accounting)?;
            self.next = None;
            return Err(WorkspaceLineageError::TraversalLimit);
        }
        if self.span == self.power {
            if let Some(next) = self.next {
                self.anchor = next;
            }
            // At most u32::MAX + 1 records are admitted, so the next
            // power of two fits in u64 even at the maximum walk bound.
            self.power *= 2;
            self.span = 0;
        }
        Ok(Some(record))
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

#[cfg(any(test, all(feature = "browser", target_arch = "wasm32")))]
pub(crate) mod codec;
mod handle;
pub use handle::{WorkspaceLineageStorage, WorkspaceLineageStorageError};

mod memory;
pub(crate) mod work;
pub use memory::{MemoryWorkspaceLineageStore, MemoryWorkspaceLineageStoreError};

#[cfg(test)]
mod tests;
