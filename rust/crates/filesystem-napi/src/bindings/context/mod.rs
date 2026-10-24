use super::*;

/// Native recursive-workspace graph backed by the shared core-state namespace.
#[napi]
pub struct NativeWorkspaceGraph {
    pub(in crate::bindings) inner: WorkspaceGraph<LocalCoreStateStore>,
}

/// One durable workspace-lineage record.
#[napi(object)]
pub struct NativeWorkspaceLineageRecord {
    pub version: u32,
    pub revision: BigInt,
    pub workspace_id: Buffer,
    pub workspace_name: String,
    pub parent_workspace_id: Option<Buffer>,
    pub parent_workspace_name: Option<String>,
    pub creation_operation: Option<Buffer>,
    pub fork_generation: Buffer,
    pub initial_generation: Buffer,
}

#[napi]
impl NativeWorkspaceGraph {
    #[napi(factory)]
    pub fn open(state_root: String) -> Self {
        Self {
            inner: WorkspaceGraph::new(LocalCoreStateStore::new(state_root)),
        }
    }

    #[napi]
    pub async fn register_root(
        &self,
        workspace: &NativeWorkspace,
    ) -> Result<NativeWorkspaceLineageRecord> {
        self.inner
            .register_root(&workspace.inner)
            .await
            .map(native_workspace_lineage_record)
            .map_err(napi_error)
    }

    #[napi]
    pub async fn fork(
        &self,
        parent: &NativeWorkspace,
        destination: String,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeWorkspace> {
        let key = native_idempotency_key(idempotency_key)?;
        use acyclic_fs::facade::FsWork as _;
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        fork_accounted(&self.inner, parent, &destination, key, &mut accounting).await
    }

    #[napi]
    pub async fn authorize_join(
        &self,
        child_workspace_id: Buffer,
        parent_workspace_id: Buffer,
    ) -> Result<NativeWorkspaceLineageRecord> {
        self.inner
            .authorize_join(
                WorkspaceId::from_bytes(fixed_16(&child_workspace_id)?),
                WorkspaceId::from_bytes(fixed_16(&parent_workspace_id)?),
            )
            .await
            .map(native_workspace_lineage_record)
            .map_err(napi_error)
    }

    #[napi]
    pub async fn ancestors(
        &self,
        workspace_id: Buffer,
        maximum: NapiU32,
    ) -> Result<Vec<NativeWorkspaceLineageRecord>> {
        let maximum = maximum.into_inner();

        self.inner
            .ancestors(WorkspaceId::from_bytes(fixed_16(&workspace_id)?), maximum)
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(native_workspace_lineage_record)
                    .collect()
            })
            .map_err(napi_error)
    }
}

pub(in crate::bindings) fn native_workspace_lineage_record(
    record: WorkspaceLineageRecord,
) -> NativeWorkspaceLineageRecord {
    NativeWorkspaceLineageRecord {
        version: record.version,
        revision: bigint(record.revision),
        workspace_id: Buffer::from(record.workspace_id.into_bytes().to_vec()),
        workspace_name: record.workspace_name,
        parent_workspace_id: record
            .parent_workspace_id
            .map(|id| Buffer::from(id.into_bytes().to_vec())),
        parent_workspace_name: record.parent_workspace_name,
        creation_operation: record
            .creation_operation
            .map(|id| Buffer::from(id.into_bytes().to_vec())),
        fork_generation: Buffer::from(record.fork_generation.digest().into_bytes().to_vec()),
        initial_generation: Buffer::from(record.initial_generation.digest().into_bytes().to_vec()),
    }
}

async fn fork_accounted<'original>(
    graph: &WorkspaceGraph<LocalCoreStateStore>,
    parent: &NativeWorkspace,
    destination: &str,
    key: acyclic_fs::IdempotencyKey,
    accounting: &mut acyclic_fs::kernel::KernelAccounting<'original>,
) -> Result<NativeWorkspace> {
    graph.fork::<_, _, _, acyclic_fs::WorkspaceLineageError<acyclic_fs::LocalCoreStateStoreError>, _>(
        &parent.inner, destination, key, None, accounting,
        acyclic_fs::lineage::GenerationFork {
            parent: &parent.inner, destination, key,
            permit: acyclic_fs::PublicationPermit::Unrestricted,
        },
        |child| child,
    ).await.map(|inner| NativeWorkspace { inner }).map_err(napi_error)
}
