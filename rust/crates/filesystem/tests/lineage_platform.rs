//! The canonical graph accepts browser event-loop owners and native executors.

use acyclic_fs::{
    MemoryWorkspaceLineageStore, MemoryWorkspaceLineageStoreError, WorkspaceGraph, WorkspaceId,
    WorkspaceLineageError, WorkspaceLineageRecord, WorkspaceLineageStore,
    kernel::{KernelAccounting, Owned},
};
use acyclic_stream::{ProviderPlatform, ProviderTask};
use std::future::Future;

#[cfg(target_arch = "wasm32")]
use std::rc::Rc as PlatformOwner;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc as PlatformOwner;

// Only the ownership adapter differs. All storage and graph transitions still
// execute the production implementation, including its original accounting.
struct Store(PlatformOwner<MemoryWorkspaceLineageStore>);

impl WorkspaceLineageStore for Store {
    type Error = MemoryWorkspaceLineageStoreError;

    async fn load(&self, id: WorkspaceId) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.0.load(id).await
    }

    async fn compare_and_swap(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.0.compare_and_swap(id, revision, replacement).await
    }

    async fn load_accounted(
        &self,
        id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Owned<WorkspaceLineageRecord>>, WorkspaceLineageError<Self::Error>> {
        self.0.load_accounted(id, accounting).await
    }

    async fn compare_accounted(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: &WorkspaceLineageRecord,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, WorkspaceLineageError<Self::Error>> {
        self.0
            .compare_accounted(id, revision, replacement, accounting)
            .await
    }
}

fn provider_task<F: Future + ProviderTask>(future: F) -> F {
    future
}

#[test]
fn lineage_uses_the_platform_provider_ownership_contract() -> Result<(), Box<dyn std::error::Error>>
{
    fn provider<S: ProviderPlatform>(_: &S) {}
    let graph = WorkspaceGraph::new(Store(
        PlatformOwner::new(MemoryWorkspaceLineageStore::new()),
    ));
    provider(graph.store());
    let future = provider_task(graph.store().load(WorkspaceId::from_bytes([7; 16])));
    assert!(futures::executor::block_on(future)?.is_none());
    Ok(())
}
