//! Memory-backed provider views for namespace and audit invariants.
use crate::{Error, Result};
use acyclic_fs::access::{AccessView, Capability, Context, PathTrie, Policy, Principal, Resource};
use std::sync::Arc;

/// Compile and audit an authenticated view against real in-memory volume storage.
pub fn access(
    policy: &Policy,
    principal: &Principal,
    capability: &Capability,
    resource: Resource<'_>,
    context: Context,
) -> Result<Arc<AccessView>> {
    futures::executor::block_on(async {
        let fs = acyclic_fs::Fs::memory();
        let cancellation = acyclic_fs::CancellationToken::new();
        let bytes =
            serde_json::to_vec(policy).map_err(|error| Error::Invalid(error.to_string()))?;
        let workspace = fs
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                "view",
                acyclic_fs::VolumeInitialization {
                    config: resource.config,
                    access_policy: Some(&bytes),
                },
                acyclic_fs::OperationId::new(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
                None,
            )
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?
            .value
            .workspace();
        let volume = uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string();
        let view = PathTrie::compile(
            policy,
            principal,
            Resource {
                volume: &volume,
                ..resource
            },
            context,
            capability,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let view = workspace
            .audit_view(
                view,
                principal.clone(),
                capability.clone(),
                None,
                #[cfg(not(target_arch = "wasm32"))]
                None,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(Arc::new(AccessView::new(view)))
    })
}
