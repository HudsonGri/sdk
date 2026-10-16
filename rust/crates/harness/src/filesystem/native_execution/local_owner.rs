use super::*;

/// Create or recover an embedded native workspace with explicit initial policy.
#[cfg(feature = "filesystem-local")]
pub async fn local_owner_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    filesystem: &acyclic_fs::Fs<A, O>, name: &str,
) -> Result<acyclic_fs::Workspace<A, O>> {
    let mut operation = [0; 16];
    operation.copy_from_slice(&blake3::hash(name.as_bytes()).as_bytes()[..16]);
    filesystem.create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
        name, acyclic_fs::VolumeInitialization {
            config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
            access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
        }, acyclic_fs::OperationId::from_bytes(operation),
        acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new(), None,
    ).await.map(|receipt| receipt.value.workspace())
        .map_err(|failure| Error::Invalid(failure.error.to_string()))
}

/// Bind an embedded host-owned workspace through the same signed authority
/// compiler as task mounts. The returned options retain their refresh owner.
#[cfg(feature = "filesystem-local")]
pub async fn local_owner_mount_options<A, O>(
    workspace: &acyclic_fs::Workspace<A, O>,
    writable: bool,
) -> Result<acyclic_fs::MountOptions>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    use crate::core::{AggregateKind, Authority, AuthorityIssuer};
    acyclic_fs::initialize_native_mount_services().map_err(|error| Error::Invalid(error.to_string()))?;
    acyclic_native_runtime::initialize_native_io_services().map_err(Error::process_io)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| Error::Invalid(error.to_string()))?;
    let mut key = [0; 32];
    getrandom::fill(&mut key).map_err(|error| Error::Invalid(error.to_string()))?;
    let issuer = AuthorityIssuer::new("embedded-workspace", key, Authority {
        kind: AggregateKind::Agent, id: "embedded-workspace-owner".into(),
    });
    key.fill(0);
    owner_mount_options(workspace, &issuer, None, writable, &work).await
}
