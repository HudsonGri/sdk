use super::*;
use acyclic_fs::access::{
    AccessView, Action, Capability, Context, Generations, Grant, PathTrie, Policy, Principal,
    PrincipalKind, Resource, ResourcePattern,
};
use acyclic_fs::native_mount::MountOptions;
use std::sync::Arc;

pub(super) async fn options<A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore>(
    workspace: &acyclic_fs::Workspace<A, O>,
) -> Result<MountOptions, Failure> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut head = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            budget,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let (policy, revision) = head
        .read_access_policy(budget, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let policy: Policy =
        serde_json::from_slice(&policy.ok_or("benchmark policy is missing")?.bytes)?;
    let positions = head
        .generation_position(head.generation_id(), budget, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let principal = Principal {
        kind: PrincipalKind::Service,
        id: "mount-benchmark".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: "**".into(),
            },
        }]],
        expires_at: None,
    };
    let volume = uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string();
    let view = PathTrie::compile(
        &policy,
        &principal,
        Resource {
            config: head.volume_config(),
            lineage: &[],
            volume: &volume,
            generation: positions.0.get(),
            head: positions.1.get(),
            path: "",
        },
        Context {
            now: 0,
            approvals: 0,
        },
        &capability,
    )?
    .with_policy_revision(head.generation_id(), revision);
    Ok(MountOptions::read_write()
        .publication(acyclic_fs::MountPublication::Manual)
        .access_view(Arc::new(AccessView::new(view))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn benchmark_mounts_bind_real_volume_policy() -> Result<(), Failure> {
        let fs = acyclic_fs::Fs::memory();
        let workspace = fs
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                "bench-options",
                acyclic_fs::VolumeInitialization {
                    config: VolumeConfig::portable(Lifecycle::Ephemeral),
                    access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
                },
                OperationId::new(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                None,
            )
            .await
            .map_err(|failure| failure.error)?
            .value
            .workspace();
        let options = options(&workspace).await?;
        let view = options.require_access_view()?.current()?;
        assert!(view.allows_every_path(&Action::ALL, 0));
        assert_eq!(
            view.volume(),
            uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string()
        );
        assert!(
            MountOptions::read_write().require_access_view().is_err(),
            "negative control must reject the former unbound options"
        );
        Ok(())
    }
}
