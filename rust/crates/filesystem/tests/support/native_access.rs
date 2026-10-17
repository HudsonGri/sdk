use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, MountOptions, WorkBudget, Workspace,
    access::*,
    kernel::MetadataField,
    model::{CheckoutMode, GenerationSelector},
};
use std::sync::Arc;

/// Explicit authority for privileged raw-SDK fixtures, outside the measured span.
pub async fn options<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    options: MountOptions,
) -> Result<MountOptions, Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let mut head = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let revision: MetadataField<_> = head
        .access_policy_revision(WorkBudget::UNBOUNDED, &cancel)
        .await?
        .value;
    let principal = Principal {
        kind: PrincipalKind::Service,
        id: "sdk-fixture".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let resource = ResourcePattern {
        lineage_root: None,
        volume: None,
        generations: Generations::All,
        path: "**".into(),
    };
    let policy = Policy {
        schema_version: POLICY_SCHEMA_VERSION,
        version: 1,
        layers: vec![vec![Rule {
            id: "fixture".into(),
            effect: Effect::Allow,
            principal: PrincipalPattern::default(),
            actions: Action::ALL.into_iter().collect(),
            resource: resource.clone(),
            conditions: Conditions::default(),
        }]],
    };
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource,
        }]],
        expires_at: None,
    };
    let volume = uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string();
    let view = PathTrie::compile(
        &policy,
        &principal,
        Resource {
            config: workspace.volume_config(),
            lineage: &[],
            volume: &volume,
            generation: 1,
            head: 1,
            path: "",
        },
        Context {
            now: 0,
            approvals: 0,
        },
        &capability,
    )?
    .with_policy_revision(head.generation_id(), revision);
    Ok(options.access_view(Arc::new(AccessView::new(view))))
}
