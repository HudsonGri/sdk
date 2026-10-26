use super::*;

/// One policy boundary for selected project queries and imports.
pub(in crate::filesystem) async fn project_source_access<
    'a,
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    workspace: &Workspace<A, O>,
    handle: &crate::source_authorization::ProjectSourceReadHandle,
    ceiling: &crate::Capabilities,
    generation: GenerationId,
    io: &'a PreparationIo,
) -> Result<super::super::access::CompiledAccess<'a>> {
    use super::super::access;
    use acyclic_fs::access::Action;
    handle
        .grant()
        .require_directory_path(&handle.authorization().project, "", Some(io))?;
    access::require_directory_ceiling(
        Some(ceiling),
        &handle.authorization().project,
        "",
        Some(io),
    )?;
    let scope = handle.grant().policy_scope();
    let label =
        crate::core::filesystem_scope_label(scope.capabilities(), &mut Some(&mut io.history_io()))?;
    let view = access::access_view(
        workspace,
        scope,
        Some(generation),
        access::capability_payload(label, VolumeOperation::Read, io)?,
        Some(ceiling),
        io,
    )
    .await?;
    access::require_unrestricted(
        &view.0,
        &[Action::List, Action::Read],
        "selected-project queries",
    )?;
    Ok(view)
}

pub(super) async fn authorize_fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    grant: &ContentGrant,
    generation: GenerationId,
    io: &PreparationIo,
) -> Result<()> {
    let access = compile(
        workspace,
        grant,
        Some(generation),
        VolumeOperation::Read,
        &[
            acyclic_fs::access::Action::Read,
            acyclic_fs::access::Action::List,
            acyclic_fs::access::Action::Fork,
        ],
        io,
    )
    .await?;
    super::super::access::require_current(workspace, &access.0, io).await
}

pub(super) async fn compile<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    grant: &ContentGrant,
    generation: Option<GenerationId>,
    operation: VolumeOperation,
    actions: &[acyclic_fs::access::Action],
    io: &'a PreparationIo,
) -> Result<super::super::access::CompiledAccess<'a>> {
    grant.recheck(Some(io))?;
    let scope = grant.policy_scope();
    let label =
        crate::core::filesystem_scope_label(scope.capabilities(), &mut Some(&mut io.history_io()))?;
    let view = super::super::access::access_view(
        workspace,
        scope,
        generation,
        super::super::access::capability_payload(label, operation, io)?,
        None,
        io,
    )
    .await?;
    super::super::access::require_unrestricted(&view.0, actions, "whole-volume copy")?;
    if view.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(
            "whole-volume copy requires non-expiring authority",
        ));
    }
    grant.recheck(Some(io))?;
    Ok(view)
}
