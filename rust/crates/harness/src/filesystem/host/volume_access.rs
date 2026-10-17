use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(super) async fn volume_copy_access<'a>(
        &self,
        workspace: &Workspace<A, O>,
        grant: &ContentGrant,
        generation: Option<GenerationId>,
        operation: VolumeOperation,
        actions: &[acyclic_fs::access::Action],
        io: &'a PreparationIo,
    ) -> Result<super::super::access::CompiledAccess<'a>> {
        let scope = grant.policy_scope();
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut io.history_io()),
        )?;
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
        Ok(view)
    }
}
