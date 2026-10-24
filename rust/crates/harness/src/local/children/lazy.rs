use super::*;
use crate::{
    filesystem::{FilesystemForkPreparer, PreparationIo},
    fork::{ForkReport, ForkRequest, ResourceRevision},
    resource_preparation::TaskResourcePreparation,
};
use acyclic_fs::facade::FsWork as _;

impl LocalWorkspace {
    pub(super) async fn prepare_child(
        &self,
        source: &workspace::View,
        cut: acyclic_fs::LazySnapshotRef,
        preparer: &FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>,
        request: &ForkRequest,
        preparation: &Arc<TaskResourcePreparation>,
        io: &PreparationIo,
    ) -> Result<(ForkReport, workspace::View)> {
        let mut accounting = io.accounting(io.cancellation());
        let result = self
            .prepare_child_accounted(
                source,
                cut,
                preparer,
                request,
                preparation,
                (io, &mut accounting),
            )
            .await;
        io.sdk(io.finish(result, &accounting))
    }

    async fn prepare_child_accounted<'original>(
        &self,
        source: &workspace::View,
        cut: acyclic_fs::LazySnapshotRef,
        preparer: &FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>,
        request: &ForkRequest,
        preparation: &Arc<TaskResourcePreparation>,
        (io, accounting): (
            &PreparationIo,
            &mut acyclic_fs::kernel::KernelAccounting<'original>,
        ),
    ) -> Result<(ForkReport, workspace::View)> {
        let selected = request.selections.iter().find_map(|selection| {
            if let ResourceRevision::Project { volume, generation } = &selection.revision {
                Some((volume, generation))
            } else {
                None
            }
        });
        let (volume, generation) =
            selected.ok_or_else(|| Error::Invalid("child fork has no project selection".into()))?;
        let source_name = volume.storage_name(&mut Some(&mut io.history_io()))?;
        if source.workspace().id()
            != self
                .filesystem
                .workspace_id(&source_name)
                .map_err(storage)?
            || cut.workspace_id != source.workspace().id()
            || generation.as_resource().key() != cut.authored_generation.digest().as_bytes()
        {
            return Err(Error::Conflict(
                "child fork differs from its lazy source cut".into(),
            ));
        }
        let operation = preparer
            .project_creation_operation(request, preparation, io)
            .await?;
        let destination = request
            .preparation
            .child_project_volume
            .storage_name(&mut Some(&mut io.history_io()))?;
        let selected = source
            .prepare_fork_snapshot(&destination, cut, operation, accounting)
            .await?;
        let graph = acyclic_fs::WorkspaceGraph::new(self.store.clone());
        graph
            .fork(
                source.workspace(),
                &destination,
                acyclic_fs::IdempotencyKey::from_bytes(operation.into_bytes()),
                Some(cut.authored_generation),
                accounting,
                PreparedChildFork {
                    workspace: self,
                    source,
                    selected,
                    preparer,
                    request,
                    preparation,
                    io,
                    destination: &destination,
                    operation,
                },
                |result| result.1.workspace(),
            )
            .await
    }
}

struct PreparedChildFork<'a> {
    workspace: &'a LocalWorkspace,
    source: &'a workspace::View,
    selected: acyclic_fs::LazySnapshotRef,
    preparer: &'a FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>,
    request: &'a ForkRequest,
    preparation: &'a Arc<TaskResourcePreparation>,
    io: &'a PreparationIo,
    destination: &'a str,
    operation: acyclic_fs::OperationId,
}

impl
    acyclic_fs::lineage::WorkspaceForkCreator<
        LocalAuthorityBackend,
        LocalObjectBackend,
        (ForkReport, workspace::View),
        Error,
    > for PreparedChildFork<'_>
{
    async fn create(
        self,
        _generation: acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>,
        accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<(ForkReport, workspace::View)> {
        let Self {
            workspace,
            source,
            selected,
            preparer,
            request,
            preparation,
            io,
            destination,
            operation,
        } = self;
        let report = preparer
            .prepare_for_task(request, preparation, Some(io))
            .await?;
        preparation.verify(false, Some(io))?.await?;
        let child = io.sdk(
            workspace
                .filesystem
                .open_workspace(&destination, io, io.cancellation())
                .await,
        )?;
        let original = io
            .sdk(
                workspace
                    .filesystem
                    .workspace_for_creation_operation(
                        &destination,
                        child.volume_config(),
                        operation,
                        io,
                        io.cancellation(),
                    )
                    .await,
            )?
            .ok_or_else(|| Error::Conflict("child fork has no original creation".into()))?;
        let original_generation = original.id();
        drop(original);
        let bound = source
            .bind_prepared_fork(selected, child, original_generation, operation, accounting)
            .await;
        let (view, _) = bound.map_err(Error::from)?;
        Ok::<_, Error>((report, view))
    }
}
