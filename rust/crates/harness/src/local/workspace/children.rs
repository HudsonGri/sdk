//! A project volume's view related to this checkout.
use super::*;

impl LocalWorkspace {
    pub(in crate::local) async fn project_view(&self, project: &VolumeRef) -> Result<View> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let project_workspace = self
            .filesystem
            .open_workspace(
                project.storage_name(&mut None)?,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        self.view()
            .open_related(
                project_workspace,
                &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await
            .map_err(storage)
    }
}
