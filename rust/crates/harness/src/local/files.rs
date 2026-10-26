use super::*;
use crate::filesystem::{FilesystemContentVerifier, FilesystemFileQuery};

impl LocalHarness {
    /// Authority selecting this local agent's followed document and task files.
    pub fn client_owner(&self) -> crate::wire::Authority {
        crate::wire_codec::encode_authority(self.verifier.audience())
    }
    /// Authenticated reader shared by file-backed client input and documents.
    pub fn client_reader(&self) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        Ok(FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?)
    }

    /// Selected identity read from the existing persisted checkout state.
    #[cfg(feature = "native-execution")]
    pub async fn project_selection(&self) -> Result<acyclic_fs::WorkspaceId> {
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("project diff requires a checkout".into()))?;
        let (selected, _) = workspace.selected_view().await?;
        let selected = selected.as_ref().unwrap_or_else(|| workspace.view());
        Ok(selected.workspace().id())
    }

    /// Bind the selected checkout's retained baseline and exact immutable query.
    /// Waiting for its baseline does not capture or mutate the checkout.
    #[cfg(any(test, feature = "native-execution"))]
    pub async fn project_diff(
        self: &Arc<Self>,
        budget: acyclic_fs::WorkBudget,
        maximum: u32,
    ) -> Result<(
        Arc<dyn crate::conversation::ContentResidencyVerifier>,
        VolumeRef,
        acyclic_stream::BoxProviderStream<'static, Result<crate::context::DiffPublication>>,
    )> {
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("project diff requires a checkout".into()))?;
        let (selected, _) = workspace.selected_view().await?;
        let selected = selected.as_ref().unwrap_or_else(|| workspace.view());
        // A reopened harness reads the baseline back from the root agent's state.
        if self.root_authority.is_some() {
            let (agents, root) = self.agents().await?;
            if let Some(base) = agents.state(root).await.map_err(storage)?.diff_base {
                super::checkout_tools::diff::restore(&workspace.diff_bases, base);
            }
        }
        let base =
            super::checkout_tools::diff::baseline(&workspace.diff_bases, selected.workspace().id())
                .await?;
        let Some(base) = base.generation else {
            return Err(Error::Unsupported(
                "project diff before-images unavailable: checkout contains an unreadable source file".into(),
            ));
        };
        let reader = FilesystemContentVerifier::new(
            self.files
                .for_workspace(&self.project, selected.workspace().clone())?,
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?;
        let stream = reader
            .file_query(&self.project, budget)
            .await?
            .observe_diff(base, maximum)
            .await?;
        Ok((reader, self.project.clone(), stream))
    }

    /// Read-only live queries over one agent volume this harness's authority
    /// reads, such as the root agent's private volume, where its turns stage
    /// their tool calls and tool output.
    pub async fn agent_files(
        &self,
        volume: &VolumeRef,
    ) -> Result<FilesystemFileQuery<LocalStream, LocalObjectBackend>> {
        FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?
        .file_query(volume, acyclic_fs::performance::BOUNDARY_BUDGET)
        .await
    }

    /// Read-only live queries over the agent's actual private SDK files.
    /// The existing verifier retains owner authority; execution-log files and
    /// the host journal remain distinct volumes.
    pub async fn scratch_files(
        &self,
        budget: acyclic_fs::WorkBudget,
    ) -> Result<FilesystemFileQuery<LocalStream, LocalObjectBackend>> {
        FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?
        .file_query(&self.scratch, budget)
        .await
    }
}
