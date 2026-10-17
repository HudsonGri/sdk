use super::*;

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// Restores selected paths from a pinned published working tree.
    ///
    /// The source and selection occupy the same durable transition as a
    /// commit-based restore. Recovery therefore uses the original source cut.
    pub async fn restore_paths_from<E: GitFilesystemExecutor>(
        &self,
        source: GitTreeRef,
        paths: Vec<String>,
        workspace_tree: GitTreeRef,
        executor: &E,
    ) -> Result<GitCommandOutput, GitCompatRunError<S::Error, E::Error>> {
        self.validate_workspace_tree(workspace_tree).await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        let output = self
            .prepare_validated(|_| {
                Ok(GitCommandOutput::Action(
                    GitFilesystemAction::RestorePaths {
                        tree: source,
                        paths: paths.clone(),
                    },
                ))
            })
            .await?;
        executor
            .validate()
            .await
            .map_err(GitCompatRunError::Executor)?;
        let GitCommandOutput::Prepared { transition, action } = output else {
            return Err(GitCompatError::InvalidState.into());
        };
        self.finish(transition, &action, executor).await
    }
}
