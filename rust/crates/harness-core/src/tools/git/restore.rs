use super::*;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn restore_paths(
        &self,
        operation_id: OperationId,
        tree: GitTreeRef,
        paths: &[String],
    ) -> Result<GitFilesystemResult, Error> {
        let source = acyclic_fs::in_heap(|| self.generation(tree)).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let current = acyclic_fs::in_heap(|| {
            self.current
                .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        })
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(Error::from)?;
        // Erased: the restore's Send proof starts here, not in every caller's.
        let restore: acyclic_stream::BoxProviderFuture<'_, _> = acyclic_fs::in_heap(|| {
            self.current.restore_paths_from(
                &source,
                paths,
                current.id(),
                IdempotencyKey::from_bytes(operation_id.into_bytes()),
                self.permit,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
        });
        let outcome = restore
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        committed(outcome)
    }
}
