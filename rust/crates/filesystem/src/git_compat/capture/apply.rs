use super::*;

pub(super) async fn apply_git_capture<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    S: crate::WorkspaceLineageStore,
>(
    graph: &crate::WorkspaceGraph<S>,
    parent: &Generation<A, O>,
    base: Option<&Generation<A, O>>,
    source: Option<&Generation<A, O>>,
    paths: &[String],
    tracked_paths: BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureAuthenticationError<S::Error>> {
    let bytes = operation_id.into_bytes();
    let workspace_hash = hex::encode(&parent.workspace_id().into_bytes()[..6]);
    let destination = format!("git-capture-{workspace_hash}-{}", hex::encode(bytes));
    let source_workspace = parent.workspace();
    let key = IdempotencyKey::from_bytes(bytes);
    let capture = graph
        .fork(
            &source_workspace,
            &destination,
            key,
            Some(parent.id()),
            |generation| {
                let source_workspace = &source_workspace;
                let destination = &destination;
                async move {
                    source_workspace
                        .fork::<_, _, WorkspaceError>(
                            &destination,
                            crate::ForkOptions::from_generation(generation, key),
                            crate::PublicationPermit::Unrestricted,
                            WorkBudget::UNBOUNDED,
                            &CancellationToken::new(),
                            (),
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| {
                            GitCaptureAuthenticationError::<S::Error>::from(failure.error)
                        })
                }
            },
            |child| child,
        )
        .await?;
    let mut apply_bytes = bytes;
    apply_bytes[0] ^= 0xa5;
    let apply_key = IdempotencyKey::from_bytes(apply_bytes);
    let replay = capture
        .operation_generation(apply_key, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let (generation, initial_generation) = if let Some(generation) = replay {
        let parents = generation
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let [initial_generation] = parents.as_slice() else {
            return Err(GitCaptureError::Conflict.into());
        };
        (generation, *initial_generation)
    } else {
        let head = capture
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        match capture
            .apply_paths_from(
                base,
                source,
                paths,
                head.id(),
                apply_key,
                crate::PublicationPermit::Unrestricted,
            )
            .await?
        {
            crate::WorkspacePathApply::Applied(generation)
            | crate::WorkspacePathApply::NoChanges(generation) => (generation, head.id()),
            crate::WorkspacePathApply::AlreadyApplied(generation) => {
                let parents = generation
                    .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)?;
                let [initial_generation] = parents.as_slice() else {
                    return Err(GitCaptureError::Conflict.into());
                };
                (generation, *initial_generation)
            }
            crate::WorkspacePathApply::Conflicted(_)
            | crate::WorkspacePathApply::Stale(_)
            | crate::WorkspacePathApply::Fenced
            | crate::WorkspacePathApply::IdempotencyConflict => {
                return Err(GitCaptureError::Conflict.into());
            }
        }
    };
    generation
        .pin(
            format!("capture-{}", hex::encode(bytes)),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    Ok(GitCapturedGeneration {
        generation,
        tracked_paths,
        origin: Some((parent.clone(), initial_generation)),
    })
}
