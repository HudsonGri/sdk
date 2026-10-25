use super::*;

impl<A, O, R> MultiRootPublisher for WorkspaceMultiRootPublisher<A, O, R>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: WorkspaceResolver<A, O>,
{
    type Error = WorkspaceMultiRootPublisherError<R::Error>;

    async fn prepare(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> Result<MultiRootPrepare, Self::Error> {
        let cancellation = CancellationToken::new();
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let head = target
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        if head.id() != root.target_generation {
            return Ok(MultiRootPrepare::Stale);
        }
        let plan = self.pinned_plan(root, Some(target.clone())).await?;
        let reservation = self
            .reserve_operation(
                &target,
                operation_id,
                "publication target changed while being fenced",
            )
            .await?;
        let result = async {
            let (prepared, conflict) = self
                .prepare_plan(
                    root_id,
                    root,
                    &plan,
                    publisher_key(operation_id, root_id, b"publish"),
                    resolutions,
                    PublicationPermit::Reservation {
                        operation_id: operation_id.into_bytes(),
                        gate_tail: reservation.gate_tail,
                        expected: reservation.expected,
                    },
                )
                .await?;
            let checkpoint = Some(
                prepared
                    .checkpoint(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            );
            let token = serde_json::to_vec(&WorkspacePublicationFence {
                root_id,
                target_workspace_id: target.id(),
                reservation,
                checkpoint,
            })
            .map_err(|error| WorkspaceMultiRootPublisherError::Candidate(error.to_string()))?;
            let fence = MultiRootFence { token };
            Ok(match conflict {
                Some(plan) => MultiRootPrepare::Conflicted { plan, fence },
                None => MultiRootPrepare::Ready(fence),
            })
        }
        .await;
        if !matches!(
            &result,
            Ok(MultiRootPrepare::Ready(_) | MultiRootPrepare::Conflicted { .. })
        ) {
            self.release_operation_reservation(&target, operation_id)
                .await?;
        }
        result
    }

    async fn release(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        fence: &MultiRootFence,
    ) -> Result<(), Self::Error> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let decoded = decode_reservation(operation_id, root_id, fence)?;
        let target = self
            .resolver
            .resolve(decoded.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        if let Some(checkpoint) = decoded.checkpoint {
            target
                .release_prepared_join(&checkpoint, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| failure.error)?;
        }
        target
            .volume
            .fs
            .authority()
            .release_publication(decoded.reservation, &mut authority_work)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(())
    }

    async fn project_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        fence: &MultiRootFence,
    ) -> Result<GenerationId, Self::Error> {
        match self
            .publish(operation_id, root_id, root, &BTreeMap::new(), fence)
            .await?
        {
            MultiRootPublishRoot::Published(generation)
            | MultiRootPublishRoot::AlreadyPublished(generation) => Ok(generation),
            MultiRootPublishRoot::UnexpectedTarget => {
                Err(WorkspaceMultiRootPublisherError::Candidate(
                    "conflict projection target changed".to_owned(),
                ))
            }
        }
    }

    async fn abort_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
        conflicts: Option<&crate::MergePlan>,
    ) -> Result<GenerationId, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let key = publisher_key(operation_id, root_id, b"conflict-abort");
        let reservation_operation =
            conflict_abort_materialization_operation_id(operation_id, root_id);
        if let Some(generation) = target
            .operation_generation(key, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
        {
            self.release_operation_reservation(&target, reservation_operation)
                .await?;
            return Ok(generation.id());
        }
        let current = target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let original = target
            .generation(
                root.target_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let projected = target
            .generation(
                projected_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let changed = original
            .diff_to(
                &projected,
                u32::MAX,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value
            .0
            .changed_paths(u32::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .0;
        let mut paths = changed
            .iter()
            .map(|change| {
                portable_namespace_path(&change.path).ok_or_else(|| {
                    WorkspaceMultiRootPublisherError::Candidate(
                        "conflict abort cannot represent a non-portable path".to_owned(),
                    )
                })
            })
            // A held conflict can have no projected delta. Its user's later
            // resolution still belongs to this merge and must be undone.
            .chain(conflicts.into_iter().flat_map(|plan| plan.conflicts.iter()).map(|conflict| {
                conflict.path.as_ref().map(|path| format!("/{}", path.trim_start_matches('/'))).ok_or_else(|| {
                    WorkspaceMultiRootPublisherError::Candidate(
                        "conflict abort cannot represent an unresolved identity without a portable path".to_owned(),
                    )
                })
            }))
            .collect::<Result<Vec<_>, _>>()?;
        paths.sort_unstable();
        paths.dedup();
        if paths.is_empty() {
            // Neither the projection nor its durable conflict set owns a path.
            return Ok(current.id());
        }
        let reservation = self
            .reserve_operation(
                &target,
                reservation_operation,
                "conflict abort target changed while being fenced",
            )
            .await?;
        let outcome = target
            .apply_paths_from(
                // Abort explicitly undoes the projected paths, including any
                // conflict resolutions authored there. Use the fenced current
                // head as the comparison base; the immutable projection above
                // determines which paths belong to this operation, so later
                // edits at unrelated paths remain outside the inverse delta.
                Some(&current),
                Some(&original),
                &paths,
                current.id(),
                key,
                PublicationPermit::Reservation {
                    operation_id: reservation_operation.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: reservation.expected,
                },
            )
            .await;
        let release = target
            .volume
            .fs
            .authority()
            .release_publication(reservation, &mut authority_work)
            .await
            .map_err(WorkspaceError::engine);
        if let Err(error) = release {
            return Err(error.into());
        }
        let outcome = outcome?;
        match outcome {
            crate::WorkspacePathApply::Applied(generation)
            | crate::WorkspacePathApply::AlreadyApplied(generation)
            | crate::WorkspacePathApply::NoChanges(generation) => Ok(generation.id()),
            crate::WorkspacePathApply::Conflicted(_)
            | crate::WorkspacePathApply::Stale(_)
            | crate::WorkspacePathApply::Fenced
            | crate::WorkspacePathApply::IdempotencyConflict => {
                Err(WorkspaceMultiRootPublisherError::Candidate(
                    "conflict abort overlaps later workspace changes".to_owned(),
                ))
            }
        }
    }

    async fn declare_conflicts(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: &crate::MergePlan,
        paths: &[String],
    ) -> Result<BTreeSet<ConflictKey>, Self::Error> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let current = target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let normalized = paths
            .iter()
            .map(|path| path.trim_start_matches('/').trim_end_matches('/'))
            .collect::<Vec<_>>();
        let declares_all = normalized
            .iter()
            .any(|candidate| *candidate == "." || candidate.is_empty());
        let mut declared = BTreeSet::new();
        for view in &conflict.conflicts {
            let Some(raw_path) = view.path.as_deref() else {
                if declares_all {
                    declared.insert(view.key.clone());
                }
                continue;
            };
            let path = raw_path.trim_start_matches('/');
            if !normalized.iter().any(|candidate| {
                *candidate == "."
                    || *candidate == path
                    || (!candidate.is_empty()
                        && path
                            .strip_prefix(*candidate)
                            .is_some_and(|suffix| suffix.starts_with('/')))
            }) {
                continue;
            }
            if view.kind == crate::ConflictKind::Text {
                match current
                    .read(
                        &format!("/{path}"),
                        64 * 1024 * 1024,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                {
                    Ok(bytes) => {
                        let text = std::str::from_utf8(&bytes).map_err(|_| {
                            WorkspaceMultiRootPublisherError::Candidate(format!(
                                "text conflict {path} is no longer UTF-8"
                            ))
                        })?;
                        if crate::text_merge::has_conflict_markers(text) {
                            return Err(WorkspaceMultiRootPublisherError::Candidate(format!(
                                "text conflict {path} still contains conflict markers"
                            )));
                        }
                    }
                    Err(WorkspaceError::NotFound) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            declared.insert(view.key.clone());
        }
        Ok(declared)
    }

    async fn finish_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<MultiRootConflictFinish, Self::Error> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let current = target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        if !generation_descends_from(&target, current.id(), projected_generation).await? {
            return Err(WorkspaceMultiRootPublisherError::Candidate(
                "resolved conflict generation does not descend from its projection".to_owned(),
            ));
        }
        let reservation_operation = conflict_finish_operation_id(operation_id, root_id);
        let reservation = self
            .reserve_operation(
                &target,
                reservation_operation,
                "resolved conflict generation changed while being fenced",
            )
            .await?;
        Ok(MultiRootConflictFinish {
            generation: current.id(),
            fence: MultiRootFence {
                token: serde_json::to_vec(&WorkspacePublicationFence {
                    root_id,
                    target_workspace_id: target.id(),
                    reservation,
                    checkpoint: None,
                })
                .map_err(|error| WorkspaceMultiRootPublisherError::Candidate(error.to_string()))?,
            },
        })
    }

    async fn publish(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        _resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        fence: &MultiRootFence,
    ) -> Result<MultiRootPublishRoot, Self::Error> {
        let cancellation = CancellationToken::new();
        let decoded = decode_reservation(operation_id, root_id, fence)?;
        if decoded.target_workspace_id != root.target_workspace_id
            || decoded.checkpoint.is_some_and(|checkpoint| {
                checkpoint.join.source_workspace
                    != root
                        .merge_workspace_id
                        .unwrap_or(root.source_workspace_id)
                        .volume_id()
                    || checkpoint.join.source_generation != root.source_generation
                    || checkpoint.join.expected_target != root.target_generation
                    || checkpoint.join.base.id != root.base_generation
            })
        {
            return Err(WorkspaceMultiRootPublisherError::InvalidFence);
        }
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let checkpoint = decoded
            .checkpoint
            .ok_or(WorkspaceMultiRootPublisherError::InvalidFence)?;
        if decoded.reservation.authority_id != target.authority_id()
            || checkpoint.target != target.id()
            || checkpoint.expected != decoded.reservation.expected
        {
            return Err(WorkspaceMultiRootPublisherError::InvalidFence);
        }
        // Replayed completion is authorized by the same retained descriptor as
        // the first publication, even when the target head has already moved.
        let prepared = target
            .resume_prepared_join(&checkpoint, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        if target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .id()
            != root.target_generation
        {
            if let Some(generation) = target
                .operation_generation(
                    publisher_key(operation_id, root_id, b"publish"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?
                .value
            {
                return Ok(MultiRootPublishRoot::AlreadyPublished(generation.id()));
            }
            return Ok(MultiRootPublishRoot::UnexpectedTarget);
        }
        let outcome = match prepared
            .publish(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
        {
            JoinOutcome::Applied(generation) => {
                MultiRootPublishRoot::Published(generation.generation.id())
            }
            JoinOutcome::NoChanges(generation) => MultiRootPublishRoot::Published(generation.id()),
            JoinOutcome::AlreadyApplied(generation) => {
                MultiRootPublishRoot::AlreadyPublished(generation.generation.id())
            }
            _ => MultiRootPublishRoot::UnexpectedTarget,
        };
        Ok(outcome)
    }
}
