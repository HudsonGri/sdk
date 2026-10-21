//! Project text markers and retain other conflicting entries for manual edits.
use super::*;

type LocalJoinPlan = acyclic_fs::JoinPlan<LocalAuthorityBackend, LocalObjectBackend>;
type LocalJoinOutcome = JoinOutcome<LocalAuthorityBackend, LocalObjectBackend>;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    // Each phase of a join runs in a heap frame of its own, awaited in turn by
    // this driver. No phase's temporaries share a frame with another's, so the
    // deepest await (preparing the merge candidate) sits under frames holding
    // only their own phase, also in builds without cross-unit inlining.
    pub(super) async fn join(
        &self,
        operation: OperationId,
        source: acyclic_fs::WorkspaceId,
        rebase: bool,
        tracked: &BTreeSet<String>,
    ) -> crate::Result<GitFilesystemResult> {
        let plan = acyclic_fs::in_heap(|| self.join_plan(source, rebase)).await?;
        let options = ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::from_bytes(operation.into_bytes()),
        };
        let initial =
            acyclic_fs::in_heap(|| plan.apply(options, BTreeMap::new(), self.permit)).await?;
        let (outcome, unresolved) = match initial {
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => {
                // Erased: the conflict phases' Send proof starts here, not in
                // every caller's.
                let conflicted: acyclic_stream::BoxProviderFuture<'_, _> =
                    acyclic_fs::in_heap(|| {
                        self.join_conflicts(operation, &plan, options, conflicts, truncated)
                    });
                conflicted.await?
            }
            initial => (initial, false),
        };
        acyclic_fs::in_heap(|| self.joined(outcome, unresolved, tracked)).await
    }

    async fn join_plan(
        &self,
        source: acyclic_fs::WorkspaceId,
        rebase: bool,
    ) -> crate::Result<LocalJoinPlan> {
        let source = acyclic_fs::in_heap(|| self.workspace(source)).await?;
        let mut builder = source.join_into(&self.current).git_metadata();
        if rebase {
            builder = builder.history(acyclic_fs::JoinHistory::Rebase);
        }
        let validate = || self.validate();
        acyclic_fs::in_heap(|| {
            super::materialize::prepared(
                async {
                    builder
                        .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| Error::from(failure.error))
                },
                &validate,
            )
        })
        .await
    }

    /// Resolves a conflicted join's entries and applies the resolved
    /// candidate; the flag reports entries left for manual edits.
    async fn join_conflicts(
        &self,
        operation: OperationId,
        plan: &LocalJoinPlan,
        options: ApplyOptions,
        conflicts: Vec<acyclic_fs::MergeConflict>,
        truncated: bool,
    ) -> crate::Result<(LocalJoinOutcome, bool)> {
        let typed = acyclic_fs::in_heap(|| plan.describe_conflicts(conflicts, truncated)).await?;
        if typed.truncated {
            return Err(Self::error("Git join conflict frontier is incomplete"));
        }
        let paths = conflict_paths(&typed)?;
        let validate = || self.validate();
        let (candidate, unresolved) = acyclic_fs::in_heap(|| {
            super::materialize::prepared(self.merge_candidate(typed), &validate)
        })
        .await?;
        let resolutions = acyclic_fs::in_heap(|| {
            super::materialize::prepared(
                async {
                    plan.prepare_candidate(&candidate)
                        .await
                        .map_err(Error::from)
                },
                &validate,
            )
        })
        .await?;
        let outcome = acyclic_fs::in_heap(|| plan.apply(options, resolutions, self.permit)).await?;
        if matches!(
            &outcome,
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) | JoinOutcome::NoChanges(_)
        ) {
            // Only an applied projection can authorize continuation.
            // An interruption before this durable write leaves the
            // existing transition uninspected and continuation refuses.
            let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
            acyclic_fs::in_heap(|| repository.record_merge_conflicts(operation, &paths))
                .await
                .map_err(super::errors::compat_error)?;
        }
        Ok((outcome, unresolved))
    }

    /// Resolves every typed conflict with the configured drivers. An entry no
    /// driver resolves keeps one side and is reported as unresolved.
    async fn merge_candidate(
        &self,
        typed: acyclic_fs::MergePlan,
    ) -> crate::Result<(acyclic_fs::UnpublishedMergeCandidate, bool)> {
        let mut cache = self.merge_cache.lock().await;
        let mut unresolved = false;
        let mut resolutions = BTreeMap::new();
        for conflict in &typed.conflicts {
            let resolution = match acyclic_fs::resolve_merge_conflict(
                &typed,
                conflict,
                &self.merge_drivers,
                &mut *cache,
                false,
            ) {
                Ok(resolution)
                    if !matches!(resolution, acyclic_fs::MergeResolution::Unresolved) =>
                {
                    resolution
                }
                Ok(_)
                | Err(
                    acyclic_fs::MergePlanResolutionError::MissingDriver(_)
                    | acyclic_fs::MergePlanResolutionError::Driver(
                        acyclic_fs::DriverError::Unsupported,
                    ),
                ) => {
                    unresolved = true;
                    acyclic_fs::MergeResolution::Select(
                        if matches!(conflict.ours, acyclic_fs::ConflictValue::Absent) {
                            acyclic_fs::ConflictSide::Theirs
                        } else {
                            acyclic_fs::ConflictSide::Ours
                        },
                    )
                }
                Err(error) => {
                    return Err(Error::from(acyclic_fs::DrivenJoinError::Resolution(error)));
                }
            };
            unresolved |= matches!(&resolution, acyclic_fs::MergeResolution::Text(text)
                if acyclic_fs::text_merge::has_conflict_markers(text));
            resolutions.insert(conflict.key.clone(), resolution);
        }
        Ok((
            acyclic_fs::UnpublishedMergeCandidate {
                plan: typed,
                resolutions,
            },
            unresolved,
        ))
    }

    /// The command result of a finished join.
    async fn joined(
        &self,
        outcome: LocalJoinOutcome,
        unresolved: bool,
        tracked: &BTreeSet<String>,
    ) -> crate::Result<GitFilesystemResult> {
        let generation = match outcome {
            JoinOutcome::Applied(application) | JoinOutcome::AlreadyApplied(application) => {
                application.generation
            }
            JoinOutcome::NoChanges(generation) => generation,
            JoinOutcome::StaleTarget(_) => {
                return Err(Self::error("Git join raced with another workspace writer"));
            }
            JoinOutcome::Conflicted { .. } => {
                return Err(Self::error("Git join has unresolved entries"));
            }
            JoinOutcome::Fenced => return Err(Self::error("Git join was fenced")),
            JoinOutcome::IdempotencyConflict => {
                return Err(Self::error("Git join retry identity was reused"));
            }
        };
        if unresolved {
            return Err(Self::error(
                "Git join has unresolved conflicts. Edit the conflicted paths, then run `fs merge --continue`, or `fs merge --abort`",
            ));
        }
        let tracked_paths =
            acyclic_fs::in_heap(|| existing_tracked_paths(&generation, tracked)).await?;
        Ok(GitFilesystemResult::Applied {
            tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
            tracked_paths: Some(tracked_paths),
        })
    }
}

/// Each conflict's path and, for a conflict bound to a file, its identity.
fn conflict_paths(
    typed: &acyclic_fs::MergePlan,
) -> crate::Result<BTreeMap<String, Option<acyclic_fs::FileId>>> {
    typed
        .conflicts
        .iter()
        .map(|conflict| {
            let path = conflict.path.clone().ok_or_else(|| {
                Error::Conflict("Git join conflict has no resolvable path".into())
            })?;
            use acyclic_fs::ConflictKey;
            let identity = match &conflict.key {
                ConflictKey::File(id)
                | ConflictKey::Metadata(id)
                | ConflictKey::HardLinks(id)
                | ConflictKey::Directory(id)
                | ConflictKey::SpecialPayload(id)
                | ConflictKey::ContentRange { file_id: id, .. }
                | ConflictKey::Rename { file_id: id, .. } => Some(*id),
                ConflictKey::Binding { .. } => None,
            };
            Ok((path, identity))
        })
        .collect()
}
