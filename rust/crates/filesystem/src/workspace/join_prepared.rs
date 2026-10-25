use super::*;
use crate::facade::{FsWork, WorkspaceJoinPreparation};
use crate::kernel::Owned;
use crate::{ObjectId, ObjectKind};

/// A single merge-planning result. Its immutable candidate and publication
/// preconditions cannot be replaced by the caller. Conflicts retain their
/// original allocation until the result is consumed or dropped.
///
/// ```compile_fail
/// use acyclic_fs::{Workspace, PreparedJoin};
/// fn replace<A, O>(prepared: &mut PreparedJoin<A, O>, other: Workspace<A, O>) {
///     prepared.target = other;
/// }
/// ```
pub struct PreparedJoin<A, O> {
    target: Workspace<A, O>,
    preparation: WorkspaceJoinPreparation,
    origin: KernelOwnedAllocation,
}

impl<A, O> PreparedJoin<A, O> {
    /// Returns the unpublished candidate, if planning completed without conflicts.
    pub fn candidate(&self) -> Option<GenerationId> {
        match &self.preparation {
            WorkspaceJoinPreparation::Ready(prepared) => Some(GenerationId::new(
                prepared.value.request.generation_root.digest,
            )),
            WorkspaceJoinPreparation::Terminal(_)
            | WorkspaceJoinPreparation::Pending(_)
            | WorkspaceJoinPreparation::Unchanged(_) => None,
        }
    }

    /// Borrows the complete bounded conflict frontier without copying it.
    pub fn conflicts(&self) -> Option<(&[MergeConflict], bool)> {
        match &self.preparation {
            WorkspaceJoinPreparation::Terminal(outcome) => match &outcome.value.value {
                crate::facade::WorkspaceJoinOutcome::Conflicted(conflicts, truncated) => {
                    Some((conflicts, *truncated))
                }
                _ => None,
            },
            WorkspaceJoinPreparation::Pending(pending) => Some(pending.value.merge.conflicts()),
            WorkspaceJoinPreparation::Ready(_) | WorkspaceJoinPreparation::Unchanged(_) => None,
        }
    }

    fn shares_origin(&self, accounting: &KernelAccounting<'_>) -> bool {
        match &self.origin {
            KernelOwnedAllocation::Standalone => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    accounting.native_work().is_none()
                }
                #[cfg(target_arch = "wasm32")]
                {
                    let _ = accounting;
                    true
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(allocation) => accounting
                .native_work()
                .is_some_and(|original| allocation.work().shares_ledger(original)),
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinPlan<A, O> {
    /// Prepares one immutable join without publishing a head or creating a preview fork.
    pub async fn prepare<W: FsWork>(
        &self,
        options: ApplyOptions,
        permit: crate::PublicationPermit,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<PreparedJoin<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if options.if_target != self.target_head.id {
                return Err(WorkspaceError::StaleTarget);
            }
            let origin = accounting.claim_owned(0, 0)?;
            let preparation = self
                .target_head
                .workspace
                .volume
                .fs
                .prepare_workspace_join_with_accounting::<WorkspaceError>(
                    crate::facade::WorkspaceJoinRequest {
                        target: &self.target_head.workspace.volume,
                        base: self.base,
                        source: &self.source_head,
                        expected_target: options.if_target,
                        expected_head: self.target_authority_head,
                        history: self.history,
                        operation_id: options.idempotency_key.operation_id(),
                        permit,
                        maximum_generations: self.maximum_generations,
                        maximum_changes: self.maximum_changes,
                        maximum_conflicts: self.maximum_conflicts,
                        resolutions: crate::facade::WorkspaceJoinResolutions::Map(&BTreeMap::new()),
                    },
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok(PreparedJoin {
                target: self.target_head.workspace.clone(),
                preparation,
                origin,
            })
        }
        .await;
        work.finish(result, &accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PreparedJoin<A, O> {
    /// Resolves a complete retained frontier on its original work owner. Missing
    /// choices keep it pending; accepted work is never planned a second time.
    pub async fn resolve<W: FsWork>(
        self,
        resolutions: &BTreeMap<MergeConflict, crate::kernel::MergeConflictResolution>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        self.resolve_with(
            crate::facade::WorkspaceJoinResolutions::Map(resolutions),
            work,
            cancellation,
        )
        .await
    }

    /// Resolves a sorted exact frontier without allocating a map or copying its keys.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resolve_slice<W: FsWork>(
        self,
        resolutions: &[(MergeConflict, crate::kernel::MergeConflictResolution)],
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        self.resolve_with(
            crate::facade::WorkspaceJoinResolutions::Slice(resolutions),
            work,
            cancellation,
        )
        .await
    }

    async fn resolve_with<W: FsWork>(
        self,
        resolutions: crate::facade::WorkspaceJoinResolutions<'_>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if !self.shares_origin(&accounting) {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            self.preparation.admit_retained(&mut accounting)?;
            let preparation = self
                .target
                .volume
                .fs
                .resolve_workspace_join_with_accounting(
                    &self.target.volume,
                    self.preparation,
                    resolutions,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok(Self {
                target: self.target,
                preparation,
                origin: self.origin,
            })
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Durably retains the exact candidate through the existing retention
    /// journal. The returned descriptor is authenticated again on resume.
    pub async fn checkpoint<W: FsWork>(
        &self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::PreparedJoinCheckpoint, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if !self.shares_origin(&accounting) {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            self.preparation.admit_retained(&mut accounting)?;
            let WorkspaceJoinPreparation::Ready(prepared) = &self.preparation else {
                return self
                    .target
                    .volume
                    .fs
                    .checkpoint_unpublished_join(
                        &self.target.volume,
                        &self.preparation,
                        &mut accounting,
                        cancellation,
                    )
                    .await;
            };
            let prepared = &prepared.value;
            if prepared.intent.original_evidence.is_some()
                || prepared.intent.fingerprint_permit.is_some()
            {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            let checkpoint = crate::PreparedJoinCheckpoint {
                target: self.target.id(),
                expected: prepared.request.expected,
                operation: prepared.request.operation_id,
                candidate: GenerationId::new(prepared.request.generation_root.digest),
                context: prepared
                    .intent
                    .operation_context
                    .ok_or(WorkspaceError::IncompatibleWorkspace)?,
                resolutions: prepared.resolutions_digest,
                permit: prepared.intent.permit,
                join: prepared.context,
                state: crate::facade::PreparedJoinCheckpointState::Candidate,
            };
            let label = checkpoint.label(&mut accounting)?;
            self.target
                .volume
                .fs
                .update_workspace_retention(
                    &self.target.volume,
                    checkpoint.candidate,
                    crate::kernel::RetentionKind::PreparedJoin,
                    label,
                    crate::facade::RetentionUpdate::Retain(None),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok(checkpoint)
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Publishes the retained candidate through the canonical authority CAS.
    /// This consumes no merge diff and never re-enters the merge planner.
    pub async fn publish(
        self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<JoinOutcome<A, O>>, WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            if !matches!(&self.origin, KernelOwnedAllocation::Standalone) {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            self.preparation.admit_retained(&mut accounting)?;
            let target = &self.target.volume;
            let fs = &target.fs;
            let output = match self.preparation {
                preparation @ (WorkspaceJoinPreparation::Terminal(_)
                | WorkspaceJoinPreparation::Pending(_)
                | WorkspaceJoinPreparation::Unchanged(_)) => {
                    preparation
                        .into_terminal(target, &mut accounting, cancellation)
                        .await?
                }
                WorkspaceJoinPreparation::Ready(prepared) => {
                    fs.publish_prepared_workspace_join_with_accounting(
                        target,
                        prepared,
                        &mut accounting,
                        cancellation,
                        |request, intent, limits, allowance| async move {
                            crate::kernel::publish_generation(
                                fs.objects(),
                                fs.authority(),
                                request,
                                intent,
                                limits,
                                allowance,
                                cancellation,
                            )
                            .await
                            .map(|receipt| crate::OperationReceipt {
                                value: crate::facade::checkout_commit_outcome(&receipt),
                                work: receipt.work,
                            })
                            .map_err(|failure| {
                                failure
                                    .map_error(|error| WorkspaceError::from(FsError::from(error)))
                            })
                        },
                    )
                    .await?
                }
            };
            self.target.map_join_outcome(output.value)
        }
        .await;
        budget.finish(result, &accounting)
    }

    /// Publishes on the original native invocation with its live owner seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn publish_with_native_work<F, Fut, E>(
        self,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        match &self.origin {
            KernelOwnedAllocation::NativeOriginal(allocation)
                if allocation.work().shares_ledger(original) => {}
            _ => return Err(WorkspaceError::IncompatibleWorkspace.into()),
        }
        let mut accounting = KernelAccounting::native(original, cancellation);
        let target = &self.target.volume;
        let fs = &target.fs;
        let output = match self.preparation {
            preparation @ (WorkspaceJoinPreparation::Terminal(_)
            | WorkspaceJoinPreparation::Pending(_)
            | WorkspaceJoinPreparation::Unchanged(_)) => {
                preparation
                    .into_terminal(target, &mut accounting, cancellation)
                    .await?
            }
            WorkspaceJoinPreparation::Ready(prepared) => {
                fs.publish_prepared_workspace_join_with_accounting(
                    target,
                    prepared,
                    &mut accounting,
                    cancellation,
                    |request, intent, limits, _| {
                        fs.publish_join_generation_with_native_work(
                            request,
                            intent,
                            limits,
                            original,
                            cancellation,
                            before_publication,
                        )
                    },
                )
                .await?
            }
        };
        let Owned {
            value, allocation, ..
        } = output;
        let allocation = match allocation {
            KernelOwnedAllocation::NativeOriginal(allocation) => allocation,
            KernelOwnedAllocation::Standalone => {
                return Err(WorkspaceError::IncompatibleWorkspace.into());
            }
        };
        Ok((self.target.map_join_outcome(value)?, allocation))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Reopens an SDK-issued candidate after restart without a join plan or
    /// another merge diff. A forged, retired or foreign descriptor fails closed.
    pub async fn resume_prepared_join<W: FsWork>(
        &self,
        checkpoint: &crate::PreparedJoinCheckpoint,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<PreparedJoin<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let fs = &self.volume.fs;
            fs.authenticate_join_checkpoint(
                &self.volume,
                checkpoint,
                &mut accounting,
                cancellation,
            )
            .await?;
            let origin = accounting.claim_owned(0, 0)?;
            if checkpoint.state == crate::facade::PreparedJoinCheckpointState::Unchanged {
                return Ok(PreparedJoin {
                    target: self.clone(),
                    origin,
                    preparation: WorkspaceJoinPreparation::unchanged(
                        crate::facade::WorkspaceJoinPublication {
                            context: checkpoint.join,
                            expected_head: checkpoint.expected,
                            operation: checkpoint.operation,
                            permit: checkpoint.permit,
                            resolutions_digest: checkpoint.resolutions,
                        },
                        &mut accounting,
                    )?,
                });
            }
            if checkpoint.state.program().is_some() {
                let preparation = fs
                    .resume_pending_join(&self.volume, checkpoint, &mut accounting, cancellation)
                    .await?;
                return Ok(PreparedJoin {
                    target: self.clone(),
                    preparation,
                    origin,
                });
            }
            let root = ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: checkpoint.candidate.digest(),
            };
            let durable = fs
                .make_generation_durable_with_accounting(
                    &self.volume,
                    checkpoint.candidate,
                    &[],
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let hold = crate::collection::Collection::hold_generation_with_accounting(
                fs.objects().collection(),
                root,
                self.volume.config,
                &mut accounting,
            )?;
            drop(durable);
            Ok(PreparedJoin {
                target: self.clone(),
                origin,
                preparation: WorkspaceJoinPreparation::ready(
                    crate::facade::PreparedWorkspaceJoin {
                        context: checkpoint.join,
                        request: crate::kernel::PublishGenerationRequest {
                            authority_id: self.authority_id(),
                            volume_id: self.volume.id,
                            epoch: checkpoint.expected.epoch,
                            expected: checkpoint.expected,
                            operation_id: checkpoint.operation,
                            generation_root: root,
                        },
                        intent: crate::kernel::PublicationIntent::guarded(
                            Some(checkpoint.context),
                            checkpoint.permit,
                        ),
                        limits: crate::facade::closure_limits(self.volume.config),
                        resolutions_digest: checkpoint.resolutions,
                        _candidate_hold: hold,
                    },
                    &mut accounting,
                )?,
            })
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Retires one exact preparation. Repeated retirement is idempotent, while
    /// the terminal journal receipt prevents the descriptor from being revived.
    pub async fn release_prepared_join<W: FsWork>(
        &self,
        checkpoint: &crate::PreparedJoinCheckpoint,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if checkpoint.target != self.id() {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let label = checkpoint.label(&mut accounting)?;
            self.volume
                .fs
                .update_workspace_retention(
                    &self.volume,
                    checkpoint.candidate,
                    crate::kernel::RetentionKind::PreparedJoin,
                    label,
                    crate::facade::RetentionUpdate::Release,
                    &mut accounting,
                    cancellation,
                )
                .await
        }
        .await;
        work.finish(result, &accounting)
    }
}
