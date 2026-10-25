use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn apply_workspace_join(
        &self,
        request: WorkspaceJoinRequest<'_, A, O>,
    ) -> Result<WorkspaceJoinOutcome, WorkspaceError> {
        let token = CancellationToken::new();
        let cancellation = &token;
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
        let output = self
            .apply_workspace_join_with_accounting(
                request,
                &mut accounting,
                cancellation,
                |request, intent, limits, budget| async move {
                    publish_generation(
                        &self.inner.objects,
                        &self.inner.authority,
                        request,
                        intent,
                        limits,
                        budget,
                        cancellation,
                    )
                    .await
                    .map(|receipt| OperationReceipt {
                        value: checkout_commit_outcome(&receipt),
                        work: receipt.work,
                    })
                    .map_err(|failure| {
                        failure.map_error(|error| WorkspaceError::from(FsError::from(error)))
                    })
                },
            )
            .await?;
        Ok(output.value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn apply_workspace_join_guarded_with_native_work<'original, F, Fut, E>(
        &self,
        request: WorkspaceJoinRequest<'_, A, O>,
        original: &'original NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<(WorkspaceJoinOutcome, NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        let mut accounting = KernelAccounting::native(original, cancellation);
        let output = self
            .apply_workspace_join_with_accounting(
                request,
                &mut accounting,
                cancellation,
                |request, intent, limits, _budget| {
                    self.publish_join_generation_with_native_work(
                        request,
                        intent,
                        limits,
                        original,
                        cancellation,
                        before_publication,
                    )
                },
            )
            .await?;
        match output.allocation {
            KernelOwnedAllocation::NativeOriginal(allocation) => Ok((output.value, allocation)),
            KernelOwnedAllocation::Standalone => {
                unreachable!("native join retains original output custody")
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn publish_join_generation_with_native_work<F, Fut, E>(
        &self,
        request: PublishGenerationRequest,
        intent: PublicationIntent,
        limits: ClosureLimits,
        original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> MeasuredResult<OperationReceipt<LivePublicationObservation>, E>
    where
        F: for<'materialization> FnMut(&'materialization NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        let (receipt, allocation) = publish_generation_async_guarded_with_native_work(
            &self.inner.objects,
            &self.inner.authority,
            request,
            intent,
            limits,
            None,
            original,
            cancellation,
            before_publication,
            None,
            None,
        )
        .await
        .map_err(|error| OperationFailure::new(error, original.receipt()))?;
        let result = OperationReceipt {
            value: checkout_commit_outcome(&receipt),
            work: receipt.work,
        };
        drop(receipt);
        drop(allocation);
        Ok(result)
    }

    pub(super) async fn apply_workspace_join_with_accounting<E, Fut>(
        &self,
        request: WorkspaceJoinRequest<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        publication: impl FnOnce(
            PublishGenerationRequest,
            PublicationIntent,
            ClosureLimits,
            WorkBudget,
        ) -> Fut
        + ProviderTask,
    ) -> Result<Owned<WorkspaceJoinOutcome>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send,
        Fut: Future<Output = MeasuredResult<OperationReceipt<LivePublicationObservation>, E>>
            + ProviderTask,
    {
        let target = request.target;
        let preparation = self
            .prepare_workspace_join_with_accounting::<E>(request, accounting, cancellation)
            .await?;
        let prepared = match preparation {
            WorkspaceJoinPreparation::Ready(prepared) => prepared,
            preparation => {
                return preparation
                    .into_terminal(target, accounting, cancellation)
                    .await
                    .map_err(E::from);
            }
        };
        self.publish_prepared_workspace_join_with_accounting(
            target,
            prepared,
            accounting,
            cancellation,
            publication,
        )
        .await
    }

    pub(crate) async fn publish_prepared_workspace_join_with_accounting<E, Fut>(
        &self,
        target: &Volume<A, O>,
        prepared: Owned<Box<PreparedWorkspaceJoin>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
        publication: impl FnOnce(
            PublishGenerationRequest,
            PublicationIntent,
            ClosureLimits,
            WorkBudget,
        ) -> Fut
        + ProviderTask,
    ) -> Result<Owned<WorkspaceJoinOutcome>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send,
        Fut: Future<Output = MeasuredResult<OperationReceipt<LivePublicationObservation>, E>>
            + ProviderTask,
    {
        let prepared = prepared.unbox(accounting)?;
        let published = publication(
            prepared.request,
            prepared.intent,
            prepared.limits,
            accounting.remaining()?,
        )
        .await;
        if accounting.standalone_parts().is_some() {
            let spent = match &published {
                Ok(receipt) => receipt.work,
                Err(failure) => *failure.work,
            };
            merge_creation_backend_work(accounting, spent)?;
        }
        let outcome = match published.map_err(|failure| failure.error)?.value {
            LivePublicationObservation::Committed {
                generation_id: id, ..
            } => WorkspaceJoinOutcome::Joined(id, prepared.resolutions_digest),
            LivePublicationObservation::AlreadyCommitted {
                generation_id: id, ..
            } => WorkspaceJoinOutcome::AlreadyJoined(id, prepared.resolutions_digest),
            LivePublicationObservation::Conflict { .. } => WorkspaceJoinOutcome::Stale(
                self.workspace_head_state(target, accounting, cancellation)
                    .await?
                    .0,
            ),
            LivePublicationObservation::Fenced { .. } => WorkspaceJoinOutcome::Fenced,
            LivePublicationObservation::IdempotencyConflict { .. } => {
                WorkspaceJoinOutcome::IdempotencyConflict
            }
        };
        workspace_join_scalar(outcome, accounting).map_err(E::from)
    }

    pub(crate) async fn prepare_workspace_join_with_accounting<E>(
        &self,
        request: WorkspaceJoinRequest<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPreparation, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send,
    {
        accounting.check()?;
        let resolutions_digest = hash_join_resolutions(&request.resolutions, accounting)?;
        let context = request.context();
        let WorkspaceJoinRequest {
            target,
            base,
            source,
            expected_target,
            expected_head,
            history,
            operation_id,
            permit,
            maximum_generations,
            maximum_changes,
            maximum_conflicts,
            resolutions,
        } = request;
        if maximum_generations == 0 || maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(WorkspaceError::JoinLimit.into());
        }
        if !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || source.workspace.volume.config != target.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace.into());
        }
        let current_target = self
            .workspace_head_state(target, accounting, cancellation)
            .await?
            .0;
        let target_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: expected_target.digest(),
        };
        let base_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: base.id.digest(),
        };
        let source_object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: source.id.digest(),
        };
        let [mut base_root, mut source_root, target_root] = self
            .read_join_roots_with_accounting(
                [base_object, source_object, target_object],
                target.config,
                accounting,
                cancellation,
            )
            .await?;
        let publication = WorkspaceJoinPublication {
            context,
            expected_head,
            operation: operation_id,
            permit,
            resolutions_digest,
        };
        let result = async {
            if source_root.value.volume_id != source.workspace.volume.id
                || target_root.value.volume_id != target.id
                || base_root.value.root_file_id != target_root.value.root_file_id
                || source_root.value.root_file_id != target_root.value.root_file_id
            {
                return Err(WorkspaceError::IncompatibleWorkspace.into());
            }
            let normalized_base_object = self
                .normalize_join_root(&mut base_root.value, target.id, accounting, cancellation)
                .await?;
            let normalized_source_object = self
                .normalize_join_root(&mut source_root.value, target.id, accounting, cancellation)
                .await?;
            if normalized_source_object == target_object && current_target == expected_target {
                return WorkspaceJoinPreparation::unchanged(publication, accounting)
                    .map_err(E::from);
            }
            let merge_request = crate::kernel::MergeGenerationBorrowedRequest {
                base_generation: normalized_base_object,
                base: &base_root.value,
                ours_generation: Some(target_object),
                ours: &target_root.value,
                theirs_generation: normalized_source_object,
                theirs: &source_root.value,
                retain_theirs_parent: history == crate::workspace::JoinHistory::Merge,
                maximum_changes,
                maximum_conflicts,
                content: crate::kernel::ContentMerge::Extents,
            };
            let merged = resolutions
                .prepare_generation(
                    &self.inner.objects,
                    merge_request,
                    DecodeLimits::for_volume(target.config),
                    accounting,
                    cancellation,
                )
                .await?;
            self.finish_join_preparation(target, publication, merged, accounting, cancellation)
                .await
                .map_err(E::from)
        }
        .await;
        target_root.retire(accounting)?;
        source_root.retire(accounting)?;
        base_root.retire(accounting)?;
        result
    }

    async fn normalize_join_root(
        &self,
        root: &mut GenerationRoot,
        target: VolumeId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        // Normalize scalar identity in place; decoded parent vectors retain
        // their existing allocation owner until their enclosing root retires.
        root.required_features = root.continuation_features();
        root.volume_id = target;
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_generation_root(root, observe, allocation_error)
            })?;
        self.put_encoded_with_accounting(
            ObjectKind::GenerationRoot,
            encoded,
            accounting,
            cancellation,
        )
        .await
    }

    pub(super) async fn finish_join_preparation(
        &self,
        target: &Volume<A, O>,
        publication: WorkspaceJoinPublication,
        merged: crate::kernel::GenerationMergePreparation,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPreparation, WorkspaceError> {
        let (conflicts, truncated) = merged.conflicts();
        if !conflicts.is_empty() || truncated {
            return self
                .retain_pending_workspace_join(
                    PendingWorkspaceJoin {
                        merge: merged,
                        publication,
                        holds: Owned::allocate(0, accounting)?,
                    },
                    target.config,
                    accounting,
                    cancellation,
                )
                .await;
        }
        let [_, _, source] = merged.generations();
        let original_table = merged.original_table();
        let (outcome, allocation) = merged
            .finish_resolved(
                &self.inner.objects,
                DecodeLimits::for_volume(target.config),
                accounting,
                cancellation,
            )
            .await?;
        let MergeGenerationOutcome::Prepared {
            generation_root,
            root,
            ..
        } = outcome
        else {
            return Err(FsError::InvalidDiff.into());
        };
        let bytes = root_parent_bytes(&root)?;
        let unchanged = root.file_table == original_table;
        Owned::new(root, allocation, bytes).retire(accounting)?;
        if unchanged {
            let current = self
                .workspace_head_state(target, accounting, cancellation)
                .await?
                .0;
            if current != publication.context.expected_target {
                return WorkspaceJoinPreparation::terminal(
                    WorkspaceJoinOutcome::Stale(current),
                    accounting,
                );
            }
            return WorkspaceJoinPreparation::unchanged(publication, accounting)
                .map_err(Into::into);
        }
        self.prepare_join_publication(
            target,
            publication,
            generation_root,
            source,
            accounting,
            cancellation,
        )
        .await
    }

    pub(super) async fn prepare_join_publication(
        &self,
        target: &Volume<A, O>,
        publication: WorkspaceJoinPublication,
        candidate: ObjectId,
        normalized_source_object: ObjectId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPreparation, WorkspaceError> {
        // Use the canonical closure proof and collection gate for every
        // history mode. The unpublished root is live until the CAS settles.
        // Closure proofs omit history. Admit the newly normalized parent
        // in the same flush, before exposing the candidate to collection.
        let history_objects = if publication.context.history == crate::workspace::JoinHistory::Merge
        {
            std::slice::from_ref(&normalized_source_object)
        } else {
            &[]
        };
        let durable = self
            .make_generation_durable_with_accounting(
                target,
                GenerationId::new(candidate.digest),
                history_objects,
                accounting,
                cancellation,
            )
            .await?;
        // Transfer protection under the admission gate into the existing
        // root registry. Keeping a read gate across publication could
        // deadlock behind a queued collector's write gate.
        let _candidate_hold = crate::collection::Collection::hold_generation_with_accounting(
            self.inner.objects.collection(),
            candidate,
            target.config,
            accounting,
        )?;
        drop(durable);
        let context = publication
            .context
            .digest_with_accounting(publication.resolutions_digest, accounting)?;
        WorkspaceJoinPreparation::ready(
            PreparedWorkspaceJoin {
                context: publication.context,
                request: PublishGenerationRequest {
                    authority_id: volume_authority_id(target.id),
                    volume_id: target.id,
                    epoch: publication.expected_head.epoch,
                    expected: publication.expected_head,
                    operation_id: publication.operation,
                    generation_root: candidate,
                },
                intent: PublicationIntent::guarded(Some(context), publication.permit),
                limits: closure_limits(target.config),
                resolutions_digest: publication.resolutions_digest,
                _candidate_hold,
            },
            accounting,
        )
        .map_err(Into::into)
    }

    async fn read_join_roots_with_accounting(
        &self,
        objects: [ObjectId; 3],
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<[Owned<GenerationRoot>; 3], FsError> {
        let mut roots = [None, None, None];
        let result = async {
            for (slot, object) in roots.iter_mut().zip(objects) {
                *slot = Some(
                    self.read_join_root_with_accounting(object, config, accounting, cancellation)
                        .await?,
                );
            }
            Ok::<_, FsError>(())
        }
        .await;
        if let Err(error) = result {
            for root in roots.into_iter().flatten() {
                root.retire(accounting)?;
            }
            return Err(error);
        }
        match roots {
            [Some(base), Some(source), Some(target)] => Ok([base, source, target]),
            _ => unreachable!("all three roots installed before successful completion"),
        }
    }

    pub(super) async fn read_join_root_with_accounting(
        &self,
        object: ObjectId,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<GenerationRoot>, FsError> {
        let (root, allocation) = read_generation_root_with_accounting(
            &self.inner.objects,
            object,
            config,
            accounting,
            cancellation,
        )
        .await?;
        let mut root = Owned::new(root, allocation, 0);
        root.bytes = root_parent_bytes(&root.value)?;
        Ok(root)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Prepares one bounded two-parent merge against this checkout's immutable base.
    pub async fn prepare_merge(
        &mut self,
        theirs: GenerationId,
        maximum_changes: u32,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<MergePreparation> {
        if self.mode.access != AccessMode::ReadWrite
            || self.mode.mutations != MutationMode::PrivateOverlay
        {
            return Err(OperationFailure::before_work(FsError::MutationNotAllowed));
        }
        if self.prepared_merge_parent.is_some() {
            return Err(OperationFailure::before_work(FsError::PreparedMergePending));
        }
        if maximum_changes == 0 || maximum_conflicts == 0 {
            return Err(OperationFailure::before_work(FsError::MergeChangeLimit));
        }
        let theirs_id = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: theirs.digest(),
        };
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self
            .volume
            .resolve_head_generation(&mut accounting, cancellation)
            .await;
        let (current_head_root, current_head) =
            result.map_err(|error| OperationFailure::new(error, accounting.spent()))?;
        let mut work = accounting.spent();
        if current_head_root != theirs_id {
            return Err(OperationFailure::new(FsError::MergeParentNotHead, work));
        }
        let (theirs_root, theirs_work) = read_generation_root(
            &self.volume.fs.inner.objects,
            theirs_id,
            self.volume.config,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, theirs_work)?;
        let merged = merge_generation_async(
            &self.volume.fs.inner.objects,
            MergeGenerationRequest {
                base_generation: self.root.base,
                base: self.base_root.clone(),
                ours_generation: (self.root.file_table == self.base_root.file_table)
                    .then_some(self.generation_root),
                ours: self.root.clone(),
                theirs_generation: theirs_id,
                theirs: theirs_root,
                retain_theirs_parent: true,
                maximum_changes,
                maximum_conflicts,
                resolutions: BTreeMap::new(),
            },
            DecodeLimits::for_volume(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| map_merge_failure(failure, work))?;
        work = add(work, merged.work)?;
        match merged.value {
            MergeGenerationOutcome::Prepared {
                generation_root,
                root,
                generation_id,
            } => {
                let held = self
                    .root
                    .hold
                    .as_ref()
                    .map(|_| (root.clone(), Default::default()));
                self.root
                    .set(self.root.base, root, Default::default(), held);
                self.generation_root = generation_root;
                self.prepared_merge_parent = Some(theirs_id);
                self.authority_head = Some(current_head);
                Ok(OperationReceipt {
                    value: MergePreparation::Prepared { generation_id },
                    work,
                })
            }
            MergeGenerationOutcome::Conflicted {
                conflicts,
                truncated,
            } => Ok(OperationReceipt {
                value: MergePreparation::Conflicted {
                    conflicts,
                    truncated,
                },
                work,
            }),
        }
    }
}
