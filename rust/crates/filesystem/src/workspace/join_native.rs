use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinPlan<A, O> {
    /// Applies exact resolutions on the original invocation and publication seal.
    pub async fn apply_with_native_work<'original, F, Fut, E>(
        &self,
        options: ApplyOptions,
        resolutions: &[(MergeConflict, crate::kernel::MergeConflictResolution)],
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
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
        if options.if_target != self.target_head.id {
            let mut accounting =
                crate::kernel::allocation::KernelAccounting::native(original, cancellation);
            let (id, _) = self
                .target_head
                .workspace
                .volume
                .fs
                .workspace_head_state(
                    &self.target_head.workspace.volume,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            return Ok((
                JoinOutcome::StaleTarget(Generation {
                    workspace: self.target_head.workspace.clone(),
                    id,
                    publication_closure: None,
                }),
                original
                    .claim(0, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(FsError::from)?,
            ));
        }
        let (outcome, allocation) = self
            .target_head
            .workspace
            .volume
            .fs
            .apply_workspace_join_guarded_with_native_work(
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
                    resolutions: crate::facade::WorkspaceJoinResolutions::Slice(resolutions),
                },
                original,
                cancellation,
                before_publication,
            )
            .await?;
        self.map_join_outcome(outcome)
            .map(|outcome| (outcome, allocation))
            .map_err(E::from)
    }

    /// Resolves the retained frontier once, then publishes under the original live seal.
    pub async fn apply_sides_with_permit_guarded_with_native_work<'original, F, Fut, E>(
        &self,
        options: ApplyOptions,
        selections: &BTreeMap<MergeConflict, ConflictSide>,
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + From<DrivenJoinError> + Send + 'static,
    {
        if options.if_target != self.target_head.id {
            return self
                .apply_with_native_work(
                    options,
                    &[],
                    permit,
                    original,
                    cancellation,
                    before_publication,
                )
                .await;
        }
        let prepared = self
            .prepare(options, permit, original, cancellation)
            .await
            .map_err(E::from)?;
        let Some((conflicts, truncated)) = prepared.conflicts() else {
            return prepared
                .publish_with_native_work(original, cancellation, before_publication)
                .await;
        };
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        validate_join_side_selections(conflicts, truncated, selections, &mut accounting)
            .map_err(E::from)?;
        let resolutions = native_join_side_resolutions(selections, &mut accounting, cancellation)
            .map_err(E::from)?;
        let resolved = prepared
            .resolve_slice(&resolutions.value, original, cancellation)
            .await;
        resolutions.retire(&mut accounting).map_err(E::from)?;
        resolved
            .map_err(E::from)?
            .publish_with_native_work(original, cancellation, before_publication)
            .await
    }
}

fn native_join_side_resolutions(
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<
    crate::kernel::Owned<Vec<(MergeConflict, crate::kernel::MergeConflictResolution)>>,
    FsError,
> {
    let mut resolutions = crate::kernel::Owned::allocate(selections.len(), accounting)?;
    for (conflict, side) in selections {
        let conflict = match conflict {
            MergeConflict::File(id) => MergeConflict::File(*id),
            MergeConflict::Binding { directory_id, name } => {
                let (name, allocation) = name.clone_with_accounting(accounting, cancellation)?;
                let bytes = name.retained_bytes() as u64;
                let mut name = crate::kernel::Owned::new(name, allocation, bytes);
                let retained = resolutions
                    .bytes
                    .checked_add(bytes)
                    .ok_or(crate::WorkError::Overflow)?;
                resolutions.allocation.absorb(&mut name.allocation)?;
                resolutions.bytes = retained;
                MergeConflict::Binding {
                    directory_id: *directory_id,
                    name: name.value,
                }
            }
        };
        let copying = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<(MergeConflict, crate::kernel::MergeConflictResolution)>(
            ) as u64,
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copying)?;
        resolutions.value.push((conflict, side_resolution(*side)));
        accounting.record_completed(copying)?;
    }
    Ok(resolutions)
}
