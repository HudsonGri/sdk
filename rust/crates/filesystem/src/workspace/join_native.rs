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

    /// Validates the ordinary conflict set, then seals the same second application.
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
        let (initial, allocation) = self
            .apply_with_native_work(
                options,
                &[],
                permit,
                original,
                cancellation,
                before_publication,
            )
            .await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok((initial, allocation));
        };
        let bytes = allocation.bytes();
        let conflicts = crate::kernel::Owned::new(
            conflicts,
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation),
            bytes,
        );
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let validation =
            validate_join_side_selections(&conflicts.value, truncated, selections, &mut accounting);
        conflicts.retire(&mut accounting).map_err(E::from)?;
        validation.map_err(E::from)?;
        let resolutions = native_join_side_resolutions(selections, &mut accounting, cancellation)
            .map_err(E::from)?;
        let applied = self
            .apply_with_native_work(
                options,
                &resolutions.value,
                permit,
                original,
                cancellation,
                before_publication,
            )
            .await;
        resolutions.retire(&mut accounting).map_err(E::from)?;
        applied
    }
}

fn validate_join_side_selections(
    conflicts: &[MergeConflict],
    truncated: bool,
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(), DrivenJoinError> {
    if truncated || conflicts.len() != selections.len() {
        return Err(DrivenJoinError::StaleCandidate);
    }
    // Kernel traversal order interleaves file and binding conflicts. Sort only
    // borrowed keys with the shared metered sorter, then walk both sets once.
    let mut ordered = crate::kernel::Owned::allocate(conflicts.len(), accounting)
        .map_err(WorkspaceError::from)?;
    let validation = (|| {
        for (ordinal, key) in conflicts.iter().enumerate() {
            let copied = crate::WorkCounters {
                items_examined: 1,
                bytes_copied: std::mem::size_of::<crate::kernel::IndexedKey<'_, MergeConflict>>()
                    as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied).map_err(WorkspaceError::from)?;
            ordered
                .value
                .push(crate::kernel::IndexedKey { ordinal, key });
            accounting
                .record_completed(copied)
                .map_err(WorkspaceError::from)?;
        }
        crate::kernel::sort_indexed(&mut ordered.value, accounting)
            .map_err(WorkspaceError::from)?;
        for (conflict, key) in ordered.value.iter().zip(selections.keys()) {
            accounting.charge_items(1).map_err(WorkspaceError::from)?;
            if conflict.key != key {
                return Err(DrivenJoinError::StaleCandidate);
            }
        }
        Ok(())
    })();
    ordered.retire(accounting).map_err(WorkspaceError::from)?;
    validation
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

#[cfg(test)]
mod selection_tests {
    use super::*;
    use crate::facade::FsWork as _;

    fn conflict(index: u128) -> MergeConflict {
        MergeConflict::File(crate::FileId::from_bytes(index.to_be_bytes()))
    }

    #[test]
    fn shuffled_conflicts_use_the_shared_sorter_and_reject_duplicate_regions() {
        let cancellation = crate::CancellationToken::new();
        let budget = crate::performance::work_budget(32_768, 1024 * 1024);
        let selections = (0..512)
            .map(|i| (conflict(i), ConflictSide::Ours))
            .collect();
        let conflicts: Vec<_> = (0..512).rev().map(conflict).collect();
        let mut accounting = budget.accounting(&cancellation);
        validate_join_side_selections(&conflicts, false, &selections, &mut accounting).unwrap();
        assert!(accounting.spent().items_examined < 32_768);

        // Actual old traversal restarts at the first selection for each key.
        // The same allowance rejects it rather than hiding the quadratic work.
        let mut old = budget.accounting(&cancellation);
        let mut denied = false;
        'conflicts: for conflict in &conflicts {
            for key in selections.keys() {
                if old.charge_items(1).is_err() {
                    denied = true;
                    break 'conflicts;
                }
                if key == conflict {
                    break;
                }
            }
        }
        assert!(denied, "restarted key scans escaped the original allowance");

        let mut duplicate = conflicts;
        duplicate.pop();
        duplicate.push(conflict(511));
        let mut accounting = budget.accounting(&cancellation);
        assert!(matches!(
            validate_join_side_selections(&duplicate, false, &selections, &mut accounting),
            Err(DrivenJoinError::StaleCandidate)
        ));
    }

    #[test]
    fn one_hundred_thousand_sorted_selections_have_linear_validation_work() {
        let cancellation = crate::CancellationToken::new();
        let selections = (0..100_000)
            .map(|i| (conflict(i), ConflictSide::Theirs))
            .collect();
        let conflicts: Vec<_> = (0..100_000).map(conflict).collect();
        let budget = crate::performance::work_budget(300_000, 4 * 1024 * 1024);
        let mut accounting = budget.accounting(&cancellation);
        validate_join_side_selections(&conflicts, false, &selections, &mut accounting).unwrap();
        assert_eq!(accounting.spent().items_examined, 299_999);
    }
}
