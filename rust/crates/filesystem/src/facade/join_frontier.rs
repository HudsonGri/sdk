use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(super) async fn retain_pending_workspace_join(
        &self,
        mut pending: PendingWorkspaceJoin,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPreparation, WorkspaceError> {
        if let Err(error) = self
            .hold_join_frontier(&mut pending, config, accounting, cancellation)
            .await
        {
            pending.retire(accounting)?;
            return Err(error);
        }
        let mut slot = Some(pending);
        match Owned::take_boxed(&mut slot, accounting) {
            Ok(pending) => Ok(WorkspaceJoinPreparation::Pending(pending)),
            Err(error) => {
                if let Some(pending) = slot {
                    pending.retire(accounting)?;
                }
                Err(error.into())
            }
        }
    }

    async fn hold_join_frontier(
        &self,
        pending: &mut PendingWorkspaceJoin,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let mut records = Owned::allocate(0, accounting)?;
        let result = async {
            for record in pending.merge.records() {
                records.push_accounted(record, usize::MAX, accounting)?;
            }
            let capacity = records
                .value
                .len()
                .checked_add(3)
                .ok_or(WorkError::Overflow)?;
            pending.holds = Owned::allocate(capacity, accounting)?;
            let collection = self.inner.objects.collection();
            // Record proofs cover accepted partial directories without pretending
            // that an unresolved namespace is a publishable generation.
            let durable = self
                .make_records_durable_with_accounting(
                    config,
                    &records.value,
                    accounting,
                    cancellation,
                )
                .await?;
            for &record in &records.value {
                if let Some(hold) = crate::collection::Collection::hold_record_with_accounting(
                    collection, record, config, accounting,
                )? {
                    pending.holds.push_accounted(hold, capacity, accounting)?;
                }
            }
            drop(durable);
            for generation in pending.merge.generations() {
                let volume = Volume {
                    fs: self.clone(),
                    id: pending.publication.context.target_workspace,
                    config,
                };
                let durable = self
                    .make_generation_durable_with_accounting(
                        &volume,
                        GenerationId::new(generation.digest),
                        &[],
                        accounting,
                        cancellation,
                    )
                    .await?;
                if let Some(hold) = crate::collection::Collection::hold_generation_with_accounting(
                    collection, generation, config, accounting,
                )? {
                    pending.holds.push_accounted(hold, capacity, accounting)?;
                }
                drop(durable);
            }
            Ok(())
        }
        .await;
        records.retire(accounting)?;
        result
    }

    pub(crate) async fn resolve_workspace_join_with_accounting(
        &self,
        target: &Volume<A, O>,
        preparation: WorkspaceJoinPreparation,
        resolutions: WorkspaceJoinResolutions<'_>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPreparation, WorkspaceError> {
        let WorkspaceJoinPreparation::Pending(mut pending) = preparation else {
            return Ok(preparation);
        };
        let resolved = resolutions
            .resolve_generation(
                &mut pending.value.merge,
                &self.inner.objects,
                DecodeLimits::for_volume(target.config),
                accounting,
                cancellation,
            )
            .await;
        match resolved {
            Ok(false) => return Ok(WorkspaceJoinPreparation::Pending(pending)),
            Ok(true) => {}
            Err(error) => {
                retire_pending(pending, accounting)?;
                return Err(error.into());
            }
        }
        let digest = match hash_join_resolutions(&resolutions, accounting) {
            Ok(digest) => digest,
            Err(error) => {
                retire_pending(pending, accounting)?;
                return Err(error.into());
            }
        };
        let PendingWorkspaceJoin {
            merge,
            mut publication,
            holds,
        } = pending.unbox(accounting)?;
        publication.resolutions_digest = digest;
        let result = self
            .finish_join_preparation(target, publication, merge, accounting, cancellation)
            .await;
        holds.retire(accounting)?;
        result
    }
}

fn retire_pending(
    pending: Owned<Box<PendingWorkspaceJoin>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    pending.unbox(accounting)?.retire(accounting)
}
