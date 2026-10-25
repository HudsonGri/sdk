use super::*;

pub(crate) enum RetentionUpdate<A, O> {
    Retain(Option<Owned<Box<VerifiedPublication<A, O>>>>),
    Release,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn workspace_generation_is_retained(
        &self,
        volume: &Volume<A, O>,
        generation: GenerationId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let mut work = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .retains_generation(volume_authority_id(volume.id), generation, &mut work)
            .await;
        record_authority_work(accounting, &work)?;
        result.map_err(Into::into)
    }

    /// Changes one exact generation's retention using its single authority.
    pub(crate) async fn update_workspace_retention(
        &self,
        volume: &Volume<A, O>,
        generation_id: GenerationId,
        kind: RetentionKind,
        label: Owned<String>,
        update: RetentionUpdate<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        async {
            let (authority_id, commit) =
                self.retention_commit(volume, generation_id, kind, label, accounting)?;
            let generation_root = ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: generation_id.digest(),
            };
            let publication = match update {
                RetentionUpdate::Retain(publication) => publication
                    .map(|publication| publication.unbox(accounting))
                    .transpose()?,
                RetentionUpdate::Release => {
                    if !kind.releasable() {
                        return Err(WorkspaceError::RetentionConflict);
                    }
                    return self
                        .release_retention_with_accounting(
                            authority_id,
                            generation_id,
                            commit,
                            accounting,
                            cancellation,
                        )
                        .await;
                }
            };
            let cached_bytes = publication
                .as_ref()
                .map_or(0, VerifiedPublication::retained_bytes);
            let cached = if let Some(publication) = publication {
                let bytes = if accounting.standalone_parts().is_some() {
                    cached_bytes
                } else {
                    0
                };
                let allocation = accounting.claim_owned(bytes, 0)?;
                Some(Owned::new(publication, allocation, bytes))
            } else {
                None
            };
            match self
                .existing_retention_creation(authority_id, &commit, accounting, cancellation)
                .await
            {
                Ok(false) => {}
                result => {
                    if let Some(cached) = cached {
                        cached.retire(accounting)?;
                    }
                    return result.map(|_| ());
                }
            }
            let limits = closure_limits(volume.config);
            let cached = match cached {
                Some(proof)
                    if self.same_deployment(&proof.value.filesystem)
                        && proof.value.generation_root == generation_root
                        && proof.value.volume_id == volume.id
                        && proof.value.limits == limits
                        && proof.value.proven_at == self.inner.objects.collection_sweeps() =>
                {
                    Some(proof)
                }
                Some(proof) => {
                    proof.retire(accounting)?;
                    None
                }
                None => None,
            };
            let publication = if let Some(proof) = cached {
                proof
            } else {
                self.retention_publication_with_accounting(
                    volume,
                    generation_root,
                    accounting,
                    cancellation,
                )
                .await?
            };
            let result = self
                .publish_retention_with_accounting(
                    authority_id,
                    commit,
                    &publication.value,
                    accounting,
                    cancellation,
                )
                .await;
            publication.retire(accounting)?;
            if result? {
                Ok(())
            } else {
                Err(WorkspaceError::RetentionConflict)
            }
        }
        .await
    }

    async fn existing_retention_creation(
        &self,
        authority_id: AuthorityId,
        commit: &ProposedCommit,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, WorkspaceError> {
        let head = match self
            .retention_head_with_accounting(authority_id, accounting, cancellation)
            .await
        {
            Ok(head) => head,
            Err(FsError::Authority(AuthorityStoreError::Missing)) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let found = self
            .retention_operation_with_accounting(
                authority_id,
                commit.operation_id,
                accounting,
                cancellation,
            )
            .await?;
        if head.sequence == Sequence::new(1)
            && found.is_some_and(|(sequence, fingerprint)| {
                sequence == Sequence::new(1) && fingerprint == commit.fingerprint
            })
        {
            Ok(true)
        } else {
            Err(WorkspaceError::RetentionConflict)
        }
    }

    pub(super) fn retention_commit(
        &self,
        volume: &Volume<A, O>,
        generation_id: GenerationId,
        kind: RetentionKind,
        label: Owned<String>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(AuthorityId, ProposedCommit), WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work()
            && (label.bytes != label.value.capacity() as u64
                || !matches!(&label.allocation,
                    KernelOwnedAllocation::NativeOriginal(allocation)
                        if allocation.bytes() == label.bytes && allocation.work().shares_ledger(original)))
        {
            return Err(FsError::from(NativeTreeMaterializationError::InvalidLayout).into());
        }

        accounting.check()?;
        let generation_root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: generation_id.digest(),
        };
        let hashed = WorkCounters {
            bytes_hashed: (b"acyclic-fs-retention-authority-v1\0".len()
                + 16
                + 1
                + 8
                + label.value.len()) as u64,
            bytes_copied: 16,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(hashed)?;
        let authority_id = retention_authority_id(volume.id, kind, &label.value);
        accounting.record_completed(hashed)?;
        let Owned {
            value: label,
            allocation,
            bytes,
        } = label;
        let fact = Owned::new(
            RetentionCreated {
                volume_id: volume.id,
                kind,
                label,
                generation_root,
                config: volume.config,
            },
            allocation,
            bytes,
        );
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_retention_created(&fact.value, observe, allocation_error)
            });
        fact.retire(accounting)?;
        let commit = creation_commit(
            OperationId::from_bytes(authority_id.into_bytes()),
            encoded?,
            accounting,
        )?;
        Ok((authority_id, commit))
    }

    async fn release_retention_with_accounting(
        &self,
        authority_id: AuthorityId,
        generation: GenerationId,
        creation: ProposedCommit,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let head = self
            .retention_head_with_accounting(authority_id, accounting, cancellation)
            .await?;
        let created = self
            .retention_operation_with_accounting(
                authority_id,
                creation.operation_id,
                accounting,
                cancellation,
            )
            .await?;
        if !created.is_some_and(|(sequence, fingerprint)| {
            sequence == Sequence::new(1) && fingerprint == creation.fingerprint
        }) {
            return Err(WorkspaceError::RetentionConflict);
        }
        // Distinct from the creation operation for every possible authority ID.
        let mut operation = authority_id.into_bytes();
        operation[0] ^= 0x80;
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_retention_released(
                    crate::kernel::RetentionReleased {
                        authority_id,
                        generation,
                    },
                    observe,
                    allocation_error,
                )
            })?;
        let commit = creation_commit(OperationId::from_bytes(operation), encoded, accounting)?;
        if head.sequence == Sequence::new(2) {
            let released = self
                .retention_operation_with_accounting(
                    authority_id,
                    commit.operation_id,
                    accounting,
                    cancellation,
                )
                .await?;
            return if released.is_some_and(|(sequence, fingerprint)| {
                sequence == Sequence::new(2) && fingerprint == commit.fingerprint
            }) {
                Ok(())
            } else {
                Err(WorkspaceError::RetentionConflict)
            };
        }
        if head.sequence != Sequence::new(1) {
            return Err(WorkspaceError::RetentionConflict);
        }
        let mut work = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id,
                    epoch: head.epoch,
                    expected: head,
                    commit,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut work,
                None::<&mut ()>,
            )
            .await;
        record_authority_work(accounting, &work)?;
        match result.map_err(|error| FsError::from(error.into_error(std::convert::identity)))? {
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => Ok(()),
            AppendOutcome::Conflict { .. }
            | AppendOutcome::Fenced { .. }
            | AppendOutcome::IdempotencyConflict { .. } => Err(WorkspaceError::RetentionConflict),
        }
    }

    pub(super) async fn retention_head_with_accounting(
        &self,
        authority_id: AuthorityId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Head, FsError> {
        let mut work = authority_work(accounting, cancellation)?;
        let result = self.inner.authority.head(authority_id, &mut work).await;
        record_authority_work(accounting, &work)?;
        result.map_err(|error| match error {
            #[cfg(not(target_arch = "wasm32"))]
            AuthorityStoreError::NativeStream(acyclic_stream::StreamError::NotFound) => {
                FsError::Authority(AuthorityStoreError::Missing)
            }
            error => error.into(),
        })
    }

    pub(super) async fn retention_operation_with_accounting(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(Sequence, Digest)>, FsError> {
        let mut work = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .find_operation(authority_id, operation_id, &mut work)
            .await;
        record_authority_work(accounting, &work)?;
        let record = result?;
        Ok(record.map(|record| (record.sequence, record.fingerprint)))
    }
    pub(super) async fn retention_publication_with_accounting(
        &self,
        volume: &Volume<A, O>,
        root: ObjectId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<VerifiedPublication<A, O>>, WorkspaceError> {
        self.publication_proof(
            root,
            self.inner.objects.collection_sweeps(),
            volume.id,
            volume.config,
            accounting,
            cancellation,
        )
        .await
        .map_err(|error| match error {
            FsError::VolumeMismatch => WorkspaceError::ForeignGeneration,
            error => error.into(),
        })
    }
    pub(super) async fn publish_retention_with_accounting(
        &self,
        authority_id: AuthorityId,
        commit: ProposedCommit,
        publication: &VerifiedPublication<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let _hold = self
            .flush_publication(
                if self.inner.objects.collection().is_none() {
                    crate::PublicationScope::Everything
                } else {
                    crate::PublicationScope::Closure {
                        objects: &publication.objects,
                        proven_at: publication.proven_at,
                    }
                },
                accounting,
                cancellation,
            )
            .await?;
        let mut authority = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .create_first_record(authority_id, commit, &mut authority, None::<&mut ()>)
            .await;
        record_authority_work(accounting, &authority)?;
        result.map_err(|error| FsError::from(error.into_error(std::convert::identity)))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Writes the immutable generation-root checkpoint for the current sparse
    pub async fn checkpoint(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        let checkpoint = self.checkpoint_root(budget, cancellation).await?;
        Ok(OperationReceipt {
            value: GenerationId::new(checkpoint.value.digest),
            work: checkpoint.work,
        })
    }

    pub(super) async fn checkpoint_root<W: FsWork>(
        &self,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<ObjectId, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = self
            .checkpoint_root_with_accounting(&mut accounting, cancellation)
            .await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn checkpoint_root_with_accounting(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        if self.prepared_merge_parent.is_some() || !self.has_pending_mutations() {
            return Ok(self.generation_root);
        }
        crate::kernel::build_checkpoint_with_accounting(
            &self.volume.fs.inner.objects,
            CheckpointRequest {
                base: self.root.base,
                file_table: self.root.file_table,
                merge_parent: None,
            },
            DecodeLimits::for_volume(self.volume.config),
            accounting,
            cancellation,
        )
        .await
    }
}
