use super::*;

/// Untrusted restart descriptor for a candidate issued by the SDK merge planner.
/// Deserializing it grants no publication authority: resume authenticates its
/// exact fingerprint against the independent prepared-join retention authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedJoinCheckpoint {
    pub(crate) target: crate::WorkspaceId,
    pub(crate) expected: Head,
    pub(crate) operation: OperationId,
    pub(crate) candidate: GenerationId,
    pub(crate) context: Digest,
    pub(crate) resolutions: Digest,
    pub(crate) permit: PublicationPermit,
    pub(crate) join: WorkspaceJoinContext,
    pub(crate) state: PreparedJoinCheckpointState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PreparedJoinCheckpointState {
    Candidate,
    Pending(Digest),
    Unchanged,
}

impl PreparedJoinCheckpointState {
    fn tag(self) -> u8 {
        match self {
            Self::Candidate => 0,
            Self::Pending(_) => 1,
            Self::Unchanged => 2,
        }
    }
    pub(crate) fn program(self) -> Option<Digest> {
        match self {
            Self::Pending(digest) => Some(digest),
            Self::Candidate | Self::Unchanged => None,
        }
    }
}

impl PreparedJoinCheckpoint {
    pub(super) fn program_from_label(label: &str) -> Result<Option<Digest>, FsError> {
        if label.len() == 64 {
            let mut digest = [0; 32];
            hex::decode_to_slice(label, &mut digest)
                .map_err(|_| FsError::InvalidAuthorityHistory)?;
            return Ok(None);
        }
        let bytes = label.as_bytes();
        if bytes.len() != 137 || !bytes.starts_with(b"program:") || bytes.get(72) != Some(&b':') {
            return Err(FsError::InvalidAuthorityHistory);
        }
        let mut program = [0; 32];
        let mut fingerprint = [0; 32];
        hex::decode_to_slice(
            bytes.get(8..72).ok_or(FsError::InvalidAuthorityHistory)?,
            &mut program,
        )
        .map_err(|_| FsError::InvalidAuthorityHistory)?;
        hex::decode_to_slice(
            bytes.get(73..).ok_or(FsError::InvalidAuthorityHistory)?,
            &mut fingerprint,
        )
        .map_err(|_| FsError::InvalidAuthorityHistory)?;
        Ok(Some(Digest::from_bytes(program)))
    }

    pub(crate) fn label(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<String>, FsError> {
        let join = self.join;
        if join.target_workspace != self.target.volume_id()
            || join.maximum_changes == 0
            || join.maximum_conflicts == 0
            || join.maximum_generations == 0
            || join.digest_with_accounting(self.resolutions, accounting)? != self.context
            || (self.state != PreparedJoinCheckpointState::Candidate
                && join.expected_target != self.candidate)
        {
            return Err(FsError::InvalidAuthorityHistory);
        }
        const DOMAIN: &[u8] = b"acyclic-fs-prepared-join-checkpoint-v1\0";
        let work = WorkCounters {
            bytes_hashed: usize_to_u64(DOMAIN.len())
                + 177
                + u64::from(self.state.program().is_some()) * 32,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(work)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN);
        hasher.update(&self.target.into_bytes());
        hasher.update(&self.expected.epoch.get().to_be_bytes());
        hasher.update(&self.expected.sequence.get().to_be_bytes());
        hasher.update(self.expected.digest.as_bytes());
        hasher.update(&self.operation.into_bytes());
        hasher.update(self.candidate.digest().as_bytes());
        hasher.update(self.context.as_bytes());
        hasher.update(self.resolutions.as_bytes());
        hasher.update(&[self.state.tag()]);
        if let Some(program) = self.state.program() {
            hasher.update(program.as_bytes());
        }
        let digest = Digest::from_bytes(*hasher.finalize().as_bytes());
        accounting.record_completed(work)?;
        // Reuse the publication engine's sole permit binding implementation.
        let digest = crate::kernel::permit_context(Some(digest), self.permit, accounting)?
            .ok_or(FsError::InvalidAuthorityHistory)?;
        let length = if self.state.program().is_some() {
            137
        } else {
            64
        };
        let copied = WorkCounters {
            bytes_encoded: length,
            bytes_copied: length,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let mut label = [0; 137];
        let output = if let Some(program) = self.state.program() {
            label[..8].copy_from_slice(b"program:");
            hex::encode_to_slice(program.as_bytes(), &mut label[8..72])
                .map_err(|_| FsError::InvalidAuthorityHistory)?;
            label[72] = b':';
            &mut label[73..]
        } else {
            &mut label[..64]
        };
        hex::encode_to_slice(digest.as_bytes(), output)
            .map_err(|_| FsError::InvalidAuthorityHistory)?;
        accounting.record_completed(copied)?;
        let length = usize::try_from(length).map_err(|_| WorkError::Overflow)?;
        let label = std::str::from_utf8(
            label
                .get(..length)
                .ok_or(FsError::InvalidAuthorityHistory)?,
        )
        .map_err(|_| FsError::InvalidAuthorityHistory)?;
        Owned::<String>::copy(label, accounting)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn authenticate_join_checkpoint(
        &self,
        volume: &Volume<A, O>,
        checkpoint: &PreparedJoinCheckpoint,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        if checkpoint.target.volume_id() != volume.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let label = checkpoint.label(accounting)?;
        let (authority_id, commit) = self.retention_commit(
            volume,
            checkpoint.candidate,
            RetentionKind::PreparedJoin,
            label,
            accounting,
        )?;
        let head = self
            .retention_head_with_accounting(authority_id, accounting, cancellation)
            .await?;
        let found = self
            .retention_operation_with_accounting(
                authority_id,
                commit.operation_id,
                accounting,
                cancellation,
            )
            .await?;
        if head.sequence != Sequence::new(1)
            || !found.is_some_and(|(sequence, fingerprint)| {
                sequence == Sequence::new(1) && fingerprint == commit.fingerprint
            })
        {
            return Err(WorkspaceError::RetentionConflict);
        }
        Ok(())
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn checkpoint_unpublished_join(
        &self,
        volume: &Volume<A, O>,
        preparation: &WorkspaceJoinPreparation,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<PreparedJoinCheckpoint, WorkspaceError> {
        if let WorkspaceJoinPreparation::Unchanged(publication) = preparation {
            let publication = *publication.value;
            let checkpoint = PreparedJoinCheckpoint {
                target: crate::WorkspaceId::from_bytes(volume.id.into_bytes()),
                expected: publication.expected_head,
                operation: publication.operation,
                candidate: publication.context.expected_target,
                context: publication
                    .context
                    .digest_with_accounting(publication.resolutions_digest, accounting)?,
                resolutions: publication.resolutions_digest,
                permit: publication.permit,
                join: publication.context,
                state: PreparedJoinCheckpointState::Unchanged,
            };
            let label = checkpoint.label(accounting)?;
            self.update_workspace_retention(
                volume,
                checkpoint.candidate,
                RetentionKind::PreparedJoin,
                label,
                RetentionUpdate::Retain(None),
                accounting,
                cancellation,
            )
            .await?;
            return Ok(checkpoint);
        }
        let WorkspaceJoinPreparation::Pending(pending) = preparation else {
            return Err(WorkspaceError::IncompatibleWorkspace);
        };
        let pending = &pending.value;
        let limits = DecodeLimits::for_volume(volume.config);
        let encoded = pending.merge.encode_program(limits, accounting)?;
        let object = self
            .put_encoded_with_accounting(ObjectKind::BlobChunk, encoded, accounting, cancellation)
            .await?;
        let publication = pending.publication;
        let checkpoint = PreparedJoinCheckpoint {
            target: crate::WorkspaceId::from_bytes(volume.id.into_bytes()),
            expected: publication.expected_head,
            operation: publication.operation,
            candidate: publication.context.expected_target,
            context: publication
                .context
                .digest_with_accounting(publication.resolutions_digest, accounting)?,
            resolutions: publication.resolutions_digest,
            permit: publication.permit,
            join: publication.context,
            state: PreparedJoinCheckpointState::Pending(object.digest),
        };
        let durable = self
            .make_generation_durable_with_accounting(
                volume,
                checkpoint.candidate,
                &[object],
                accounting,
                cancellation,
            )
            .await?;
        let hold = crate::collection::Collection::hold_merge_program_with_accounting(
            self.objects().collection(),
            object,
            volume.config,
            accounting,
        )?;
        drop(durable);
        let label = checkpoint.label(accounting)?;
        let result = self
            .update_workspace_retention(
                volume,
                checkpoint.candidate,
                RetentionKind::PreparedJoin,
                label,
                RetentionUpdate::Retain(None),
                accounting,
                cancellation,
            )
            .await;
        drop(hold);
        result?;
        Ok(checkpoint)
    }

    pub(crate) async fn resume_pending_join(
        &self,
        volume: &Volume<A, O>,
        checkpoint: &PreparedJoinCheckpoint,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceJoinPreparation, WorkspaceError> {
        let context = checkpoint.join;
        let holds = Owned::allocate(0, accounting)?;
        let object = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: checkpoint
                .state
                .program()
                .ok_or(WorkspaceError::RetentionConflict)?,
        };
        let (read, bytes) = crate::kernel::read_object_with_accounting(
            self.objects(),
            object,
            volume.config.limits.maximum_object_bytes,
            accounting,
            cancellation,
        )
        .await?;
        let result = crate::kernel::GenerationMergePreparation::decode_program(
            &read.bytes,
            DecodeLimits::for_volume(volume.config),
            accounting,
        );
        drop(read);
        crate::kernel::release_standalone_read_bytes(bytes, accounting)?;
        let merge = result?;
        self.retain_pending_workspace_join(
            PendingWorkspaceJoin {
                merge,
                publication: WorkspaceJoinPublication {
                    context,
                    expected_head: checkpoint.expected,
                    operation: checkpoint.operation,
                    permit: checkpoint.permit,
                    resolutions_digest: checkpoint.resolutions,
                },
                holds,
            },
            volume.config,
            accounting,
            cancellation,
        )
        .await
    }
}
