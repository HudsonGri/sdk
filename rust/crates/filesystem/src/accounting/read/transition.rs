use super::*;

impl AuthorityKind {
    pub(super) async fn transition<O: AsyncObjectStore>(
        &self,
        objects: &O,
        authority: (AuthorityId, Sequence),
        record: &DurableCommit,
        measured: &mut AccountingReadWork,
        cancellation: &CancellationToken,
    ) -> Result<AccountingTransition, AccountingError> {
        match self {
            Self::Volume { .. } => {
                self.volume_transition(objects, authority, record, measured, cancellation)
                    .await
            }
            Self::Retention { .. } => {
                self.retention_transition(objects, authority, record, measured, cancellation)
                    .await
            }
        }
    }

    async fn volume_transition<O: AsyncObjectStore>(
        &self,
        objects: &O,
        authority: (AuthorityId, Sequence),
        record: &DurableCommit,
        measured: &mut AccountingReadWork,
        cancellation: &CancellationToken,
    ) -> Result<AccountingTransition, AccountingError> {
        let Self::Volume { volume_id, config } = self else {
            return Err(AccountingError::InvalidHistory);
        };
        let (authority_id, head_sequence) = authority;
        let transition = if record.sequence == Sequence::new(1) {
            let created = decode_volume_created(&record.payload, MAX_EVENT_BYTES, &mut |_| {
                Ok::<_, crate::kernel::VolumeCreatedError>(())
            })
            .map_err(|_| AccountingError::InvalidHistory)?;
            let bytes = prove_bytes(
                objects,
                created.initial_generation_root,
                (*volume_id, *config),
                authority_id,
                record,
                measured,
                cancellation,
            )
            .await?;
            AccountingTransition::Published {
                volume_id: *volume_id,
                generation_root: created.initial_generation_root,
                logical_regular_file_bytes: bytes,
            }
        } else {
            if let Ok(published) =
                decode_published_generation(&record.payload, MAX_EVENT_BYTES, |_| {
                    Ok::<_, crate::kernel::CanonicalDecodeError>(())
                })
            {
                if published.volume_id != *volume_id {
                    return Err(AccountingError::InvalidHistory);
                }
                let bytes = prove_bytes(
                    objects,
                    published.generation_root,
                    (*volume_id, *config),
                    authority_id,
                    record,
                    measured,
                    cancellation,
                )
                .await?;
                AccountingTransition::Published {
                    volume_id: *volume_id,
                    generation_root: published.generation_root,
                    logical_regular_file_bytes: bytes,
                }
            } else {
                let deleted = decode_workspace_deleted(&record.payload, MAX_EVENT_BYTES)
                    .map_err(|_| AccountingError::InvalidHistory)?;
                if deleted != *volume_id || record.sequence != head_sequence {
                    return Err(AccountingError::InvalidHistory);
                }
                AccountingTransition::WorkspaceDeleted { volume_id: deleted }
            }
        };
        Ok(transition)
    }

    async fn retention_transition<O: AsyncObjectStore>(
        &self,
        objects: &O,
        authority: (AuthorityId, Sequence),
        record: &DurableCommit,
        measured: &mut AccountingReadWork,
        cancellation: &CancellationToken,
    ) -> Result<AccountingTransition, AccountingError> {
        let Self::Retention {
            volume_id,
            kind,
            config,
            generation_root,
        } = self
        else {
            return Err(AccountingError::InvalidHistory);
        };
        let (authority_id, head_sequence) = authority;
        let transition = {
            if record.sequence == Sequence::new(2) && kind.releasable() {
                let released =
                    crate::kernel::decode_retention_released(&record.payload, MAX_EVENT_BYTES)
                        .map_err(|_| AccountingError::InvalidHistory)?;
                if record.sequence != head_sequence
                    || released.authority_id != authority_id
                    || released.generation.digest() != generation_root.digest
                {
                    return Err(AccountingError::InvalidHistory);
                }
                AccountingTransition::RetentionReleased {
                    volume_id: *volume_id,
                    kind: *kind,
                    generation_root: *generation_root,
                }
            } else {
                if record.sequence != Sequence::new(1) {
                    return Err(AccountingError::InvalidHistory);
                }
                let retained = decode_retention_created(&record.payload, MAX_EVENT_BYTES)
                    .map_err(|_| AccountingError::InvalidHistory)?;
                if retained.volume_id != *volume_id
                    || retained.kind != *kind
                    || retention_authority_id(retained.volume_id, retained.kind, &retained.label)
                        != authority_id
                {
                    return Err(AccountingError::InvalidHistory);
                }
                let bytes = prove_bytes(
                    objects,
                    retained.generation_root,
                    (*volume_id, *config),
                    authority_id,
                    record,
                    measured,
                    cancellation,
                )
                .await?;
                let owner_volume_id = match retained.kind {
                    RetentionKind::ForkBase => hex::decode(&retained.label)
                        .ok()
                        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                        .map(VolumeId::from_bytes)
                        .ok_or(AccountingError::InvalidHistory)?,
                    RetentionKind::Checkpoint
                    | RetentionKind::Pin
                    | RetentionKind::PreparedJoin => *volume_id,
                };
                AccountingTransition::Retained {
                    volume_id: *volume_id,
                    owner_volume_id,
                    kind: *kind,
                    generation_root: retained.generation_root,
                    logical_regular_file_bytes: bytes,
                }
            }
        };
        Ok(transition)
    }
}
