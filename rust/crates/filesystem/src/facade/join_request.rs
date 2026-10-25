use super::*;

pub(super) const WORKSPACE_JOIN_OPERATION_DOMAIN: &[u8] =
    b"acyclic-fs-workspace-join-operation-v1\0";

pub(crate) enum WorkspaceJoinOutcome {
    Applied(GenerationId),
    AlreadyApplied(GenerationId),
    Joined(GenerationId, Digest),
    AlreadyJoined(GenerationId, Digest),
    NoChanges(GenerationId),
    Stale(GenerationId),
    Conflicted(Vec<MergeConflict>, bool),
    Fenced,
    IdempotencyConflict,
}

/// The immutable output of the sole merge planner, before authority publication.
pub(crate) struct PreparedWorkspaceJoin {
    pub context: WorkspaceJoinContext,
    pub request: PublishGenerationRequest,
    pub intent: PublicationIntent,
    pub limits: ClosureLimits,
    pub resolutions_digest: Digest,
    pub _candidate_hold: Option<crate::collection::Hold>,
}

pub(crate) enum WorkspaceJoinPreparation {
    Ready(Owned<Box<PreparedWorkspaceJoin>>),
    Pending(Owned<Box<PendingWorkspaceJoin>>),
    Unchanged(Owned<Box<WorkspaceJoinPublication>>),
    Terminal(Owned<Box<Owned<WorkspaceJoinOutcome>>>),
}

pub(crate) struct PendingWorkspaceJoin {
    pub merge: crate::kernel::GenerationMergePreparation,
    pub(super) publication: WorkspaceJoinPublication,
    // Last: decoded records are dropped before their collection registrations.
    pub(super) holds: Owned<Vec<crate::collection::Hold>>,
}

#[derive(Clone, Copy)]
pub(crate) struct WorkspaceJoinPublication {
    pub context: WorkspaceJoinContext,
    pub expected_head: Head,
    pub operation: OperationId,
    pub permit: PublicationPermit,
    pub resolutions_digest: Digest,
}

impl PendingWorkspaceJoin {
    pub(super) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        self.merge.retire(accounting)?;
        self.holds.retire(accounting)
    }
}

impl WorkspaceJoinPreparation {
    pub(crate) fn unchanged(
        publication: WorkspaceJoinPublication,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        Owned::take_boxed(&mut Some(publication), accounting).map(Self::Unchanged)
    }

    pub(super) fn terminal(
        outcome: WorkspaceJoinOutcome,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, WorkspaceError> {
        let mut slot = Some(workspace_join_scalar(outcome, accounting)?);
        match Owned::take_boxed(&mut slot, accounting) {
            Ok(outcome) => Ok(Self::Terminal(outcome)),
            Err(error) => {
                if let Some(outcome) = slot {
                    outcome.retire(accounting)?;
                }
                Err(error.into())
            }
        }
    }

    /// Re-admits existing buffers when a standalone invocation opens a new
    /// ledger. Native buffers remain charged to their original owner instead.
    pub(crate) fn admit_retained(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if accounting.standalone_parts().is_none() {
            return Ok(());
        }
        let bytes = match self {
            Self::Ready(prepared) => prepared.bytes.checked_add(
                prepared
                    .value
                    ._candidate_hold
                    .as_ref()
                    .map_or(0, crate::collection::Hold::logical_owned_bytes),
            ),
            Self::Terminal(outcome) => outcome.bytes.checked_add(outcome.value.bytes),
            Self::Unchanged(publication) => Some(publication.bytes),
            Self::Pending(pending) => {
                let bytes = pending
                    .bytes
                    .checked_add(pending.value.merge.retained_bytes()?)
                    .and_then(|bytes| bytes.checked_add(pending.value.holds.bytes))
                    .ok_or(WorkError::Overflow)?;
                // These are the existing collection slots, never another root
                // registry or a rescan of the immutable merge inputs.
                accounting.charge_items(pending.value.holds.value.len() as u64)?;
                pending
                    .value
                    .holds
                    .value
                    .iter()
                    .try_fold(bytes, |bytes, hold| {
                        bytes
                            .checked_add(hold.logical_owned_bytes())
                            .ok_or(WorkError::Overflow)
                    })
                    .ok()
            }
        }
        .ok_or(WorkError::Overflow)?;
        accounting.claim_owned(bytes, 0)?;
        Ok(())
    }

    pub(crate) fn ready(
        prepared: PreparedWorkspaceJoin,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        Owned::take_boxed(&mut Some(prepared), accounting).map(Self::Ready)
    }

    pub(crate) async fn into_terminal<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        self,
        target: &Volume<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<WorkspaceJoinOutcome>, WorkspaceError> {
        match self {
            Self::Terminal(outcome) => outcome.unbox(accounting).map_err(Into::into),
            Self::Unchanged(publication) => {
                let publication = publication.unbox(accounting)?;
                let (current, head) = target
                    .fs
                    .workspace_head_state(target, accounting, cancellation)
                    .await?;
                let outcome = if head == publication.expected_head {
                    WorkspaceJoinOutcome::NoChanges(current)
                } else {
                    WorkspaceJoinOutcome::Stale(current)
                };
                workspace_join_scalar(outcome, accounting).map_err(Into::into)
            }
            Self::Pending(pending) => {
                let pending = pending.unbox(accounting)?;
                let conflicts = pending.merge.into_conflicts(accounting);
                pending.holds.retire(accounting)?;
                let Owned {
                    value: (conflicts, truncated),
                    allocation,
                    bytes,
                } = conflicts?;
                Ok(Owned::new(
                    WorkspaceJoinOutcome::Conflicted(conflicts, truncated),
                    allocation,
                    bytes,
                ))
            }
            Self::Ready(_) => Err(FsError::InvalidDiff.into()),
        }
    }
}

pub(crate) struct WorkspaceJoinRequest<'a, A, O> {
    pub target: &'a Volume<A, O>,
    pub base: WorkspaceCommonAncestor,
    pub source: &'a crate::Generation<A, O>,
    pub expected_target: GenerationId,
    pub expected_head: Head,
    pub history: crate::workspace::JoinHistory,
    pub operation_id: OperationId,
    pub permit: PublicationPermit,
    pub maximum_generations: u32,
    pub maximum_changes: u32,
    pub maximum_conflicts: u32,
    pub resolutions: WorkspaceJoinResolutions<'a>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceJoinContext {
    pub source_workspace: VolumeId,
    pub source_generation: GenerationId,
    pub target_workspace: VolumeId,
    pub expected_target: GenerationId,
    pub base: WorkspaceCommonAncestor,
    pub history: crate::workspace::JoinHistory,
    pub maximum_generations: u32,
    pub maximum_changes: u32,
    pub maximum_conflicts: u32,
}

impl<A, O> WorkspaceJoinRequest<'_, A, O> {
    pub(super) fn context(&self) -> WorkspaceJoinContext {
        WorkspaceJoinContext {
            source_workspace: self.source.workspace.volume.id,
            source_generation: self.source.id,
            target_workspace: self.target.id,
            expected_target: self.expected_target,
            base: self.base,
            history: self.history,
            maximum_generations: self.maximum_generations,
            maximum_changes: self.maximum_changes,
            maximum_conflicts: self.maximum_conflicts,
        }
    }
}

impl WorkspaceJoinContext {
    pub(super) fn digest_with_accounting(
        self,
        resolutions: Digest,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Digest, FsError> {
        let work = WorkCounters {
            bytes_hashed: usize_to_u64(WORKSPACE_JOIN_OPERATION_DOMAIN.len()) + 189,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(work)?;
        let digest = self.digest(resolutions);
        accounting.record_completed(work)?;
        Ok(digest)
    }

    pub(super) fn digest(self, resolutions: Digest) -> Digest {
        let mut hasher = blake3::Hasher::new();
        hasher.update(WORKSPACE_JOIN_OPERATION_DOMAIN);
        hasher.update(&self.source_workspace.into_bytes());
        hasher.update(self.source_generation.digest().as_bytes());
        hasher.update(&self.target_workspace.into_bytes());
        hasher.update(self.expected_target.digest().as_bytes());
        hasher.update(&self.base.volume_id.into_bytes());
        hasher.update(self.base.id.digest().as_bytes());
        hasher.update(&[match self.history {
            crate::workspace::JoinHistory::Merge => 1,
            crate::workspace::JoinHistory::Rebase => 2,
            crate::workspace::JoinHistory::Squash => 3,
            crate::workspace::JoinHistory::CherryPick => 4,
        }]);
        hasher.update(&self.maximum_generations.to_le_bytes());
        hasher.update(&self.maximum_changes.to_le_bytes());
        hasher.update(&self.maximum_conflicts.to_le_bytes());
        hasher.update(resolutions.as_bytes());
        Digest::from_bytes(*hasher.finalize().as_bytes())
    }
}

pub(crate) enum WorkspaceJoinResolutions<'a> {
    Map(&'a BTreeMap<MergeConflict, MergeConflictResolution>),
    #[cfg(not(target_arch = "wasm32"))]
    Slice(&'a [(MergeConflict, MergeConflictResolution)]),
}

impl WorkspaceJoinResolutions<'_> {
    pub(super) async fn prepare_generation<O: AsyncObjectStore>(
        &self,
        objects: &O,
        request: crate::kernel::MergeGenerationBorrowedRequest<'_>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::GenerationMergePreparation, FsError> {
        match self {
            Self::Map(map) => {
                crate::kernel::GenerationMergePreparation::prepare_map(
                    objects,
                    request,
                    map,
                    limits,
                    accounting,
                    cancellation,
                )
                .await
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Slice(slice) => {
                crate::kernel::GenerationMergePreparation::prepare_slice(
                    objects,
                    request,
                    slice,
                    limits,
                    accounting,
                    cancellation,
                )
                .await
            }
        }
    }

    pub(super) async fn resolve_generation<O: AsyncObjectStore>(
        &self,
        preparation: &mut crate::kernel::GenerationMergePreparation,
        objects: &O,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        match self {
            Self::Map(map) => {
                preparation
                    .resolve_entries(objects, map.iter(), limits, accounting, cancellation)
                    .await
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Slice(slice) => {
                preparation
                    .resolve_entries(
                        objects,
                        slice.iter().map(|(key, value)| (key, value)),
                        limits,
                        accounting,
                        cancellation,
                    )
                    .await
            }
        }
    }

    pub(super) fn visit(
        &self,
        mut visit: impl FnMut(&MergeConflict, &MergeConflictResolution) -> Result<(), FsError>,
    ) -> Result<(), FsError> {
        match self {
            Self::Map(map) => {
                for (key, value) in *map {
                    visit(key, value)?;
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Slice(slice) => {
                for (key, value) in *slice {
                    visit(key, value)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn hash_join_resolutions(
    resolutions: &WorkspaceJoinResolutions<'_>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Digest, FsError> {
    let mut hasher = blake3::Hasher::new();
    let domain = b"acyclic-fs-workspace-join-resolutions-v1\0";
    let prefix = WorkCounters {
        bytes_hashed: domain.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(prefix)?;
    hasher.update(domain);
    accounting.record_completed(prefix)?;
    resolutions.visit(|conflict, resolution| {
        let key_bytes = match conflict {
            MergeConflict::File(_) => 17,
            MergeConflict::Binding { name, .. } => 26_u64
                .checked_add(name.as_bytes().len() as u64)
                .ok_or(WorkError::Overflow)?,
        };
        let resolution_bytes = match resolution {
            MergeConflictResolution::Select(_)
            | MergeConflictResolution::File(None)
            | MergeConflictResolution::Binding(None) => 2,
            MergeConflictResolution::Binding(Some(_)) => 18,
            MergeConflictResolution::File(Some(record)) => {
                let payload_bytes = match record.payload {
                    FilePayload::InlineRegular(data) => 9 + data.as_bytes().len() as u64,
                    FilePayload::Regular { .. }
                    | FilePayload::SymbolicLink { .. }
                    | FilePayload::ReparsePoint { .. } => 42,
                    FilePayload::Directory { .. } => 34,
                    FilePayload::Empty => 1,
                    FilePayload::Device { .. } => 9,
                };
                60 + payload_bytes
            }
        };
        let hashing = WorkCounters {
            bytes_hashed: key_bytes
                .checked_add(resolution_bytes)
                .ok_or(WorkError::Overflow)?,
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        match conflict {
            MergeConflict::File(file_id) => {
                hasher.update(&[0]);
                hasher.update(&file_id.into_bytes());
            }
            MergeConflict::Binding { directory_id, name } => {
                hasher.update(&[1]);
                hasher.update(&directory_id.into_bytes());
                hasher.update(&[match name.encoding() {
                    NameEncoding::Utf8 => 0,
                    NameEncoding::PosixBytes => 1,
                    NameEncoding::WindowsUtf16Le => 2,
                }]);
                hasher.update(&(name.as_bytes().len() as u64).to_le_bytes());
                hasher.update(name.as_bytes());
            }
        }
        hash_merge_resolution(&mut hasher, resolution);
        accounting.record_completed(hashing)
    })?;
    Ok(Digest::from_bytes(*hasher.finalize().as_bytes()))
}

pub(super) fn workspace_join_scalar(
    outcome: WorkspaceJoinOutcome,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<WorkspaceJoinOutcome>, FsError> {
    Ok(Owned::new(outcome, accounting.claim_owned(0, 0)?, 0))
}
