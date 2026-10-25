use super::*;

pub(super) fn child_wins_resolutions(
    plan: &crate::MergePlan,
    paths: &BTreeSet<String>,
) -> BTreeMap<ConflictKey, MergeResolution> {
    plan.conflicts
        .iter()
        .filter(|conflict| {
            conflict.kind == crate::ConflictKind::Binding
                && conflict
                    .path
                    .as_ref()
                    .is_some_and(|path| paths.contains(path))
        })
        .map(|conflict| {
            (
                conflict.key.clone(),
                MergeResolution::Select(ConflictSide::Theirs),
            )
        })
        .collect()
}

pub(super) fn publisher_key(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
    purpose: &[u8],
) -> IdempotencyKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.workspace-multi-root-publisher.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    hasher.update(purpose);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
}

pub(super) fn materialization_operation_id(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.multi-root-materialization.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

pub(super) fn conflict_abort_materialization_operation_id(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.multi-root-conflict-abort-materialization.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

/// The portable text of a path in any encoding the volume stores (UTF-8 or Windows UTF-16); `None` only for a name with no Unicode text.
pub(super) fn portable_namespace_path(path: &crate::kernel::NamespacePath) -> Option<String> {
    let mut value = String::new();
    for component in path.components() {
        value.push('/');
        value.push_str(
            &component
                .unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .ok()
                .map(|receipt| receipt.value)?,
        );
    }
    if value.is_empty() {
        value.push('/');
    }
    Some(value)
}

pub(super) fn conflict_finish_operation_id(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.multi-root-conflict-finish.v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(&root_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

pub(super) async fn generation_descends_from<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    descendant: GenerationId,
    ancestor: GenerationId,
) -> Result<bool, WorkspaceError> {
    if descendant == ancestor {
        return Ok(true);
    }
    let mut pending = vec![descendant];
    let mut visited = BTreeSet::new();
    while let Some(candidate) = pending.pop() {
        if !visited.insert(candidate) || visited.len() > 65_536 {
            continue;
        }
        for parent in workspace
            .generation(candidate, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
        {
            if parent == ancestor {
                return Ok(true);
            }
            pending.push(parent);
        }
    }
    Ok(false)
}

pub(super) fn decode_reservation<E: std::error::Error + 'static>(
    operation_id: OperationId,
    root_id: WorkspaceRootId,
    fence: &MultiRootFence,
) -> Result<WorkspacePublicationFence, WorkspaceMultiRootPublisherError<E>> {
    let decoded: WorkspacePublicationFence = serde_json::from_slice(&fence.token)
        .map_err(|_| WorkspaceMultiRootPublisherError::InvalidFence)?;
    if decoded.reservation.operation_id != operation_id || decoded.root_id != root_id {
        return Err(WorkspaceMultiRootPublisherError::InvalidFence);
    }
    if let Some(checkpoint) = decoded.checkpoint {
        let permit = PublicationPermit::Reservation {
            operation_id: operation_id.into_bytes(),
            gate_tail: decoded.reservation.gate_tail,
            expected: decoded.reservation.expected,
        };
        if checkpoint.target != decoded.target_workspace_id
            || checkpoint.expected != decoded.reservation.expected
            || checkpoint.permit != permit
            || checkpoint.operation
                != publisher_key(operation_id, root_id, b"publish").operation_id()
        {
            return Err(WorkspaceMultiRootPublisherError::InvalidFence);
        }
    }
    Ok(decoded)
}

// The durable projection and conflict set jointly identify this operation's undo.
pub(super) fn conflict_abort_paths(
    changed: &[crate::ChangedPath],
    conflicts: Option<&crate::MergePlan>,
) -> Result<Vec<String>, String> {
    let mut paths = changed
            .iter()
            .map(|change| {
                portable_namespace_path(&change.path).ok_or_else(|| {
                    "conflict abort cannot represent a non-portable path".to_owned()
                })
            })
            // A held conflict can have no projected delta. Its user's later
            // resolution still belongs to this merge and must be undone.
            .chain(conflicts.into_iter().flat_map(|plan| plan.conflicts.iter()).map(|conflict| {
                conflict.path.as_ref().map(|path| format!("/{}", path.trim_start_matches('/'))).ok_or_else(|| {
                    "conflict abort cannot represent an unresolved identity without a portable path".to_owned()
                })
            }))
            .collect::<Result<Vec<_>, _>>()?;
    paths.sort_unstable();
    paths.dedup();
    Ok(paths)
}
