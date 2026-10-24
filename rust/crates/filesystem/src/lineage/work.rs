use super::*;

fn record<E: std::error::Error + 'static>(
    workspace_name: &str,
    parent_name: Option<&str>,
    build: impl FnOnce(String, Option<String>) -> WorkspaceLineageRecord,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<WorkspaceLineageRecord>, WorkspaceLineageError<E>> {
    let bytes = workspace_name
        .len()
        .checked_add(parent_name.map_or(0, str::len))
        .ok_or(crate::WorkError::Overflow)? as u64;
    let operations = u64::from(!workspace_name.is_empty())
        + u64::from(parent_name.is_some_and(|name| !name.is_empty()));
    let mut allocation = accounting.claim_owned(bytes, operations)?;
    let copied = accounting.perform(
        crate::WorkCounters {
            bytes_copied: bytes,
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        },
        || build(workspace_name.to_owned(), parent_name.map(str::to_owned)),
    );
    match copied {
        Ok(value) => Ok(Owned::new(value, allocation, bytes)),
        Err(error) => {
            accounting.release_owned(&mut allocation, bytes)?;
            Err(error.into())
        }
    }
}

pub(crate) fn copy<E: std::error::Error + 'static>(
    value: &WorkspaceLineageRecord,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<WorkspaceLineageRecord>, WorkspaceLineageError<E>> {
    record(
        &value.workspace_name,
        value.parent_workspace_name.as_deref(),
        |workspace_name, parent_workspace_name| WorkspaceLineageRecord {
            workspace_name,
            parent_workspace_name,
            ..*value
        },
        accounting,
    )
}

pub(super) fn new_root<E: std::error::Error + 'static, A, O>(
    workspace: &Workspace<A, O>,
    generation: GenerationId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<WorkspaceLineageRecord>, WorkspaceLineageError<E>> {
    record(
        workspace.name().as_str(),
        None,
        |workspace_name, parent_workspace_name| WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: workspace.id(),
            workspace_name,
            parent_workspace_id: None,
            parent_workspace_name,
            creation_operation: None,
            fork_generation: generation,
            initial_generation: generation,
        },
        accounting,
    )
}

pub(super) fn new_child<E: std::error::Error + 'static, A, O>(
    parent: &Workspace<A, O>,
    destination: &crate::WorkspaceName,
    child: WorkspaceId,
    key: IdempotencyKey,
    generation: GenerationId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<WorkspaceLineageRecord>, WorkspaceLineageError<E>> {
    record(
        destination.as_str(),
        Some(parent.name().as_str()),
        |workspace_name, parent_workspace_name| WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: false,
            workspace_id: child,
            workspace_name,
            parent_workspace_id: Some(parent.id()),
            parent_workspace_name,
            creation_operation: Some(key.operation_id()),
            fork_generation: generation,
            initial_generation: generation,
        },
        accounting,
    )
}

pub(super) async fn promote<S: WorkspaceLineageStore>(
    graph: &WorkspaceGraph<S>,
    planned: &WorkspaceLineageRecord,
    initial: GenerationId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceLineageError<S::Error>> {
    let mut ready = copy(planned, accounting)?;
    ready.value.ready = true;
    ready.value.initial_generation = initial;
    let result = async {
        let existing = graph
            .store
            .load_accounted(ready.value.workspace_id, accounting)
            .await?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        let valid = lineage_shape_accounted(&existing.value, accounting)
            .map(|valid| valid && same_lineage(&existing.value, &ready.value));
        let finished = existing.value.ready;
        let revision = existing.value.revision;
        existing.retire(accounting)?;
        if !valid? {
            return Err(WorkspaceLineageError::ConflictingRegistration);
        }
        if finished {
            return Ok(());
        }
        ready.value.revision = revision
            .checked_add(1)
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        if graph
            .store
            .compare_accounted(ready.value.workspace_id, revision, &ready.value, accounting)
            .await?
        {
            return Ok(());
        }
        // There is one legal change after an immutable intent: promotion to
        // ready. A losing CAS observes that winner; it never starts a retry loop.
        let winner = graph
            .store
            .load_accounted(ready.value.workspace_id, accounting)
            .await?
            .ok_or(WorkspaceLineageError::IncompatibleState)?;
        let valid = lineage_shape_accounted(&winner.value, accounting)
            .map(|valid| valid && winner.value.ready && same_lineage(&winner.value, &ready.value));
        winner.retire(accounting)?;
        if valid? {
            Ok(())
        } else {
            Err(WorkspaceLineageError::ConflictingRegistration)
        }
    }
    .await;
    ready.retire(accounting)?;
    result
}
