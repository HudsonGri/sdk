//! Indexed, read-only publication ownership; never constructs a publisher.
use super::*;

/// Failure to establish publication ownership from the durable parent slot.
#[derive(Debug, Error)]
pub enum PublicationLookupError<E: std::error::Error + 'static> {
    /// The original durable store read failed.
    #[error("publication ownership read failed: {0}")]
    Store(E),
    /// The claimed operation is absent or its journal is malformed.
    #[error("publication ownership journal is missing or invalid")]
    InvalidJournal,
}

/// Whether discarding this context would retire an owned merge participant.
/// Work is bounded by two parent-slot reads and one journal read, independent
/// of the number of other agents or retained publications.
pub async fn context_has_publication<S: MultiRootPublicationStore>(
    store: &S,
    context: WorkspaceContextId,
    parent: Option<WorkspaceContextId>,
) -> Result<bool, PublicationLookupError<S::Error>> {
    if store
        .parent_operation(context)
        .await
        .map_err(PublicationLookupError::Store)?
        .is_some()
    {
        return Ok(true);
    }
    let Some(parent) = parent else {
        return Ok(false);
    };
    let Some(operation) = store
        .parent_operation(parent)
        .await
        .map_err(PublicationLookupError::Store)?
    else {
        return Ok(false);
    };
    let Some(journal) = store
        .load(operation)
        .await
        .map_err(PublicationLookupError::Store)?
    else {
        // The claim owns admission even before its journal exists.
        return Ok(true);
    };
    validate_journal(&journal).map_err(|()| PublicationLookupError::InvalidJournal)?;
    if journal.candidate.plan.parent_context_id != parent
        || journal.candidate.plan.operation_id != operation
    {
        return Err(PublicationLookupError::InvalidJournal);
    }
    Ok(journal.candidate.plan.child_context_id == context)
}

/// Loads the one publication owned by this parent's durable slot.
pub async fn parent_publication<S: MultiRootPublicationStore>(
    store: &S,
    parent: WorkspaceContextId,
) -> Result<Option<MultiRootPublication>, PublicationLookupError<S::Error>> {
    let Some(operation) = store
        .parent_operation(parent)
        .await
        .map_err(PublicationLookupError::Store)?
    else {
        return Ok(None);
    };
    let journal = store
        .load(operation)
        .await
        .map_err(PublicationLookupError::Store)?
        .ok_or(PublicationLookupError::InvalidJournal)?;
    validate_journal(&journal).map_err(|()| PublicationLookupError::InvalidJournal)?;
    if journal.candidate.plan.parent_context_id != parent
        || journal.candidate.plan.operation_id != operation
    {
        return Err(PublicationLookupError::InvalidJournal);
    }
    Ok(Some(journal))
}
