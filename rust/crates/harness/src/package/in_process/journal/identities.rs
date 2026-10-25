//! Projections of the existing journal encoding; payloads remain in its records.
use super::*;

#[derive(Deserialize)]
pub(super) struct Admission {
    pub bootstrap: Option<Bootstrap>,
}

#[derive(Deserialize)]
pub(super) struct Entry {
    pub event: Option<EventIdentity>,
    pub workflow: Option<Workflow>,
}

#[derive(Deserialize)]
pub(super) struct EventIdentity {
    pub operation_id: OperationId,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Deserialize)]
pub(super) struct Workflow {
    pub operation_id: OperationId,
    pub transition: Transition,
}

#[derive(Deserialize)]
pub(super) struct Transition {
    pub commands: Vec<Command>,
}

#[derive(Deserialize)]
pub(super) struct Command {
    pub operation_id: OperationId,
}

#[derive(Deserialize)]
pub(super) struct ReplayBounds {
    pub bootstrap: Option<PackageIdentity>,
    pub snapshot: SnapshotRevision,
}

#[derive(Deserialize)]
pub(super) struct PackageIdentity {
    pub manifest: Manifest,
}

#[derive(Deserialize)]
pub(super) struct SnapshotRevision {
    pub checkpoint: Revision,
}

#[derive(Deserialize)]
pub(super) struct Revision {
    pub revision: u64,
}

#[derive(Deserialize)]
pub(super) struct ReplayEntry {
    pub workflow: Option<WorkflowRecord>,
}
