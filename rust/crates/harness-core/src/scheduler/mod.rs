//! Deterministic durable scheduling and structured concurrency.

mod apply;
mod declarations;
mod events;
mod inbox;
mod orchestration;
mod physical;
mod sessions;
mod state;
#[cfg(test)]
mod tests;
mod validation;

pub use declarations::{
    DurableOwner, EntrypointRef, LeaseFence, OperationSpec, Orchestration, ParentLink, Reservation,
    ResourceRequest, SessionLimits,
};
pub use events::SchedulerEvent;
pub use inbox::InboxItem;
pub use orchestration::{
    AssemblyKind, OrchestrationDecision, assembly_invocation_digest, reduction_invocation_digest,
};
pub use state::{OperationPhase, OperationState, WorkflowSuspension};
pub(crate) use validation::require_execution_owner;

use crate::store::HistoryIo;
use crate::wire::ErrorCode::{Conflict, Invalid, NotFound, Unauthorized, Unsupported};
use crate::{
    Error, OperationId, Outcome, Result, TaskId, conversation::FileRef, core::Authority,
    resources::CheckpointRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
struct TaskModelClaims {
    ceiling: u64,
    used: u64,
    attempts: BTreeMap<OperationId, BTreeMap<u32, [u8; 32]>>,
}
impl TaskModelClaims {
    fn used(&self) -> u64 {
        self.used
    }
}

// Immutable ancestry and root-local counters are projected by the same event
// application as operations. No caller maintains or reconstructs a second tree.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
struct TreeEntry {
    root: OperationId,
    depth: u32,
    total: u64,
    active: u64,
    model_steps: u64,
    first_completed: Option<OperationId>,
    last_completed: Option<OperationId>,
    next_completed: Option<OperationId>,
}

/// Pure projection of dependency, capacity, ownership and cancellation events.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
pub struct Scheduler {
    operations: BTreeMap<OperationId, OperationState>,
    child_slots: BTreeMap<OperationId, BTreeMap<String, OperationId>>,
    tree: BTreeMap<OperationId, TreeEntry>,
    // Test-only event trace: production completion order lives in parent links.
    #[cfg(test)]
    completion_order: Vec<OperationId>,
    #[serde(default)]
    sessions: BTreeMap<OperationId, SessionLimits>,
    #[serde(default)]
    model_claims: BTreeMap<OperationId, TaskModelClaims>,
}
impl Scheduler {
    /// Creates an empty projection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: BTreeMap::new(),
            child_slots: BTreeMap::new(),
            tree: BTreeMap::new(),
            #[cfg(test)]
            completion_order: Vec::new(),
            sessions: BTreeMap::new(),
            model_claims: BTreeMap::new(),
        }
    }
    /// Returns one operation's current projection.
    #[must_use]
    pub fn operation(&self, id: OperationId) -> Option<&OperationState> {
        self.operations.get(&id)
    }
    /// Structured children in stable slot order.
    pub fn children(&self, parent: OperationId) -> impl Iterator<Item = (&str, &OperationState)> {
        self.children_after(parent, None)
    }

    /// Seeks directly past a parent-local cursor; callers bound each page with
    /// `take` without visiting earlier pages or unrelated tasks.
    pub fn children_after<'a>(
        &'a self,
        parent: OperationId,
        after: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a str, &'a OperationState)> + 'a {
        use std::ops::Bound::{Excluded, Unbounded};
        let start = after.map_or(Unbounded, Excluded);
        self.child_slots
            .get(&parent)
            .into_iter()
            .flat_map(move |slots| slots.range::<str, _>((start, Unbounded)))
            .filter_map(|(slot, child)| {
                self.operations
                    .get(child)
                    .map(|state| (slot.as_str(), state))
            })
    }

    /// Looks up a parent-local child reference without enumerating siblings.
    #[must_use]
    pub fn child(&self, parent: OperationId, slot: &str) -> Option<&OperationState> {
        self.child_slots
            .get(&parent)?
            .get(slot)
            .and_then(|id| self.operation(*id))
    }

    /// Resolves an immutable direct parent without walking the tree.
    #[must_use]
    pub fn parent(&self, child: OperationId) -> Option<&OperationState> {
        self.operation(child)?
            .spec
            .parent
            .as_ref()
            .and_then(|parent| self.operation(parent.operation_id))
    }

    fn completed_children(&self, parent: OperationId) -> impl Iterator<Item = &OperationState> {
        std::iter::successors(
            self.tree
                .get(&parent)
                .and_then(|entry| entry.first_completed),
            |id| self.tree.get(id).and_then(|entry| entry.next_completed),
        )
        .filter_map(|id| self.operation(id))
    }
}

fn observe(work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}
pub(crate) fn diagnostic(
    work: &Option<&mut dyn HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_ref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        Unauthorized => Error::Unauthorized(reason.into()),
        Conflict => Error::Conflict(reason.into()),
        NotFound => Error::NotFound(reason.into()),
        Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}
fn revision(revision: u64, work: &Option<&mut dyn HistoryIo>) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| diagnostic(work, Invalid, "revision exhausted"))
}
