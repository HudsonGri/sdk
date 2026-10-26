//! Process-local operation journal and observation history.
//!
//! Prized makes create and fork idempotent on its side (an `Idempotency-Key` replays the first
//! answer, even after the parent moved on), but it has no operation objects, no state events,
//! and forgets a sandbox once it is deleted. This registry supplies those: one record per
//! idempotency key bound to the digest of the intent it admitted, the sandboxes it made or
//! acted on, its phase and terminal outcome; plus the last observation and the state-event
//! history of every machine, which serve `events` and post-destroy `inspect_machine`.
//!
//! It mirrors the Daytona provider's registry. A restarted provider recovers create, fork and
//! checkpoint outcomes from sandbox labels and Prized's own idempotency records (see
//! `PrizedProvider::recover`); other outcomes are lost with the process.

use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use acyclic_machines::{
    CheckpointId, CheckpointObservation, EventFact, IdempotencyKey, MachineEvent, MachineId,
    MachineObservation, MachineState, MutationOutcome, OperationId, OperationObservation,
    OperationPhase, ProviderError, intent_digest,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// Most state events retained per machine before the oldest are dropped.
const MAX_EVENTS_PER_MACHINE: usize = 4_096;

/// One admitted mutation.
#[derive(Clone, Debug)]
pub struct OperationRecord {
    /// Operation identity derived from the key.
    pub id: OperationId,
    /// Idempotency key the operation was admitted under.
    pub key: IdempotencyKey,
    /// SHA-256 of the canonical intent, to detect key rebinding.
    pub intent_digest: [u8; 32],
    /// Sandboxes the operation made; cancelling or a definite failure deletes them.
    pub created: Vec<String>,
    /// Sandboxes the operation acted on or adopted from an earlier attempt; never deleted.
    pub targets: Vec<String>,
    /// Current phase.
    pub phase: OperationPhase,
    /// Terminal outcome once `phase` is `Succeeded`.
    pub outcome: Option<MutationOutcome>,
}

impl OperationRecord {
    /// Customer-visible view of the record.
    #[must_use]
    pub fn observation(&self) -> OperationObservation {
        OperationObservation {
            id: self.id,
            phase: self.phase,
        }
    }
}

/// Result of admitting an intent under a key.
#[derive(Clone, Debug)]
pub enum Admission {
    /// The key already succeeded with the same intent; replay this outcome.
    Replay(Box<MutationOutcome>),
    /// The key is new; the operation is pending under this id.
    Fresh(OperationId),
}

#[derive(Default)]
struct Inner {
    operations: BTreeMap<OperationId, OperationRecord>,
    machines: BTreeMap<MachineId, MachineObservation>,
    events: BTreeMap<MachineId, Vec<MachineEvent>>,
    checkpoints: BTreeMap<CheckpointId, CheckpointObservation>,
}

/// Registry keyed by idempotency key; see the module documentation.
#[derive(Default)]
pub struct OperationRegistry {
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for OperationRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.lock();
        formatter
            .debug_struct("OperationRegistry")
            .field("operations", &inner.operations.len())
            .field("machines", &inner.machines.len())
            .field("checkpoints", &inner.checkpoints.len())
            .finish()
    }
}

/// Operation identity derived deterministically from an idempotency key.
#[must_use]
pub fn operation_id(key: IdempotencyKey) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"acyclic-machines-prized/operation");
    hash.update(key.as_bytes());
    OperationId::from_digest(hash.finalize().into())
}

impl OperationRegistry {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn with_record<T>(
        &self,
        operation: OperationId,
        action: impl FnOnce(&mut OperationRecord) -> T,
    ) -> Option<T> {
        self.lock().operations.get_mut(&operation).map(action)
    }

    /// Admits `intent` under `key`.
    ///
    /// # Errors
    /// - [`ProviderError::Conflict`] when the key is bound to another intent.
    /// - [`ProviderError::Indeterminate`] while the key's operation is pending or unresolved.
    /// - [`ProviderError::Failed`] / [`ProviderError::Cancelled`] once it ended that way.
    pub fn admit<T: Serialize>(
        &self,
        key: IdempotencyKey,
        intent: &T,
    ) -> Result<Admission, ProviderError> {
        let digest = intent_digest(intent)?;
        let id = operation_id(key);
        let mut inner = self.lock();
        if let Some(record) = inner.operations.get(&id) {
            if record.intent_digest != digest {
                return Err(ProviderError::Conflict(
                    "idempotency key is bound to another intent".into(),
                ));
            }
            return match (record.phase, &record.outcome) {
                (OperationPhase::Succeeded, Some(outcome)) => {
                    Ok(Admission::Replay(Box::new(outcome.clone())))
                }
                (OperationPhase::Failed, _) => Err(ProviderError::Failed),
                (OperationPhase::Cancelled, _) => Err(ProviderError::Cancelled),
                _ => Err(ProviderError::Indeterminate(key)),
            };
        }
        inner.operations.insert(
            id,
            OperationRecord {
                id,
                key,
                intent_digest: digest,
                created: Vec::new(),
                targets: Vec::new(),
                phase: OperationPhase::Pending,
                outcome: None,
            },
        );
        Ok(Admission::Fresh(id))
    }

    /// Records a sandbox the pending operation made (`adopted` false) or adopted from an
    /// earlier attempt under its key (`adopted` true). Returns `false`, recording nothing, once
    /// the operation is no longer pending: it was cancelled while the request was in flight,
    /// and the caller owns any sandbox it made.
    #[must_use]
    pub fn bind(&self, operation: OperationId, sandbox_id: &str, adopted: bool) -> bool {
        self.with_record(operation, |record| {
            if record.phase != OperationPhase::Pending {
                return false;
            }
            let list = if adopted {
                &mut record.targets
            } else {
                &mut record.created
            };
            list.push(sandbox_id.to_owned());
            true
        })
        .unwrap_or(false)
    }

    /// Records an existing sandbox the operation acts on. Cancellation leaves it alone.
    pub fn bind_target(&self, operation: OperationId, sandbox_id: &str) {
        self.with_record(operation, |record| {
            record.targets.push(sandbox_id.to_owned());
        });
    }

    /// Marks the operation succeeded with `outcome`, unless it was cancelled meanwhile.
    pub fn complete(&self, operation: OperationId, outcome: MutationOutcome) {
        self.with_record(operation, |record| {
            if record.phase == OperationPhase::Pending {
                record.phase = OperationPhase::Succeeded;
                record.outcome = Some(outcome);
            }
        });
    }

    /// Marks the operation terminal in the non-success phase `error` implies.
    pub fn fail(&self, operation: OperationId, error: &ProviderError) {
        let phase = match error {
            ProviderError::Indeterminate(_) | ProviderError::Unavailable => {
                OperationPhase::Indeterminate
            }
            ProviderError::Cancelled => OperationPhase::Cancelled,
            _ => OperationPhase::Failed,
        };
        self.with_record(operation, |record| {
            if record.phase == OperationPhase::Pending {
                record.phase = phase;
            }
        });
    }

    /// Releases a key whose operation was refused before any Prized side effect, so a corrected
    /// request can reuse it.
    pub fn forget(&self, operation: OperationId) {
        self.lock().operations.remove(&operation);
    }

    /// Reads one operation.
    #[must_use]
    pub fn inspect(&self, operation: OperationId) -> Option<OperationObservation> {
        self.lock()
            .operations
            .get(&operation)
            .map(OperationRecord::observation)
    }

    /// Reads the full record of one operation.
    #[must_use]
    pub fn record(&self, operation: OperationId) -> Option<OperationRecord> {
        self.lock().operations.get(&operation).cloned()
    }

    /// Reads the record admitted under `key`.
    #[must_use]
    pub fn record_for_key(&self, key: IdempotencyKey) -> Option<OperationRecord> {
        self.record(operation_id(key))
    }

    /// Cancels an operation and hands back the sandboxes it *made*, for the caller to delete.
    /// A pending or indeterminate operation becomes cancelled; a failed or cancelled one keeps
    /// its phase but still hands back sandboxes an earlier rollback could not delete; a
    /// succeeded one is unchanged and hands back nothing.
    #[must_use]
    pub fn cancel(&self, operation: OperationId) -> Option<(OperationObservation, Vec<String>)> {
        self.with_record(operation, |record| match record.phase {
            OperationPhase::Succeeded => (record.observation(), Vec::new()),
            phase => {
                if matches!(
                    phase,
                    OperationPhase::Pending | OperationPhase::Indeterminate
                ) {
                    record.phase = OperationPhase::Cancelled;
                }
                (record.observation(), std::mem::take(&mut record.created))
            }
        })
    }

    /// Takes the sandboxes a failed operation made, for rollback.
    #[must_use]
    pub fn take_created(&self, operation: OperationId) -> Vec<String> {
        self.with_record(operation, |record| std::mem::take(&mut record.created))
            .unwrap_or_default()
    }

    /// Hands back sandboxes a rollback could not delete, so a later `cancel` retries them.
    pub fn restore_created(&self, operation: OperationId, sandboxes: Vec<String>) {
        self.with_record(operation, |record| record.created.extend(sandboxes));
    }

    /// Polls until the operation leaves `Pending` or `timeout` elapses.
    ///
    /// # Errors
    /// [`ProviderError::NotFound`] for an unknown operation; [`ProviderError::Indeterminate`]
    /// with its key when the timeout elapses.
    pub async fn watch(
        &self,
        operation: OperationId,
        poll: Duration,
        timeout: Duration,
    ) -> Result<OperationObservation, ProviderError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let record = self
                .record(operation)
                .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
            if record.phase != OperationPhase::Pending {
                return Ok(record.observation());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ProviderError::Indeterminate(record.key));
            }
            tokio::time::sleep(poll).await;
        }
    }

    /// Records the latest observation of a machine, appending a state event when its state
    /// differs from the last one recorded.
    pub fn observe_machine(&self, observation: MachineObservation, observed_at_unix_ms: u64) {
        let mut inner = self.lock();
        let id = observation.id;
        if inner.machines.get(&id).map(|value| value.state) != Some(observation.state) {
            let events = inner.events.entry(id).or_default();
            if events.len() >= MAX_EVENTS_PER_MACHINE {
                events.remove(0);
            }
            let sequence = events
                .last()
                .map_or(1, |event| event.sequence.saturating_add(1));
            events.push(MachineEvent {
                machine: id,
                sequence,
                observed_at_unix_ms,
                fact: EventFact::State(observation.state),
            });
        }
        inner.machines.insert(id, observation);
    }

    /// Last recorded observation of a machine.
    #[must_use]
    pub fn machine(&self, machine: MachineId) -> Option<MachineObservation> {
        self.lock().machines.get(&machine).cloned()
    }

    /// Records a machine as destroyed, keeping its last contract, and returns that observation.
    pub fn mark_destroyed(
        &self,
        machine: MachineId,
        observed_at_unix_ms: u64,
    ) -> Option<MachineObservation> {
        let mut observation = self.machine(machine)?;
        if observation.state != MachineState::Destroyed {
            observation.state = MachineState::Destroyed;
            observation.changed_at_unix_ms = observed_at_unix_ms;
            self.observe_machine(observation.clone(), observed_at_unix_ms);
        }
        Some(observation)
    }

    /// Every recorded machine, in identity order.
    #[must_use]
    pub fn machines(&self) -> Vec<MachineObservation> {
        self.lock().machines.values().cloned().collect()
    }

    /// Bounded page of recorded state events after `after_sequence`.
    #[must_use]
    pub fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> (Vec<MachineEvent>, Option<u64>) {
        let inner = self.lock();
        let mut values = inner
            .events
            .get(&machine)
            .into_iter()
            .flatten()
            .filter(|event| after_sequence.is_none_or(|cursor| event.sequence > cursor))
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let next = if values.len() > limit {
            values.pop();
            values.last().map(|event| event.sequence)
        } else {
            None
        };
        (values, next)
    }

    /// Records a checkpoint observation.
    pub fn remember_checkpoint(&self, checkpoint: CheckpointObservation) {
        self.lock().checkpoints.insert(checkpoint.id, checkpoint);
    }

    /// Last recorded observation of a checkpoint.
    #[must_use]
    pub fn checkpoint(&self, checkpoint: CheckpointId) -> Option<CheckpointObservation> {
        self.lock().checkpoints.get(&checkpoint).cloned()
    }

    /// Records that a checkpoint no longer accepts forks.
    pub fn mark_checkpoint_destroyed(&self, checkpoint: CheckpointId) {
        if let Some(value) = self.lock().checkpoints.get_mut(&checkpoint) {
            value.forkable = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(suffix: u8) -> IdempotencyKey {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000000{suffix:02x}")).unwrap()
    }

    #[test]
    fn admission_replays_success_and_rejects_rebinding() {
        let registry = OperationRegistry::default();
        let Admission::Fresh(id) = registry.admit(key(1), &"intent-a").unwrap() else {
            panic!("expected fresh")
        };
        assert!(matches!(
            registry.admit(key(1), &"intent-a"),
            Err(ProviderError::Indeterminate(_))
        ));
        assert!(matches!(
            registry.admit(key(1), &"intent-b"),
            Err(ProviderError::Conflict(_))
        ));
        let machine = MachineId::new();
        registry.complete(id, MutationOutcome::Suspended(machine));
        let Admission::Replay(outcome) = registry.admit(key(1), &"intent-a").unwrap() else {
            panic!("expected replay")
        };
        assert_eq!(*outcome, MutationOutcome::Suspended(machine));
        assert_ne!(operation_id(key(1)), operation_id(key(2)));
    }

    #[test]
    fn cancel_hands_back_only_what_the_operation_made() {
        let registry = OperationRegistry::default();
        let Admission::Fresh(id) = registry.admit(key(3), &"fork").unwrap() else {
            panic!("expected fresh")
        };
        assert!(registry.bind(id, "sbx_made", false));
        assert!(registry.bind(id, "sbx_adopted", true));
        registry.bind_target(id, "sbx_parent");
        let (observation, sandboxes) = registry.cancel(id).unwrap();
        assert_eq!(observation.phase, OperationPhase::Cancelled);
        assert_eq!(sandboxes, ["sbx_made"]);
        assert!(registry.cancel(id).unwrap().1.is_empty());
        assert!(
            !registry.bind(id, "sbx_late", false),
            "a sandbox made after cancellation goes back to the caller to delete"
        );
        registry.complete(id, MutationOutcome::Woken(MachineId::new()));
        assert_eq!(
            registry.inspect(id).unwrap().phase,
            OperationPhase::Cancelled
        );
    }

    #[test]
    fn failures_keep_what_rollback_could_not_delete_for_cancel() {
        let registry = OperationRegistry::default();
        let Admission::Fresh(id) = registry.admit(key(5), &"fork").unwrap() else {
            panic!("expected fresh")
        };
        assert!(registry.bind(id, "sbx_child", false));
        registry.fail(id, &ProviderError::Failed);
        assert_eq!(registry.take_created(id), ["sbx_child"]);
        registry.restore_created(id, vec!["sbx_child".to_owned()]);
        let (observation, sandboxes) = registry.cancel(id).unwrap();
        assert_eq!(observation.phase, OperationPhase::Failed);
        assert_eq!(sandboxes, ["sbx_child"]);
        assert!(matches!(
            registry.admit(key(5), &"fork"),
            Err(ProviderError::Failed)
        ));
    }
}
