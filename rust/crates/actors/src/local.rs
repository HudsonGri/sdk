//! Single-node Actors runtime primitives over the canonical Stream transaction.
/// Shared readiness registration and hibernation handoff.
pub mod ready;
use crate::addressing::ActorReference;
use crate::guest::{AlarmChange, Checkpoint, Transition, Trigger};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitId, CommitMutation, CommitOutcome, CommitRequest,
    IdempotencyKey, ReadRequest, StreamError, StreamPath, StreamProvider,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// Typed failure at the shared actor publication boundary.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Canonical provider failure, including uncertain acknowledgement.
    #[error(transparent)]
    Stream(#[from] StreamError),
    /// Durable state encoding failed.
    #[error(transparent)]
    Codec(#[from] serde_json::Error),
    /// Another owner or transition changed the exact durable cut.
    #[error("actor epoch or checkpoint changed")]
    Conflict,
    /// The actor is paused; queued work remains unconsumed.
    #[error("actor is paused")]
    Paused,
    /// A request contradicts the durable state machine.
    #[error("invalid actor transition: {0}")]
    Invalid(&'static str),
}

/// Alarm identity and deadline are one durable value, including cleared alarms.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Alarm {
    generation: u64,
    deadline_unix_ms: Option<u64>,
    delivered_generation: Option<u64>,
}
impl Alarm {
    /// Generation increases on replacement and clear; Keep preserves it.
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    /// No deadline means cleared; its generation is still retained.
    pub const fn deadline_unix_ms(&self) -> Option<u64> {
        self.deadline_unix_ms
    }
}

/// One committed state/cursor/alarm tuple. Fields cannot be forged by callers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    epoch: u64,
    checkpoint: Option<Checkpoint>,
    cursors: BTreeMap<String, u64>,
    alarm: Alarm,
    paused: bool,
}
impl State {
    /// Current placement fence.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Last committed artifact checkpoint.
    pub const fn checkpoint(&self) -> Option<&Checkpoint> {
        self.checkpoint.as_ref()
    }
    /// Next unconsumed sequence of a declared subscription.
    pub fn cursor(&self, subscription: &str) -> Option<u64> {
        self.cursors.get(subscription).copied()
    }
    /// Current alarm, including its invalidation generation.
    pub const fn alarm(&self) -> &Alarm {
        &self.alarm
    }
    /// Whether execution is durably paused.
    pub const fn paused(&self) -> bool {
        self.paused
    }
}

/// Immutable exact-cut token. Publication compares both tail and commit anchor.
pub struct Snapshot {
    path: StreamPath,
    tail: u64,
    anchor: Option<CommitId>,
    state: Option<State>,
}
impl Snapshot {
    /// Complete retained tuple, absent only before placement.
    pub const fn state(&self) -> Option<&State> {
        self.state.as_ref()
    }
}

/// One storage mechanism for memory, durable local and hosted Stream providers.
pub struct Store<P: StreamProvider + ?Sized> {
    provider: Arc<P>,
}
impl<P: StreamProvider + ?Sized> Store<P> {
    /// Retain the caller's admitted provider session, without a private ledger.
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }

    /// Read only the final state record; recovery work is independent of history length.
    pub async fn load(&self, actor: &ActorReference) -> Result<Snapshot, Error> {
        let path = StreamPath::new(format!(
            "actors/{}/state",
            acyclic_auth::encode_component(&actor.encode())
        ))?;
        let tail = match self.provider.tail(path.clone()).await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(error.into()),
        };
        if tail == 0 {
            return Ok(Snapshot {
                path,
                tail,
                anchor: None,
                state: None,
            });
        }
        let mut records = self
            .provider
            .read(ReadRequest {
                path: path.clone(),
                from: tail - 1,
                limit: 1,
            })
            .await?;
        let record = records
            .try_next()
            .await?
            .ok_or(Error::Invalid("checkpoint history is incomplete"))?;
        if record.sequence != tail - 1 {
            return Err(Error::Invalid("checkpoint cursor changed"));
        }
        let state = serde_json::from_slice(&record.value)?;
        Ok(Snapshot {
            path,
            tail,
            anchor: Some(record.commit_id),
            state: Some(state),
        })
    }

    /// Establish a strictly newer epoch while preserving checkpoint and pending work.
    pub async fn claim(
        &self,
        previous: &Snapshot,
        epoch: u64,
        subscriptions: BTreeMap<String, u64>,
        key: IdempotencyKey,
    ) -> Result<(), Error> {
        if epoch == 0
            || previous
                .state
                .as_ref()
                .is_some_and(|state| epoch <= state.epoch)
        {
            return Err(Error::Conflict);
        }
        let state = match &previous.state {
            Some(state) => {
                if state.cursors.keys().ne(subscriptions.keys()) {
                    return Err(Error::Invalid("subscription set changed during placement"));
                }
                let mut state = state.clone();
                state.epoch = epoch;
                state
            }
            None => State {
                epoch,
                checkpoint: None,
                cursors: subscriptions,
                alarm: Alarm::default(),
                paused: false,
            },
        };
        self.commit(previous, state, Vec::new(), key).await
    }

    /// Pause/resume and in-flight publication share the identical CAS fence.
    pub async fn set_paused(
        &self,
        previous: &Snapshot,
        epoch: u64,
        paused: bool,
        key: IdempotencyKey,
    ) -> Result<(), Error> {
        let mut state = self.owned(previous, epoch)?.clone();
        state.paused = paused;
        self.commit(previous, state, Vec::new(), key).await
    }

    /// Atomically publish the checkpoint, consumed cursor, alarm and outbox.
    pub async fn publish(
        &self,
        previous: &Snapshot,
        epoch: u64,
        trigger: &Trigger,
        transition: Transition,
        key: IdempotencyKey,
    ) -> Result<(), Error> {
        let mut state = self.owned(previous, epoch)?.clone();
        if state.paused {
            return Err(Error::Paused);
        }
        match trigger {
            Trigger::Record {
                subscription,
                sequence,
            } => {
                let cursor = state
                    .cursors
                    .get_mut(subscription)
                    .ok_or(Error::Invalid("undeclared subscription"))?;
                if *cursor != *sequence {
                    return Err(Error::Conflict);
                }
                *cursor = cursor
                    .checked_add(1)
                    .ok_or(Error::Invalid("subscription cursor overflow"))?;
            }
            Trigger::Alarm {
                generation,
                now_unix_ms,
            } => {
                if state.alarm.generation != *generation
                    || state.alarm.delivered_generation == Some(*generation)
                    || !state
                        .alarm
                        .deadline_unix_ms
                        .is_some_and(|deadline| deadline <= *now_unix_ms)
                {
                    return Err(Error::Conflict);
                }
                // Delivery consumption is distinct from the desired alarm.
                // Keep preserves its generation/deadline without permitting a
                // second successful firing to publish another transition.
                state.alarm.delivered_generation = Some(*generation);
            }
        }
        let deadline = match transition.alarm {
            AlarmChange::Keep => None,
            AlarmChange::Clear => Some(None),
            AlarmChange::Set {
                deadline_unix_ms: 0,
            } => return Err(Error::Invalid("alarm deadline is zero")),
            AlarmChange::Set { deadline_unix_ms } => Some(Some(deadline_unix_ms)),
        };
        if let Some(deadline) = deadline {
            state.alarm.generation = state
                .alarm
                .generation
                .checked_add(1)
                .ok_or(Error::Invalid("alarm generation overflow"))?;
            state.alarm.deadline_unix_ms = deadline;
            state.alarm.delivered_generation = None;
        }
        state.checkpoint = Some(transition.checkpoint);
        let mut destinations: BTreeMap<StreamPath, (CommitCondition, Vec<AppendRecord>)> =
            BTreeMap::new();
        for effect in transition.effects {
            if effect.stream() == &previous.path {
                return Err(Error::Invalid("effect targets actor checkpoint stream"));
            }
            let destination = destinations
                .entry(effect.stream().clone())
                .or_insert_with(|| (effect.condition.clone(), Vec::new()));
            if destination.0 != effect.condition {
                return Err(Error::Invalid("outbox destination has inconsistent cuts"));
            }
            destination.1.push(effect.value.into());
        }
        let effects = destinations
            .into_iter()
            .map(|(path, (condition, records))| {
                (condition, CommitMutation::Append { path, records })
            })
            .collect();
        self.commit(previous, state, effects, key).await
    }

    fn owned<'a>(&self, previous: &'a Snapshot, epoch: u64) -> Result<&'a State, Error> {
        previous
            .state
            .as_ref()
            .filter(|state| state.epoch == epoch)
            .ok_or(Error::Conflict)
    }

    async fn commit(
        &self,
        previous: &Snapshot,
        state: State,
        effects: Vec<(CommitCondition, CommitMutation)>,
        key: IdempotencyKey,
    ) -> Result<(), Error> {
        let condition = match previous.anchor {
            Some(commit_id) => CommitCondition::Head {
                path: previous.path.clone(),
                expected: previous.tail,
                commit_id,
            },
            None => CommitCondition::Absent {
                path: previous.path.clone(),
            },
        };
        let (mut conditions, mut mutations): (Vec<_>, Vec<_>) = effects.into_iter().unzip();
        conditions.push(condition);
        mutations.push(CommitMutation::Append {
            path: previous.path.clone(),
            records: vec![serde_json::to_vec(&state)?.into()],
        });
        match self
            .provider
            .commit(CommitRequest {
                conditions,
                mutations,
                idempotency_key: key,
            })
            .await?
        {
            CommitOutcome::Committed(_) => Ok(()),
            CommitOutcome::Conflict(_) => Err(Error::Conflict),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
