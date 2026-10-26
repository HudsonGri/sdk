//! One agent step: the pure transition from (state, input) to (state, effects, alarm).
//!
//! The step is deterministic given the turn driver's result, so a crash before the
//! actor checkpoint commits simply recomputes it. Its effects are lowered into one
//! composed commit by [`super::lower::lower`]; nothing is acknowledged before that
//! commit.

use super::{
    AgentOutcome, AgentPath, ChildDone, Error, ForkAdopted, InboxRecord, IntentId, Phase, Sender,
    StatusRecord, VolumeBinding,
};
use crate::conversation::FileRef;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
};

/// Most children an agent may have open at once.
pub const MAX_OPEN_CHILDREN: usize = 256;
/// Most queued inputs waiting for the turn.
pub const MAX_QUEUED: usize = 256;
/// Most archive requests sent to children in one step; the rest continue on the alarm.
pub const MAX_FANOUT: usize = 32;
/// Alarm deadline that is due immediately: continue in a fresh step.
pub const CONTINUE_NOW: u64 = 1;

/// Input queued for the turn, in inbox order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TurnInput {
    /// The adoption brief.
    Brief {
        /// Brief file.
        brief: FileRef,
    },
    /// A message.
    Message {
        /// Sender.
        from: Sender,
        /// Message identity.
        id: IntentId,
        /// Body.
        body: FileRef,
    },
    /// A child finished.
    ChildDone {
        /// Child and outcome.
        done: ChildDone,
    },
    /// A child delegated an approval request.
    ApprovalRequested {
        /// The child.
        child: AgentPath,
        /// Request identity.
        id: IntentId,
    },
    /// A response file exists for this agent's approval request.
    Approval {
        /// Request identity.
        id: IntentId,
    },
    /// A host hook event.
    Hook {
        /// Host.
        host: String,
        /// Hook identity.
        id: IntentId,
        /// Event file.
        event: FileRef,
    },
}

/// Durable agent state, carried in the actor checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentState {
    /// This agent.
    pub path: AgentPath,
    /// Spawning agent.
    pub parent: Option<AgentPath>,
    /// Owned volumes.
    pub volumes: Vec<VolumeBinding>,
    /// Approval policy file.
    pub policy: Option<FileRef>,
    /// Exact conversation tail; the agent is its only writer.
    pub conversation_tail: u64,
    /// Exact status tail; the agent is its only writer.
    pub status_tail: u64,
    /// Last published phase.
    pub phase: Phase,
    /// Children that have not reported `ChildDone`.
    pub open_children: BTreeSet<AgentPath>,
    /// Open children that still have to receive `Archive`.
    pub archive_pending: BTreeSet<AgentPath>,
    /// Inputs waiting for the turn.
    pub queued: VecDeque<TurnInput>,
    /// Archived: no further turns, no further input accepted.
    pub closed: bool,
    /// `ChildDone` has been sent to the parent.
    pub reported: bool,
    /// Turn driver checkpoint while a turn is in progress.
    pub turn: Option<Vec<u8>>,
}

impl AgentState {
    /// State at adoption, before the first step.
    #[must_use]
    pub fn adopted(path: AgentPath, adopted: &ForkAdopted) -> Self {
        Self {
            path,
            parent: adopted.parent.clone(),
            volumes: adopted.volumes.clone(),
            policy: adopted.policy.clone(),
            conversation_tail: adopted.conversation_tail,
            status_tail: 0,
            phase: Phase::Idle,
            open_children: BTreeSet::new(),
            archive_pending: BTreeSet::new(),
            queued: VecDeque::new(),
            closed: false,
            reported: false,
            turn: None,
        }
    }
}

/// An effect of one step, lowered into the step's single commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Out {
    /// Append to this agent's conversation.
    Conversation(Vec<u8>),
    /// Send an inbox record to another agent.
    Send {
        /// Recipient.
        to: AgentPath,
        /// Record (never `ForkAdopted` or `Archive`).
        record: InboxRecord,
    },
    /// Archive another agent (a child).
    Archive {
        /// Target.
        target: AgentPath,
        /// Archive intent.
        id: IntentId,
    },
    /// Spawn a child.
    Spawn {
        /// Spawning intent; names the child.
        intent: IntentId,
        /// Task brief.
        brief: Option<FileRef>,
        /// Volumes bound to the child (filled by the filesystem contribution).
        volumes: Vec<VolumeBinding>,
    },
}

/// What the turn waits for after a turn step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wait {
    /// Run the next turn step at once.
    Continue,
    /// Wait for the response to this approval request.
    Approval(IntentId),
    /// Wait until this Unix millisecond deadline.
    Until(u64),
    /// The turn finished; wait for more input.
    Input,
    /// The agent's task is complete.
    Done(AgentOutcome),
}

/// The result of one turn step.
pub struct TurnStep {
    /// Driver checkpoint for the next step; ignored when the turn finished.
    pub checkpoint: Option<Vec<u8>>,
    /// Effects, in order.
    pub outs: Vec<Out>,
    /// What to wait for next.
    pub wait: Wait,
}

/// Future returned by a turn driver.
pub type TurnFuture<'a> = Pin<Box<dyn Future<Output = Result<TurnStep, Error>> + Send + 'a>>;

/// Executes turn steps (model calls, tool calls) for an agent.
///
/// A step may repeat after a crash; anything durable it does outside the returned
/// effects must be keyed by `(agent, cursor)` so the repeat replays exactly.
pub trait TurnDriver: Send + Sync {
    /// Runs one step. `input` is `None` when continuing or after an alarm.
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        cursor: u64,
        input: Option<TurnInput>,
    ) -> TurnFuture<'a>;
}

/// What woke the agent.
pub enum Wake {
    /// The inbox record at `sequence`.
    Inbox {
        /// Inbox sequence.
        sequence: u64,
        /// Decoded record.
        record: InboxRecord,
    },
    /// The actor alarm fired.
    Alarm,
}

/// Desired alarm after a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Alarm {
    /// No alarm.
    Clear,
    /// Fire at this Unix millisecond deadline ([`CONTINUE_NOW`] fires at once).
    At(u64),
}

/// One committed step: new state, ordered effects, desired alarm.
pub struct Step {
    /// Next state.
    pub state: AgentState,
    /// Effects, lowered into the same commit as the checkpoint.
    pub outs: Vec<Out>,
    /// Status appended by this step, if the phase changed.
    pub status: Option<StatusRecord>,
    /// Desired alarm.
    pub alarm: Alarm,
}

/// Runs one step for `wake`.
///
/// `state` is `None` only for the first delivery, which must be `ForkAdopted`.
pub async fn step<T: TurnDriver + ?Sized>(
    driver: &T,
    path: &AgentPath,
    state: Option<AgentState>,
    cursor: u64,
    wake: Wake,
) -> Result<Step, Error> {
    let mut outs = Vec::new();
    let (mut state, input) = match (state, wake) {
        (None, Wake::Inbox { record: InboxRecord::ForkAdopted(adopted), .. }) => {
            if adopted.agent != path.id() {
                return Err(Error::Record("ForkAdopted names another agent"));
            }
            let state = AgentState::adopted(path.clone(), &adopted);
            (state, adopted.brief.map(|brief| TurnInput::Brief { brief }))
        }
        (None, _) => return Err(Error::Record("first agent input is not ForkAdopted")),
        (Some(state), Wake::Alarm) => (state, None),
        (Some(state), Wake::Inbox { record, .. }) => admit(state, record)?,
    };
    let previous = state.phase.clone();
    if let Some(input) = input {
        if state.queued.len() >= MAX_QUEUED {
            return Err(Error::Record("agent input queue is full"));
        }
        state.queued.push_back(input);
    }
    let mut alarm = Alarm::Clear;
    if state.closed {
        alarm = cascade(&mut state, &mut outs)?;
    } else if let Some(wait) = run(driver, &mut state, cursor, &mut outs).await? {
        alarm = settle(&mut state, wait, &mut outs);
    }
    track_children(&mut state, &outs)?;
    let status = (state.phase != previous).then(|| StatusRecord {
        phase: state.phase.clone(),
        cursor,
    });
    let appended = outs
        .iter()
        .filter(|out| matches!(out, Out::Conversation(_)))
        .count() as u64;
    state.conversation_tail += appended;
    state.status_tail += u64::from(status.is_some());
    Ok(Step {
        state,
        outs,
        status,
        alarm,
    })
}

/// Folds a non-turn record into the state; returns the turn input it carries.
fn admit(
    mut state: AgentState,
    record: InboxRecord,
) -> Result<(AgentState, Option<TurnInput>), Error> {
    let input = match record {
        InboxRecord::ForkAdopted(_) => return Err(Error::Record("agent adopted twice")),
        InboxRecord::Message(message) => Some(TurnInput::Message {
            from: message.from,
            id: message.id,
            body: message.body,
        }),
        InboxRecord::ChildDone(done) => {
            state.open_children.remove(&done.child);
            state.archive_pending.remove(&done.child);
            (!state.closed).then_some(TurnInput::ChildDone { done })
        }
        InboxRecord::Approval(approval) => Some(TurnInput::Approval { id: approval.id }),
        InboxRecord::ApprovalRequested(request) => Some(TurnInput::ApprovalRequested {
            child: request.child,
            id: request.id,
        }),
        InboxRecord::Hook(hook) => Some(TurnInput::Hook {
            host: hook.host,
            id: hook.id,
            event: hook.event,
        }),
        InboxRecord::Cancel(_) => {
            state.turn = None;
            state.queued.clear();
            if !matches!(state.phase, Phase::Done { .. }) {
                state.phase = Phase::Idle;
            }
            None
        }
        InboxRecord::Archive(_) => {
            state.closed = true;
            state.turn = None;
            state.queued.clear();
            state.archive_pending = state.open_children.clone();
            None
        }
    };
    if state.closed {
        return Ok((state, None));
    }
    Ok((state, input))
}

/// Runs at most one turn step. Returns `None` when there was nothing to run.
async fn run<T: TurnDriver + ?Sized>(
    driver: &T,
    state: &mut AgentState,
    cursor: u64,
    outs: &mut Vec<Out>,
) -> Result<Option<Wait>, Error> {
    if matches!(state.phase, Phase::Done { .. }) {
        state.queued.clear();
        return Ok(None);
    }
    let input = match &state.phase {
        // A waiting turn resumes only on its own approval; other input queues.
        Phase::WaitingApproval { id } => {
            let Some(position) = state
                .queued
                .iter()
                .position(|input| matches!(input, TurnInput::Approval { id: queued } if queued == id))
            else {
                return Ok(None);
            };
            state.queued.remove(position)
        }
        Phase::Running | Phase::WaitingAlarm { .. } if state.turn.is_some() => None,
        _ => match state.queued.pop_front() {
            Some(input) => Some(input),
            None => return Ok(None),
        },
    };
    let turn = driver.step(state, cursor, input).await?;
    outs.extend(turn.outs);
    state.turn = match turn.wait {
        Wait::Input | Wait::Done(_) => None,
        _ => turn.checkpoint,
    };
    Ok(Some(turn.wait))
}

/// Publishes the phase implied by `wait` and the desired alarm.
fn settle(state: &mut AgentState, wait: Wait, outs: &mut Vec<Out>) -> Alarm {
    match wait {
        Wait::Continue => {
            state.phase = Phase::Running;
            Alarm::At(CONTINUE_NOW)
        }
        Wait::Approval(id) => {
            state.phase = Phase::WaitingApproval { id };
            Alarm::Clear
        }
        Wait::Until(deadline_unix_ms) => {
            state.phase = Phase::WaitingAlarm { deadline_unix_ms };
            Alarm::At(deadline_unix_ms.max(CONTINUE_NOW))
        }
        Wait::Input => {
            state.phase = Phase::Idle;
            if state.queued.is_empty() {
                Alarm::Clear
            } else {
                Alarm::At(CONTINUE_NOW)
            }
        }
        Wait::Done(outcome) => {
            report(state, outcome, outs);
            Alarm::Clear
        }
    }
}

/// Archive cascade: bounded fan-out to open children, then one report upward.
fn cascade(state: &mut AgentState, outs: &mut Vec<Out>) -> Result<Alarm, Error> {
    let batch: Vec<AgentPath> = state.archive_pending.iter().take(MAX_FANOUT).cloned().collect();
    for child in batch {
        state.archive_pending.remove(&child);
        outs.push(Out::Archive {
            id: IntentId::try_from(format!("cascade:{}", state.path.id()))?,
            target: child,
        });
    }
    if !state.archive_pending.is_empty() {
        return Ok(Alarm::At(CONTINUE_NOW));
    }
    if !state.reported {
        report(state, AgentOutcome::Archived, outs);
    }
    Ok(Alarm::Clear)
}

/// Terminal: publish `Done` and send `ChildDone` to the parent exactly once.
fn report(state: &mut AgentState, outcome: AgentOutcome, outs: &mut Vec<Out>) {
    state.phase = Phase::Done {
        outcome: outcome.clone(),
    };
    state.queued.clear();
    state.turn = None;
    if state.reported {
        return;
    }
    state.reported = true;
    if let Some(parent) = &state.parent {
        outs.push(Out::Send {
            to: parent.clone(),
            record: InboxRecord::ChildDone(ChildDone {
                child: state.path.clone(),
                outcome,
            }),
        });
    }
}

/// Records spawned children as open.
fn track_children(state: &mut AgentState, outs: &[Out]) -> Result<(), Error> {
    for out in outs {
        if let Out::Spawn { intent, .. } = out {
            let child = state
                .path
                .child(&super::AgentId::derive(&state.path.id(), intent.as_str()))?;
            if state.open_children.len() >= MAX_OPEN_CHILDREN && !state.open_children.contains(&child)
            {
                return Err(Error::Record("agent has too many open children"));
            }
            state.open_children.insert(child);
        }
    }
    Ok(())
}
