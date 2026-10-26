//! The turn driver over the replaceable [`Executor`]: one agent turn is one
//! executor operation, journaled by the host and settled by the agent step.
//!
//! The executor's journal holds every intent before its effect, so a crash before
//! the step commit re-runs the same operation and the journal replays it. The
//! conversation is written only by the step commit (all-or-none with the inbox
//! cursor), from the records [`TurnHost::settle`] returns.

use super::{
    AgentOutcome, AgentState, Error, IntentId, Out, TurnDriver, TurnFuture, TurnInput, TurnStep,
    Wait,
};
use crate::{
    OperationId,
    conversation::FileRef,
    executor::{ExecutionJournal, ExecutionProgress, Executor, TurnInput as ExecutorInput, TurnOutput},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use std::sync::Arc;

/// One opened turn: the executor and journal bound to the agent, and its input.
pub struct OpenTurn {
    /// Executor bound to the agent's tools, volumes and policy.
    pub executor: Arc<dyn Executor>,
    /// Journal of this agent's turns.
    pub journal: Arc<dyn ExecutionJournal>,
    /// The executor input; on resume, the input the operation started with.
    pub input: ExecutorInput,
}

/// A settled turn's conversation records and result file.
pub struct Settled {
    /// Encoded conversation records, appended by the step commit.
    pub conversation: Vec<Vec<u8>>,
    /// Result file reported to the parent when the agent completes.
    pub result: Option<FileRef>,
}

/// Host services for agent turns: binding and conversation staging.
pub trait TurnHost: Send + Sync {
    /// Opens `turn` for `agent`. `input` is `None` when resuming a started turn.
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: Option<&'a TurnInput>,
    ) -> BoxFuture<'a, crate::Result<OpenTurn>>;

    /// Stages a completed turn's output and returns its conversation records.
    fn settle<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        output: TurnOutput,
    ) -> BoxFuture<'a, crate::Result<Settled>>;
}

/// [`TurnDriver`] running each turn as one executor operation.
pub struct ExecutorDriver<H> {
    host: H,
}

impl<H: TurnHost> ExecutorDriver<H> {
    /// Driver over `host`.
    pub const fn new(host: H) -> Self {
        Self { host }
    }
}

/// Operation identity of the turn `input` starts. Every input is unique within an
/// agent's inbox (its sender and intent form the Streams idempotency key, and a
/// child reports once), so this is stable across replays and distinct per turn,
/// including turns started by an alarm at an unchanged inbox cursor.
pub fn turn_id(agent: &AgentState, input: &TurnInput) -> Result<OperationId, Error> {
    let encoded =
        serde_json::to_vec(input).map_err(|_| Error::Record("turn input does not encode"))?;
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.turn.v1");
    hasher.update(agent.path.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(&encoded);
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    Ok(OperationId::from_bytes(bytes))
}

/// The turn a step continues, or a fresh one for new input.
fn current(agent: &AgentState, input: Option<&TurnInput>) -> Result<(OperationId, bool), Error> {
    match (&agent.turn, input) {
        (Some(bytes), None | Some(TurnInput::Approval { .. })) => {
            let bytes = <[u8; 16]>::try_from(bytes.as_slice())
                .map_err(|_| Error::Record("turn checkpoint is not an operation id"))?;
            Ok((OperationId::from_bytes(bytes), true))
        }
        (None, Some(input)) => Ok((turn_id(agent, input)?, false)),
        (Some(_), Some(_)) => Err(Error::Record("new input while a turn is in progress")),
        (None, None) => Err(Error::Record("resume without a turn in progress")),
    }
}

fn host_error(error: crate::Error) -> Error {
    Error::Turn(Box::new(error))
}

impl<H: TurnHost> TurnDriver for ExecutorDriver<H> {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        input: Option<TurnInput>,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let (turn, resumed) = current(agent, input.as_ref())?;
            let fresh = if resumed { None } else { input.as_ref() };
            let open = self.host.open(agent, turn, fresh).await.map_err(host_error)?;
            let progress = open
                .executor
                .execute_progress(open.input, &*open.journal)
                .await
                .map_err(host_error)?;
            let failed = |message: &str| TurnStep {
                checkpoint: None,
                outs: Vec::new(),
                wait: Wait::Done(AgentOutcome::Failed {
                    message: message.to_owned(),
                }),
            };
            Ok(match progress {
                ExecutionProgress::Ready(output) => {
                    let settled = self.host.settle(agent, turn, output).await.map_err(host_error)?;
                    let outs = settled.conversation.into_iter().map(Out::Conversation).collect();
                    // A child reports when its work is done; a root waits for input.
                    let wait = if agent.parent.is_some() && agent.queued.is_empty() {
                        Wait::Done(AgentOutcome::Succeeded {
                            result: settled.result,
                        })
                    } else {
                        Wait::Input
                    };
                    TurnStep {
                        checkpoint: None,
                        outs,
                        wait,
                    }
                }
                ExecutionProgress::Pending(approval) => TurnStep {
                    checkpoint: Some(turn.into_bytes().to_vec()),
                    outs: Vec::new(),
                    wait: Wait::Approval(IntentId::try_from(approval_intent(approval))?),
                },
                ExecutionProgress::Rejected(reason) => failed(&format!("approval {reason}")),
                ExecutionProgress::Failed(kind) => failed(kind.message()),
                ExecutionProgress::PreDispatchFailed(failure) => failed(failure.message()),
            })
        })
    }
}

/// Approval intent naming the executor's pending approval operation.
#[must_use]
pub fn approval_intent(operation: OperationId) -> String {
    let mut text = String::with_capacity(41);
    text.push_str("tool:");
    for byte in operation.into_bytes() {
        for nibble in [byte >> 4, byte & 0x0f] {
            text.extend(char::from_digit(u32::from(nibble), 16));
        }
    }
    text
}
