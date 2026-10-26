//! The executor turn driver: one turn is one executor operation, resumed by identity.

use super::{Result, adopted, child_of, id, intent};
use acyclic_harness_core::{
    Error as CoreError, InteractionRejection, OperationId,
    agent::*,
    conversation::FileRef,
    executor::{
        ExecutionClaim, ExecutionEvent, ExecutionJournal, ExecutionProgress, ExecutionRecord,
        Executor, PreparationIo, TurnInput as ExecutorInput, TurnOutput,
    },
    interaction::{Interaction, InteractionOutcome},
    model::ModelContent,
};
use acyclic_stream::{BoxProviderFuture as BoxFuture, OwnedBytes};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

/// Journal the scripted executor never touches; every method refuses.
struct Unused;

fn unused<T>() -> acyclic_harness_core::Result<T> {
    Err(CoreError::Unsupported("unused test journal".into()))
}

impl ExecutionJournal for Unused {
    fn claim_execution<'a>(
        &'a self,
        _: OperationId,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<ExecutionClaim<'a>> {
        unused()
    }

    fn replay<'a>(
        &'a self,
        _: OperationId,
        _: u64,
        _: u32,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>,
    > {
        unused()
    }

    fn append<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: &'a ExecutionEvent,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<acyclic_harness_core::conversation::BoundedContentFuture<'a, ()>>
    {
        unused()
    }

    fn stage<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: &'a [u8],
        _: &'static str,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, FileRef>,
    > {
        unused()
    }

    fn load<'a>(
        &'a self,
        _: &'a FileRef,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, OwnedBytes>,
    > {
        unused()
    }

    fn open_interaction<'a>(
        &'a self,
        _: acyclic_harness_core::InteractionId,
        _: &'a Interaction,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<acyclic_harness_core::conversation::BoundedContentFuture<'a, ()>>
    {
        unused()
    }

    fn interaction_outcome<'a>(
        &'a self,
        _: acyclic_harness_core::InteractionId,
        _: Option<&'a PreparationIo>,
    ) -> acyclic_harness_core::Result<
        acyclic_harness_core::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>,
    > {
        unused()
    }
}

/// Executor replaying a script of progress results, recording each operation.
#[derive(Default)]
struct Script {
    progress: Mutex<VecDeque<ExecutionProgress<TurnOutput>>>,
    operations: Mutex<Vec<OperationId>>,
}

impl Executor for Script {
    fn execute<'a>(
        &'a self,
        _: ExecutorInput,
        _: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<TurnOutput>> {
        Box::pin(async { unused() })
    }

    fn execute_progress<'a>(
        &'a self,
        input: ExecutorInput,
        _: &'a dyn ExecutionJournal,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<ExecutionProgress<TurnOutput>>> {
        self.operations.lock().expect("lock").push(input.operation_id);
        let next = self.progress.lock().expect("lock").pop_front();
        Box::pin(async move { next.map_or_else(unused, Ok) })
    }
}

/// Host binding every turn to the script; records whether each open was fresh.
struct Host {
    script: Arc<Script>,
    fresh: Mutex<Vec<bool>>,
}

impl TurnHost for Host {
    fn open<'a>(
        &'a self,
        _: &'a AgentState,
        turn: OperationId,
        input: Option<&'a TurnInput>,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<OpenTurn>> {
        self.fresh.lock().expect("lock").push(input.is_some());
        let open = OpenTurn {
            executor: self.script.clone(),
            journal: Arc::new(Unused),
            input: ExecutorInput {
                operation_id: turn,
                input: ModelContent::Text("input".into()),
                selected_context: None,
                max_steps: 4,
            },
        };
        Box::pin(async move { Ok(open) })
    }

    fn settle<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        output: TurnOutput,
    ) -> BoxFuture<'a, acyclic_harness_core::Result<Settled>> {
        let settled = Settled {
            conversation: vec![output.text.into_bytes()],
            result: None,
        };
        Box::pin(async move { Ok(settled) })
    }
}

fn driver(progress: Vec<ExecutionProgress<TurnOutput>>) -> (Arc<Script>, ExecutorDriver<Host>) {
    let script = Arc::new(Script {
        progress: Mutex::new(progress.into()),
        ..Script::default()
    });
    let host = Host {
        script: script.clone(),
        fresh: Mutex::new(Vec::new()),
    };
    (script, ExecutorDriver::new(host))
}

fn ready(text: &str) -> ExecutionProgress<TurnOutput> {
    ExecutionProgress::Ready(TurnOutput {
        text: text.into(),
        attachments: Vec::new(),
        metadata: serde_json::Value::Null,
        steps: 1,
    })
}

/// A turn-starting input that carries no file: a child's approval request.
fn request(id: &str) -> Result<(InboxRecord, TurnInput)> {
    let child = AgentPath::root(&super::id(1)).child(&super::id(7))?;
    Ok((
        InboxRecord::ApprovalRequested(ApprovalRequested {
            child: child.clone(),
            id: intent(id),
        }),
        TurnInput::ApprovalRequested {
            child,
            id: intent(id),
        },
    ))
}

#[tokio::test]
async fn a_child_turn_settles_into_the_conversation_and_reports() -> Result {
    let parent = AgentPath::root(&id(1));
    let (path, state) = child_of(&parent, 2)?;
    let (script, driver) = driver(vec![ready("answer")]);
    let (record, started) = request("m1")?;
    let wake = Wake::Inbox {
        sequence: 2,
        record,
    };
    let done = step(&driver, &path, Some(state.clone()), 2, wake).await?;
    assert!(done.outs.contains(&Out::Conversation(b"answer".to_vec())));
    assert!(done.outs.iter().any(
        |out| matches!(out, Out::Send { to, record: InboxRecord::ChildDone(_) } if *to == parent)
    ));
    assert_eq!(
        done.state.phase,
        Phase::Done {
            outcome: AgentOutcome::Succeeded { result: None }
        }
    );
    assert_eq!(done.state.turn, None);
    assert_eq!(
        script.operations.lock().expect("lock").as_slice(),
        [turn_id(&state, &started)?]
    );
    Ok(())
}

#[tokio::test]
async fn an_approval_resumes_the_operation_it_paused() -> Result {
    let path = AgentPath::root(&id(1));
    let state = AgentState::adopted(path.clone(), &adopted(id(1)));
    let approval = OperationId::from_bytes([9; 16]);
    let (script, driver) = driver(vec![ExecutionProgress::Pending(approval), ready("after")]);
    let first = step(
        &driver,
        &path,
        Some(state),
        1,
        Wake::Inbox {
            sequence: 1,
            record: request("m1")?.0,
        },
    )
    .await?;
    let waiting = intent(&approval_intent(approval));
    assert_eq!(
        first.state.phase,
        Phase::WaitingApproval {
            id: waiting.clone()
        }
    );
    assert!(first.state.turn.is_some());
    // An unrelated approval does not resume the paused operation.
    let other = step(
        &driver,
        &path,
        Some(first.state.clone()),
        2,
        Wake::Inbox {
            sequence: 2,
            record: InboxRecord::Approval(Approval { id: intent("tool:other") }),
        },
    )
    .await?;
    assert_eq!(script.operations.lock().expect("lock").len(), 1);
    assert_eq!(other.state.phase, first.state.phase);
    // The matching approval resumes the same operation, not a new turn.
    let resumed = step(
        &driver,
        &path,
        Some(other.state),
        3,
        Wake::Inbox {
            sequence: 3,
            record: InboxRecord::Approval(Approval { id: waiting }),
        },
    )
    .await?;
    let operations = script.operations.lock().expect("lock").clone();
    assert_eq!(operations.len(), 2);
    assert_eq!(operations[0], operations[1]);
    assert_eq!(resumed.state.phase, Phase::Idle);
    assert_eq!(resumed.state.turn, None);
    assert!(resumed.outs.contains(&Out::Conversation(b"after".to_vec())));
    Ok(())
}

#[tokio::test]
async fn a_refused_approval_fails_the_agent() -> Result {
    let path = AgentPath::root(&id(1));
    let state = AgentState::adopted(path.clone(), &adopted(id(1)));
    let (_, driver) = driver(vec![ExecutionProgress::Rejected(InteractionRejection::Declined)]);
    let failed = step(
        &driver,
        &path,
        Some(state),
        1,
        Wake::Inbox {
            sequence: 1,
            record: request("m1")?.0,
        },
    )
    .await?;
    assert_eq!(
        failed.state.phase,
        Phase::Done {
            outcome: AgentOutcome::Failed {
                message: "approval declined".into()
            }
        }
    );
    Ok(())
}

#[test]
fn distinct_inputs_start_distinct_turns() -> Result {
    let path = AgentPath::root(&id(1));
    let state = AgentState::adopted(path, &adopted(id(1)));
    let input = |id: &str| -> Result<TurnInput> { Ok(request(id)?.1) };
    assert_eq!(turn_id(&state, &input("a")?)?, turn_id(&state, &input("a")?)?);
    assert_ne!(turn_id(&state, &input("a")?)?, turn_id(&state, &input("b")?)?);
    Ok(())
}
