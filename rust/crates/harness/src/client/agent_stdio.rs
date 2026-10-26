//! Bounded native input over the shared agent client, without workflow files.

use super::{AgentTurn, HeadlessOutcome, LocalAgentClient, LocalAgentError};
use crate::agent::{ConversationEntry, IntentId, StatusRecord};
use futures::{FutureExt as _, Stream, StreamExt as _};
use serde::{Deserialize, Serialize};
use std::io::Write;

/// One JSONL prompt. The caller owns the stable identity across reconnects.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRequest {
    /// Exact retry identity, not a fresh identity minted by the transport.
    pub id: IntentId,
    /// Prompt staged through the host's bounded private-volume publisher.
    pub prompt: String,
}

/// The same output dialect for `-p` and JSONL mode. Immutable references and
/// the exact input receipt remain available alongside the displayed answer.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Reply<'a> {
    Settled {
        id: &'a IntentId,
        sequence: u64,
        entry: ConversationEntry,
        output: crate::executor::TurnOutput,
    },
    Blocked { id: &'a IntentId, inbox: u64, status: StatusRecord },
    SettlementUnavailable {
        id: &'a IntentId,
        inbox: u64,
        first_sequence: u64,
        status: Option<StatusRecord>,
    },
    Closed { id: &'a IntentId },
}

/// Submit a single bounded prompt through the canonical actor entry. A typed
/// Unknown retains its original commit request in the returned error.
pub async fn run_agent_prompt(
    client: &mut LocalAgentClient,
    id: &IntentId,
    prompt: &str,
    output: &mut impl Write,
) -> Result<HeadlessOutcome, LocalAgentError> {
    if prompt.is_empty() || prompt.len() > super::jsonl::MAXIMUM_REQUEST_BYTES {
        return Err(crate::Error::Invalid("agent prompt must be 1..=65536 bytes".into()).into());
    }
    let (reply, outcome) = match client.prompt_turn(id, prompt).await {
        Ok(AgentTurn { sequence, entry, output }) => (
            Reply::Settled { id, sequence, entry, output }, HeadlessOutcome::Completed,
        ),
        Err(LocalAgentError::Blocked { inbox, status }) => (
            Reply::Blocked { id, inbox, status }, HeadlessOutcome::Blocked,
        ),
        Err(LocalAgentError::SettlementUnavailable { inbox, first_sequence, status }) => (
            Reply::SettlementUnavailable { id, inbox, first_sequence, status },
            HeadlessOutcome::Indeterminate,
        ),
        Err(LocalAgentError::Closed) => (Reply::Closed { id }, HeadlessOutcome::Cancelled),
        Err(error) => return Err(error),
    };
    serde_json::to_writer(&mut *output, &reply)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(outcome)
}

/// Read one finite line, settle its exact input, then admit the next line.
/// This directly backpressures stdin: there is no unbounded accepted-work
/// queue. Once accepted work settles, drain immediately available complete
/// lines, then exit even if the producer keeps its input pipe open.
pub async fn run_agent_jsonl(
    client: &mut LocalAgentClient,
    mut input: impl Stream<Item = crate::Result<String>> + Unpin,
    output: &mut impl Write,
) -> Result<HeadlessOutcome, LocalAgentError> {
    let mut accepted = false;
    loop {
        let next = if accepted {
            input.next().now_or_never().flatten()
        } else {
            input.next().await
        };
        let Some(line) = next else { return Ok(HeadlessOutcome::Completed); };
        let line = line?;
        if line.len() > super::jsonl::MAXIMUM_REQUEST_BYTES {
            return Err(crate::Error::Invalid("agent request line exceeds byte bound".into()).into());
        }
        let request: AgentRequest = serde_json::from_str(&line)?;
        accepted = true;
        let outcome = run_agent_prompt(client, &request.id, &request.prompt, output).await?;
        if outcome != HeadlessOutcome::Completed {
            return Ok(outcome);
        }
    }
}
