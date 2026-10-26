//! Bounded native input over the shared agent client, without workflow files.

use super::{AgentTurn, HeadlessOutcome, LocalAgentClient, LocalAgentError, MAXIMUM_AGENT_PROMPT_BYTES};
use crate::agent::{ConversationEntry, IntentId, StatusRecord};
use futures::{FutureExt as _, Stream, StreamExt as _};
use prost::Message as _;
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
    Unknown {
        id: &'a IntentId,
        /// Canonical Streams CommitRequest protobuf bytes, not a second request
        /// schema. The original identity, conditions and payloads stay intact.
        commit: Vec<u8>,
    },
}

fn unknown_reply<'a>(id: &'a IntentId, request: &acyclic_stream::CommitRequest) -> Reply<'a> {
    Reply::Unknown { id, commit: acyclic_stream::wire_codec::commit_to_wire(request).encode_to_vec() }
}

/// Preserve exact Unknown custody when an interactive process ends. The caller
/// writes to its existing error channel after releasing the terminal; this adds
/// no outbox, recovery loop or automatic resubmission.
pub fn write_agent_unknown(error: &LocalAgentError, output: &mut impl Write) -> Result<bool, LocalAgentError> {
    let LocalAgentError::Unknown { id, request } = error else { return Ok(false); };
    write_reply(output, &unknown_reply(id, request))?;
    Ok(true)
}

fn write_reply(output: &mut impl Write, reply: &Reply<'_>) -> Result<(), LocalAgentError> {
    serde_json::to_writer(&mut *output, reply)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

/// Submit a single bounded prompt through the canonical actor entry. A typed
/// Unknown emits its identity and original canonical request with exit class
/// Indeterminate; it is never acknowledged or silently resubmitted.
pub async fn run_agent_prompt(
    client: &mut LocalAgentClient,
    id: &IntentId,
    prompt: &str,
    output: &mut impl Write,
) -> Result<HeadlessOutcome, LocalAgentError> {
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
        Err(LocalAgentError::Unknown { id: actual, request }) => {
            write_reply(output, &unknown_reply(&actual, &request))?;
            return Ok(HeadlessOutcome::Indeterminate);
        }
        Err(error) => return Err(error),
    };
    write_reply(output, &reply)?;
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
        if line.len() > MAXIMUM_AGENT_PROMPT_BYTES {
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

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath};

    #[test]
    fn unknown_process_receipt_preserves_original_commit_custody() {
        let path = StreamPath::new("harness/agents/01010101010101010101010101010101").unwrap();
        let request = CommitRequest {
            conditions: vec![CommitCondition::Absent { path: StreamPath::new(format!("{path}/closed")).unwrap() }],
            mutations: vec![CommitMutation::Append { path, records: vec![vec![0, 255, 128, 7].into()] }],
            idempotency_key: IdempotencyKey::new(vec![7; 32]).unwrap(),
        };
        let error = LocalAgentError::Unknown {
            id: IntentId::try_from("caller-owned-intent").unwrap(),
            request: Box::new(request.clone()),
        };
        let mut output = Vec::new();
        assert!(write_agent_unknown(&error, &mut output).unwrap());
        let reply: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(reply["kind"], "unknown");
        assert_eq!(reply["id"], "caller-owned-intent");
        let commit: Vec<u8> = serde_json::from_value(reply["commit"].clone()).unwrap();
        let recovered = acyclic_stream::wire_codec::commit_from_wire(
            acyclic_stream::wire::CommitRequest::decode(commit.as_slice()).unwrap(),
        ).unwrap();
        assert_eq!(recovered, request, "recovery must retain key, conditions and every mutation byte");
        // The former process boundary only printed Display. The same recovery
        // consumer cannot recover custody from that output.
        assert!(serde_json::from_str::<serde_json::Value>(&error.to_string()).is_err());
    }
}
