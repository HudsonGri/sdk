//! Native agent input and immutable turn output; no workflow subscription.

use super::{AgentClient, AgentClientError, AgentEvent};
use crate::{
    Error,
    agent::{Control, Delivered, InboxRecord, IntentId, Sender},
    conversation::ContentResidencyVerifier,
    executor::TurnOutput,
    local::{LOCAL_SESSION_CREDENTIAL_SLOT, LocalHarness},
};
use std::sync::Arc;

/// Keep host, provider and client refusals typed through the native boundary.
#[derive(Debug, thiserror::Error)]
pub enum LocalAgentError {
    /// Filesystem, staging or host failure.
    #[error(transparent)]
    Harness(#[from] Error),
    /// Session admission, transport or record failure.
    #[error(transparent)]
    Client(#[from] AgentClientError),
    /// Original agent refusal, including exact unknown-commit custody.
    #[error(transparent)]
    Agent(#[from] crate::agent::Error),
    /// Original bounded JSON codec failure.
    #[error(transparent)]
    Encoding(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, LocalAgentError>;

/// A settled record and its bounded, authenticated output file.
pub struct AgentTurn {
    /// Sequence in the conversation, for pagination and render identity.
    pub sequence: u64,
    /// Exact record naming the immutable input and output.
    pub entry: crate::agent::ConversationEntry,
    /// Decoded output, read only for the selected turn.
    pub output: TurnOutput,
}

/// Native connection using the same AgentClient/WireSession as browser hosts.
/// No CommandSubscription, TaskVolume follow or execution machine is started.
pub struct LocalAgentClient {
    local: Arc<LocalHarness>,
    client: AgentClient,
    reader: Arc<dyn ContentResidencyVerifier>,
    maximum_output_bytes: u64,
}

impl LocalAgentClient {
    /// Attach the local root to its existing authenticated Streams pool.
    /// Explicit cursors come from the caller's retained page/window selection.
    pub async fn connect(
        local: Arc<LocalHarness>,
        conversation_from: u64,
        status_from: u64,
        maximum_output_bytes: u64,
    ) -> Result<Self> {
        if maximum_output_bytes == 0 || maximum_output_bytes > 16 * 1024 * 1024 {
            return Err(Error::Invalid("agent output byte limit must be 1..=16777216".into()).into());
        }
        let (_, root) = local.agents().await?;
        let pool = local.session_client().await?.clone();
        let client = AgentClient::new(
            pool,
            root.clone(),
            conversation_from,
            status_from,
            LOCAL_SESSION_CREDENTIAL_SLOT,
        )?;
        let reader = local.client_reader()?;
        Ok(Self { local, client, reader, maximum_output_bytes })
    }

    /// Borrow the shared fold; frames never enter an independent native cache.
    pub const fn client(&self) -> &AgentClient {
        &self.client
    }

    /// Stage and send one user intent through the LocalHarness actor entry.
    pub async fn prompt(&self, id: &IntentId, text: &str) -> Result<Delivered> {
        Ok(self.local.prompt(id, text).await?)
    }

    /// Cancel through the same inbox commit path, retaining the caller's intent.
    pub async fn cancel(&self, id: &IntentId) -> Result<Delivered> {
        let (agents, _) = self.local.agents().await?;
        let by = Sender::User { principal: "local".into() };
        Ok(agents.send(
            self.client.agent(),
            &by,
            &InboxRecord::Cancel(Control { by: by.clone(), id: id.clone() }),
        ).await?)
    }

    /// One canonical transition, with no timer, background reader or queue.
    pub async fn next(&mut self) -> Result<AgentEvent> {
        Ok(self.client.next().await?)
    }

    /// Read a selected settled turn. Filesystem authority and byte admission are
    /// enforced by the original content reader before decoding any JSON.
    pub async fn read_turn(
        &self,
        sequence: u64,
        entry: crate::agent::ConversationEntry,
    ) -> Result<AgentTurn> {
        let bytes = self.reader.read(&entry.output, self.maximum_output_bytes, None, None)?.await?;
        let output = serde_json::from_slice(&bytes)?;
        Ok(AgentTurn { sequence, entry, output })
    }

}
