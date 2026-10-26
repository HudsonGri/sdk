//! Native agent input and immutable turn output; no workflow subscription.

use super::{AgentClient, AgentClientError, AgentEvent};
use crate::{
    Error,
    agent::{Control, Delivered, InboxRecord, IntentId, Phase, Sender, StatusRecord, TurnInput},
    conversation::ContentResidencyVerifier,
    executor::TurnOutput,
    local::{LOCAL_SESSION_CREDENTIAL_SLOT, LocalHarness},
};
use std::sync::Arc;
use acyclic_stream::StreamProvider as _;

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
    /// Preserve the actual output device failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The selected agent is closed; no prompt was admitted.
    #[error("selected agent is closed")]
    Closed,
    /// The agent needs an external action before it can settle the prompt.
    #[error("agent prompt {inbox} is blocked: {status:?}")]
    Blocked {
        /// Exact submitted inbox sequence.
        inbox: u64,
        /// Observed lifecycle record, never an inferred idle state.
        status: StatusRecord,
    },
    /// No matching settlement exists in the selected page. The caller must
    /// backfill older pages or inspect the terminal outcome, not resubmit with
    /// a new identity or acknowledge an unrelated turn.
    #[error("agent prompt {inbox} has no settlement in the selected window from {first_sequence}: {status:?}")]
    SettlementUnavailable {
        /// Exact submitted inbox sequence.
        inbox: u64,
        /// First retained conversation sequence, for explicit backfill.
        first_sequence: u64,
        /// Observed lifecycle record, if one has arrived. A later FIFO message
        /// also establishes a missing settlement without inventing a status.
        status: Option<StatusRecord>,
    },
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
    /// Open a bounded recent window, with one metadata lookup per selected
    /// stream. Records still arrive through the authenticated shared session.
    /// An absent conversation is the empty initial prefix; its follow remains
    /// the Streams provider's responsibility, without a placeholder or poll.
    pub async fn connect_recent(
        local: Arc<LocalHarness>,
        maximum_output_bytes: u64,
    ) -> Result<Self> {
        validate_output_limit(maximum_output_bytes)?;
        let (_, root) = local.agents().await?;
        let conversation = tail(&local, root.conversation()?).await?;
        let status = tail(&local, root.status()?).await?;
        Self::connect(local, conversation.saturating_sub(128), status.saturating_sub(1), maximum_output_bytes).await
    }

    /// Attach the local root to its existing authenticated Streams pool.
    /// Explicit cursors come from the caller's retained page/window selection.
    pub async fn connect(
        local: Arc<LocalHarness>,
        conversation_from: u64,
        status_from: u64,
        maximum_output_bytes: u64,
    ) -> Result<Self> {
        validate_output_limit(maximum_output_bytes)?;
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

    /// Submit once and wait for the exact receipt's settled turn. Unknown
    /// commits leave with their original request, rather than being retried or
    /// interpreted as a successful submission.
    pub async fn prompt_turn(&mut self, id: &IntentId, text: &str) -> Result<AgentTurn> {
        let inbox = match self.prompt(id, text).await? {
            Delivered::Committed { inbox } => inbox,
            Delivered::Closed => return Err(LocalAgentError::Closed),
        };
        self.wait_prompt(id, inbox).await
    }

    /// Drain the same shared fold to the exact prompt, including a replay in
    /// the retained window. Status can overtake a conversation delivery on the
    /// multiplexed transport: a non-running status is evaluated only after a
    /// fixed conversation tail observed after that status has been drained.
    pub async fn wait_prompt(&mut self, id: &IntentId, inbox: u64) -> Result<AgentTurn> {
        let matches = |entry: &crate::agent::ConversationEntry| matches!(
            &entry.input,
            TurnInput::Message { from: Sender::User { principal }, id: actual, inbox: actual_inbox, .. }
                if principal == "local" && actual == id && *actual_inbox == inbox
        );
        // Bounded by StreamView policy, even when the agent has a long history.
        for index in 0..self.client.conversation().records().len() {
            if let Some(AgentEvent::Conversation { sequence, entry }) = self.client.conversation_entry(index)? {
                if matches(&entry) {
                    return self.read_turn(sequence, entry).await;
                }
                self.check_not_overtaken(&entry, inbox)?;
            }
        }
        let mut barrier = match self.client.current_status()? {
            Some(status) if status.cursor > inbox && !matches!(&status.phase, Phase::Running) => {
                Some((tail(&self.local, self.client.agent().conversation()?).await?, status))
            }
            _ => None,
        };
        loop {
            if let Some((cut, status)) = &barrier
                && self.client.conversation().next_sequence() >= *cut
            {
                return Err(match &status.phase {
                    Phase::WaitingApproval { .. } | Phase::WaitingAlarm { .. } => LocalAgentError::Blocked {
                        inbox, status: status.clone(),
                    },
                    _ => LocalAgentError::SettlementUnavailable {
                        inbox,
                        first_sequence: self.client.conversation().first_sequence(),
                        status: Some(status.clone()),
                    },
                });
            }
            match self.next().await? {
                AgentEvent::Conversation { sequence, entry } => {
                    if matches(&entry) {
                        return self.read_turn(sequence, entry).await;
                    }
                    self.check_not_overtaken(&entry, inbox)?;
                }
                AgentEvent::Status { status } if status.cursor > inbox => {
                    if matches!(&status.phase, Phase::Running) {
                        barrier = None;
                    } else {
                        let cut = tail(&self.local, self.client.agent().conversation()?).await?;
                        barrier = Some((cut, status));
                    }
                }
                _ => {}
            }
        }
    }

    fn check_not_overtaken(&self, entry: &crate::agent::ConversationEntry, inbox: u64) -> Result<()> {
        // This adapter selects only the local root, which has no inherited
        // parent conversation. Message settlement follows inbox order. A later
        // settled message therefore proves this selected page has passed the
        // target, even if an unchanged idle phase published no new status.
        if let TurnInput::Message { inbox: observed, .. } = &entry.input
            && *observed > inbox
        {
            return Err(LocalAgentError::SettlementUnavailable {
                inbox,
                first_sequence: self.client.conversation().first_sequence(),
                status: self.client.current_status()?,
            });
        }
        Ok(())
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

// Local owner metadata only; data/authorization/credit are owned by the one
// canonical session. Never substitute zero for any refusal other than absence.
async fn tail(local: &Arc<LocalHarness>, path: acyclic_stream::StreamPath) -> Result<u64> {
    let (agents, _) = local.agents().await?;
    match agents.provider().tail(path).await {
        Ok(tail) => Ok(tail),
        Err(acyclic_stream::StreamError::NotFound) => Ok(0),
        Err(error) => Err(crate::agent::Error::Stream(error).into()),
    }
}

fn validate_output_limit(maximum_output_bytes: u64) -> Result<()> {
    if maximum_output_bytes == 0 || maximum_output_bytes > 16 * 1024 * 1024 {
        return Err(Error::Invalid("agent output byte limit must be 1..=16777216".into()).into());
    }
    Ok(())
}
