//! Agent turns on the filesystem: each agent's journal is a conversation-scoped
//! authority over the agent's private volume; its executor comes from the host's
//! [`TurnExecutors`] for that agent and authority, per turn.
//!
//! The model context of a turn is the agent's own conversation stream: the last
//! settled turns (inherited ones included) as user/assistant messages, then the
//! turn's input. The host binds exactly that selection into the turn's journal,
//! so the executor can dispatch no other history.

use super::{
    FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost, FilesystemJournalBinding,
};
use crate::{
    Capabilities, Error, OperationId, Result,
    agent::{AgentRecord, AgentState, ConversationEntry, OpenTurn, TurnHost},
    conversation::{
        ContentResidencyVerifier, ModelContextSelection, VolumeClass, VolumeOperation, VolumeOwner,
    },
    core::{AggregateKind, Authority, AuthorityIssuer, SchemaRegistry, Scope},
    executor::{ExecutionJournal, Executor, TurnOutput},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture as BoxFuture, ReadRequest, StreamError, StreamProvider};
use futures::TryStreamExt;
use std::sync::Arc;

/// Name of the volume binding that holds an agent's journal and staged turn output.
pub const PRIVATE_VOLUME: &str = "private";

/// The authority of one agent turn: the agent's conversation issuer, its scope
/// over every bound volume, and a content reader bound to that scope.
pub struct TurnAuthority {
    /// Signing custody; it must outlive the turn's journal.
    pub issuer: AuthorityIssuer,
    /// Read and write over the agent's bound volumes.
    pub scope: Scope,
    /// The capabilities `scope` grants, for the executor's tool authority.
    pub grants: Capabilities,
    /// Reader of files under `scope`, for model content and tool inputs.
    pub content: Arc<dyn ContentResidencyVerifier>,
}

/// Builds the executor of one agent turn: its tools, policy, limits and
/// compaction, under the turn's [`TurnAuthority`].
pub trait TurnExecutors: Send + Sync {
    /// Executor for `agent`'s next turn.
    fn executor(&self, agent: &AgentState, authority: &TurnAuthority) -> Result<Arc<dyn Executor>>;
}

impl<F> TurnExecutors for F
where
    F: Fn(&AgentState, &TurnAuthority) -> Result<Arc<dyn Executor>> + Send + Sync,
{
    fn executor(&self, agent: &AgentState, authority: &TurnAuthority) -> Result<Arc<dyn Executor>> {
        self(agent, authority)
    }
}

/// Bounds of one agent turn.
#[derive(Clone, Copy, Debug)]
pub struct TurnLimits {
    /// Journal bytes per turn.
    pub maximum_bytes: u64,
    /// Model and tool steps per turn.
    pub max_steps: u32,
    /// Settled turns of history in the model context.
    pub history_turns: u32,
}

/// [`TurnHost`] binding every agent to its executor and a journal on its
/// private volume, under a conversation authority named by the agent's id.
pub struct FilesystemTurnHost<P, A, O> {
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    issuer: (String, [u8; 32]),
    executors: Arc<dyn TurnExecutors>,
    schemas: SchemaRegistry,
    limits: TurnLimits,
}

impl<P, A, O> FilesystemTurnHost<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Host signing each agent's authority with `issuer` (an id and its key).
    #[must_use]
    pub fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        issuer: (String, [u8; 32]),
        executors: Arc<dyn TurnExecutors>,
        limits: TurnLimits,
    ) -> Self {
        Self {
            stream,
            host,
            issuer,
            executors,
            schemas: SchemaRegistry::new(),
            limits,
        }
    }

    fn journal(
        &self,
        agent: &AgentState,
    ) -> Result<(FilesystemExecutionJournal<P, A, O>, TurnAuthority)> {
        let owner = crate::AgentId::from_bytes(agent.path.id().to_bytes());
        let private = agent
            .volumes
            .iter()
            .find(|binding| binding.name == PRIVATE_VOLUME)
            .ok_or_else(|| Error::Invalid("agent has no private volume".into()))?;
        if private.volume.class() != VolumeClass::AgentPrivate
            || *private.volume.owner() != VolumeOwner::Agent(owner)
        {
            return Err(Error::Unauthorized("private volume belongs to another agent".into()));
        }
        let issuer = AuthorityIssuer::new(
            self.issuer.0.clone(),
            self.issuer.1,
            Authority {
                kind: AggregateKind::Conversation,
                id: agent.path.id().as_str().to_owned(),
            },
        );
        let mut grants = Vec::with_capacity(2 * agent.volumes.len());
        for binding in &agent.volumes {
            grants.push(binding.volume.capability(VolumeOperation::Read, &mut None)?);
            grants.push(binding.volume.capability(VolumeOperation::Write, &mut None)?);
        }
        let capabilities = Capabilities::new(grants);
        let scope = issuer.issue("agent", &capabilities, Some(owner), None, &mut None)?;
        let verifier = issuer.verifier(&mut None)?;
        let content = FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            &verifier,
            &scope,
            self.limits.maximum_bytes,
            None,
        )?;
        let journal = FilesystemExecutionJournal::new(
            Arc::clone(&self.stream),
            Arc::clone(&self.host),
            &self.schemas,
            FilesystemJournalBinding {
                volume: &private.volume,
                verifier: &verifier,
                scope: &scope,
                maximum_bytes: self.limits.maximum_bytes,
                io: None,
            },
        )?
        .with_input_verifier(content.clone());
        Ok((
            journal,
            TurnAuthority {
                issuer,
                scope,
                grants: capabilities,
                content,
            },
        ))
    }

    /// The agent's last settled turns, oldest first.
    async fn history(&self, agent: &AgentState) -> Result<Vec<ConversationEntry>> {
        let tail = agent.conversation_tail;
        let from = tail.saturating_sub(u64::from(self.limits.history_turns));
        if from == tail {
            return Ok(Vec::new());
        }
        let path = agent.path.conversation().map_err(agent_error)?;
        let records = match self
            .stream
            .read(ReadRequest {
                path,
                from,
                limit: u32::try_from(tail - from).unwrap_or(u32::MAX),
            })
            .await
        {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        records
            .iter()
            .map(|record| ConversationEntry::decode(&record.value).map_err(agent_error))
            .collect()
    }
}

fn agent_error(error: crate::agent::Error) -> Error {
    Error::Invalid(error.to_string())
}

/// Stable message identity of one side of a settled or current turn.
fn message_id(turn: OperationId, role: ModelRole) -> uuid::Uuid {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.message.v1");
    hasher.update(&turn.into_bytes());
    hasher.update(&[u8::from(role == ModelRole::Assistant)]);
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    uuid::Uuid::from_bytes(bytes)
}

/// The selection of `history` plus the current `input`: one user and one
/// assistant message per settled turn, assistant text read from its staged output.
async fn select(
    agent: &AgentState,
    journal: &dyn ExecutionJournal,
    history: Vec<ConversationEntry>,
    turn: OperationId,
    input: &ModelContent,
) -> Result<Option<SelectedModelContext>> {
    if history.is_empty() {
        return Ok(None);
    }
    let mut messages = Vec::with_capacity(2 * history.len() + 1);
    let mut message_ids = Vec::with_capacity(messages.capacity());
    for entry in history {
        let settled = crate::agent::turn_id(agent, &entry.input).map_err(agent_error)?;
        let bytes = journal.load(&entry.output, None)?.await?;
        let output: TurnOutput = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Invalid("staged turn output is malformed".into()))?;
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: crate::agent::model_input(&entry.input).map_err(agent_error)?,
        });
        message_ids.push(message_id(settled, ModelRole::User));
        messages.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text(output.text),
        });
        message_ids.push(message_id(settled, ModelRole::Assistant));
    }
    messages.push(ModelMessage {
        role: ModelRole::User,
        content: input.clone(),
    });
    message_ids.push(message_id(turn, ModelRole::User));
    Ok(Some(SelectedModelContext {
        selection: ModelContextSelection {
            conversation_revision: agent.conversation_tail + 1,
            message_ids,
            checkpoint: None,
        },
        messages,
    }))
}

impl<P, A, O> TurnHost for FilesystemTurnHost<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
    ) -> BoxFuture<'a, Result<OpenTurn>> {
        Box::pin(async move {
            let (journal, authority) = self.journal(agent)?;
            let history = self.history(agent).await?;
            let selected = select(agent, &journal, history, turn, input).await?;
            let journal = match &selected {
                Some(selected) => journal.with_bound_selection(selected.clone()),
                None => journal,
            };
            Ok(OpenTurn {
                executor: self.executors.executor(agent, &authority)?,
                journal: Arc::new(journal),
                max_steps: self.limits.max_steps,
                custody: Some(authority.issuer),
                selected,
            })
        })
    }
}
