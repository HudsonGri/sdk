//! One GraphCoder conversation: a root agent of the local harness. Prompts,
//! cancels and approval decisions are inbox commits; observation follows the
//! agent's status stream and reads its settled conversation entries.
use acyclic_harness::{
    Error, LocalHarness, Result,
    agent::{
        AgentPath, AgentRecord as _, ConversationEntry, Control, Delivered, InboxRecord, IntentId,
        Phase, Sender, StatusRecord, TurnInput, intent_key,
    },
    approvals::ApprovalResponder,
    conversation::{ContentResidencyVerifier, FileRef},
    executor::TurnOutput,
};
use acyclic_stream::{
    CommitMutation, CommitRequest, FollowFilter, ReadRequest, StreamError, StreamPath,
    StreamProvider as _,
};
use futures::{StreamExt as _, TryStreamExt as _, stream::LocalBoxStream};
use std::{collections::VecDeque, sync::Arc};

/// Bytes read back from one staged prompt or turn output.
const MAXIMUM_TEXT: u64 = 16 * 1024 * 1024;
/// Conversation entries read per page.
const PAGE: u32 = 64;
/// The interactive conversation that `--resume` reopens.
const INTERACTIVE: &str = "graphcoder/interactive";
const USER: &str = "local";

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn intent(id: &str) -> Result<IntentId> {
    IntentId::try_from(id).map_err(failed)
}

/// What a follower observes, in commit order.
pub enum Event {
    /// The agent's phase after a transition, with its consumed inbox cursor.
    Status(StatusRecord),
    /// One settled turn: its message's inbox sequence, if a message started it.
    Turn {
        inbox: Option<u64>,
        entry: ConversationEntry,
    },
}

pub struct Conversation {
    local: Arc<LocalHarness>,
    agent: AgentPath,
    reader: Arc<dyn ContentResidencyVerifier>,
}

impl Conversation {
    /// Create or reopen conversation `id`.
    pub async fn open(local: Arc<LocalHarness>, id: &str) -> Result<Arc<Self>> {
        let agent = local.conversation(&intent(id)?).await?;
        let reader = local.client_reader()?;
        Ok(Arc::new(Self {
            local,
            agent,
            reader,
        }))
    }

    /// Remember `id` as the conversation `--resume` reopens.
    pub async fn select(local: &Arc<LocalHarness>, id: &str) -> Result<()> {
        let (agents, _) = local.agents().await?;
        agents
            .provider()
            .commit(CommitRequest {
                conditions: Vec::new(),
                mutations: vec![CommitMutation::Append {
                    path: StreamPath::new(INTERACTIVE).map_err(failed)?,
                    records: vec![id.as_bytes().to_vec().into()],
                }],
                idempotency_key: intent_key(
                    "graphcoder-interactive",
                    &[id, &acyclic_harness::OperationId::new().to_string()],
                )
                .map_err(failed)?,
            })
            .await
            .map_err(failed)?;
        Ok(())
    }

    /// The last selected interactive conversation.
    pub async fn last(local: &Arc<LocalHarness>) -> Result<String> {
        let (agents, _) = local.agents().await?;
        let path = StreamPath::new(INTERACTIVE).map_err(failed)?;
        let missing = || Error::NotFound("interactive conversation".into());
        let tail = match agents.provider().tail(path.clone()).await {
            Ok(tail) => tail.checked_sub(1).ok_or_else(missing)?,
            Err(StreamError::NotFound) => return Err(missing()),
            Err(error) => return Err(failed(error)),
        };
        let mut records = agents
            .provider()
            .read(ReadRequest {
                path,
                from: tail,
                limit: 1,
            })
            .await
            .map_err(failed)?;
        let record = records.try_next().await.map_err(failed)?.ok_or_else(missing)?;
        String::from_utf8(record.value.to_vec()).map_err(failed)
    }

    /// Send `prompt` as message `id`; returns its inbox sequence. The same id
    /// and prompt replay exactly.
    pub async fn send(&self, id: &str, prompt: &str) -> Result<u64> {
        match self
            .local
            .prompt(&self.agent, &intent(id)?, prompt)
            .await
            .map_err(failed)?
        {
            Delivered::Committed { inbox } => Ok(inbox),
            Delivered::Closed => Err(Error::Conflict("conversation is archived".into())),
        }
    }

    /// Stop the running turn; the conversation stays open.
    pub async fn cancel(&self) -> Result<()> {
        let (agents, _) = self.local.agents().await?;
        let by = Sender::User {
            principal: USER.into(),
        };
        let id = intent(&acyclic_harness::OperationId::new().to_string())?;
        agents
            .send(&self.agent, &by, &InboxRecord::Cancel(Control { by: by.clone(), id }))
            .await
            .map_err(failed)?;
        Ok(())
    }

    /// Answer the agent's pending approval request as the local user.
    pub async fn decide(&self, request: &IntentId, approved: bool) -> Result<()> {
        self.local
            .decide(
                &self.agent,
                request,
                ApprovalResponder::Human(USER),
                approved,
                None,
            )
            .await
            .map_err(failed)?;
        Ok(())
    }

    /// The text of a staged prompt or turn output.
    pub async fn text(&self, file: &FileRef) -> Result<String> {
        let bytes = self.reader.read(file, MAXIMUM_TEXT, None, None)?.await?;
        String::from_utf8(bytes.to_vec()).map_err(failed)
    }

    /// The prompt and answer text of a settled turn.
    pub async fn turn(&self, entry: &ConversationEntry) -> Result<(Option<String>, String)> {
        let prompt = match &entry.input {
            TurnInput::Message { body, .. } => Some(self.text(body).await?),
            _ => None,
        };
        let output = self.text(&entry.output).await?;
        let output: TurnOutput = serde_json::from_str(&output).map_err(failed)?;
        Ok((prompt, output.text))
    }

    /// Number of settled turns.
    pub async fn settled(&self) -> Result<u64> {
        let (agents, _) = self.local.agents().await?;
        let conversation = self.agent.conversation().map_err(failed)?;
        match agents.provider().tail(conversation).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(failed(error)),
        }
    }

    /// Every settled turn from conversation sequence `from`, interleaved with
    /// the agent's status from its latest record on. Each status record
    /// commits with the turn it settles, so the turns read after it are
    /// complete up to that status.
    pub async fn events(self: &Arc<Self>, from: u64) -> Result<LocalBoxStream<'static, Result<Event>>> {
        let provider = Arc::clone(self.local.agents().await?.0.provider());
        let status = self.agent.status().map_err(failed)?;
        let latest = provider.tail(status.clone()).await.map_err(failed)?;
        let statuses = provider
            .follow(status, latest.saturating_sub(1), FollowFilter::default())
            .await
            .map_err(failed)?;
        let conversation = self.agent.conversation().map_err(failed)?;
        let state = (statuses, from, VecDeque::new());
        Ok(futures::stream::try_unfold(state, move |(mut statuses, mut next, mut queued)| {
            let (provider, conversation) = (Arc::clone(&provider), conversation.clone());
            async move {
                loop {
                    if let Some(event) = queued.pop_front() {
                        return Ok::<_, Error>(Some((event, (statuses, next, queued))));
                    }
                    let record = statuses
                        .try_next()
                        .await
                        .map_err(failed)?
                        .ok_or_else(|| Error::Conflict("agent status follow ended".into()))?;
                    let status = StatusRecord::decode(&record.value).map_err(failed)?;
                    loop {
                        let page = match provider
                            .read(ReadRequest {
                                path: conversation.clone(),
                                from: next,
                                limit: PAGE,
                            })
                            .await
                        {
                            Ok(page) => page.try_collect::<Vec<_>>().await.map_err(failed)?,
                            Err(StreamError::NotFound) => Vec::new(),
                            Err(error) => return Err(failed(error)),
                        };
                        let read = page.len();
                        for record in page {
                            next = record.sequence + 1;
                            let entry = ConversationEntry::decode(&record.value).map_err(failed)?;
                            let inbox = match &entry.input {
                                TurnInput::Message { inbox, .. } => Some(*inbox),
                                _ => None,
                            };
                            queued.push_back(Event::Turn { inbox, entry });
                        }
                        if read < PAGE as usize {
                            break;
                        }
                    }
                    queued.push_back(Event::Status(status));
                }
            }
        })
        .boxed_local())
    }
}

/// Whether `status` ends the wait for the turn of inbox message `inbox`
/// without its output: the agent finished, or consumed it and went idle.
pub fn ended(status: &StatusRecord, inbox: u64) -> Option<String> {
    match &status.phase {
        Phase::Done { outcome } => Some(format!("conversation ended: {outcome:?}")),
        Phase::Idle if status.cursor > inbox => Some("turn ended without output".into()),
        _ => None,
    }
}
