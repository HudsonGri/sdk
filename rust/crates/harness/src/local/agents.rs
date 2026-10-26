//! The local host of the agent model: every agent of this harness on one A4
//! runtime over the harness stream, its turns on the stock executor with this
//! harness's model, tools, context and compaction.

use super::{LocalHarness, LocalStream, storage};
use crate::{
    Error, OperationId, Result,
    agent::{AgentHost, AgentPath, AgentState, ExecutorDriver, ForkAdopted, IntentId, VolumeBinding},
    conversation::VolumeRef,
    executor::{Executor, StockExecutor},
    filesystem::{FilesystemTurnHost, PRIVATE_VOLUME, TurnAuthority, TurnExecutors, TurnLimits},
    model::ModelContent,
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_stream::BoxProviderFuture;
use std::sync::{Arc, Weak};

/// Turns of local agents.
pub type LocalTurns = FilesystemTurnHost<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
/// Every agent of one local harness.
pub type LocalAgents = AgentHost<LocalStream, ExecutorDriver<LocalTurns>>;

/// Settled turns of history in each agent turn's model context.
const HISTORY_TURNS: u32 = 64;

/// The hosted agents, their root, and the runtime task that ends with them.
pub(super) struct Hosted {
    agents: Arc<LocalAgents>,
    root: AgentPath,
    run: tokio::task::JoinHandle<()>,
}

impl Drop for Hosted {
    fn drop(&mut self) {
        self.run.abort();
    }
}

/// Builds each agent turn's executor from the harness that hosts it.
struct Executors(Weak<LocalHarness>);

impl TurnExecutors for Executors {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let local = self
                .0
                .upgrade()
                .ok_or_else(|| Error::Unsupported("local harness is closed".into()))?;
            let writer = authority
                .content
                .writer
                .as_deref()
                .ok_or_else(|| Error::Invalid("agent turn has no private writer".into()))?;
            let prompt = match input {
                ModelContent::Text(text) => Some(text.as_str()),
                ModelContent::Part(_) | ModelContent::Parts(_) => None,
            };
            let manifest = match &local.context_manifest {
                Some(manifest) => manifest.clone(),
                None => {
                    local
                        .stage_context_manifest_with(turn, prompt, writer)
                        .await?
                }
            };
            let context = local.runtime.harness().agent_context(
                turn,
                &authority.grants,
                authority.content.clone(),
            )?;
            let reader = authority.content.reader.clone();
            let provider = local
                .provider
                .bind_content(reader.clone(), local.limits.file_bytes)
                .unwrap_or_else(|| local.provider.clone());
            let executor = StockExecutor::new(
                local.model.clone(),
                provider,
                crate::context::ContextPipeline::default(),
                local.tools().clone(),
            )
            .with_limits(local.limits)
            .with_compaction_policy(local.compaction.clone())
            .with_context_manifest(manifest, reader)
            .await?
            .with_tool_authority(context.scope().clone(), None)?
            .with_task_context(&context, turn)?;
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        })
    }
}

impl LocalHarness {
    /// Every agent of this harness on one A4 runtime, and its root agent.
    ///
    /// The first call creates the root agent (replayed exactly on reopen) and
    /// starts the runtime; it stops when this harness drops. Controls are
    /// [`AgentHost`] commits; observation is the agents' status and
    /// conversation streams.
    pub async fn agents(self: &Arc<Self>) -> Result<(&Arc<LocalAgents>, &AgentPath)> {
        let hosted = self
            .agents
            .get_or_try_init(|| async { self.host_agents().await })
            .await?;
        Ok((&hosted.agents, &hosted.root))
    }

    async fn host_agents(self: &Arc<Self>) -> Result<Hosted> {
        let key = self.agent_key.ok_or_else(|| {
            Error::Unsupported("an embedded harness has no agent signing custody".into())
        })?;
        let id = crate::agent::AgentId::from_bytes(self.agent.into_bytes());
        let stream = self.runtime.task_host().workflow_stream();
        let limits = TurnLimits {
            maximum_bytes: self.limits.file_bytes,
            max_steps: u32::try_from(self.limits.model_steps)
                .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?,
            history_turns: HISTORY_TURNS,
        };
        let turns = FilesystemTurnHost::new(
            stream.clone(),
            self.files.clone(),
            ("acyclic.local".to_owned(), key),
            Arc::new(Executors(Arc::downgrade(self))),
            limits,
        );
        let within = ActorReference::new(b"local", b"agents", id.as_str().as_bytes())
            .map_err(storage)?;
        let agents = Arc::new(
            AgentHost::new(
                stream,
                ExecutorDriver::new(turns),
                within,
                ActorLimits::new(10_000, 1 << 20, 1 << 16).map_err(storage)?,
                Arc::new(SystemClock),
            )
            .map_err(storage)?,
        );
        let bind = |name: &str, volume: &VolumeRef| VolumeBinding {
            name: name.to_owned(),
            volume: volume.clone(),
        };
        let root = agents
            .create(
                &IntentId::try_from("root").map_err(storage)?,
                ForkAdopted {
                    agent: id,
                    parent: None,
                    forked_at: 0,
                    conversation_tail: 0,
                    volumes: vec![
                        bind(PRIVATE_VOLUME, &self.journal),
                        bind("project", &self.project),
                        bind("scratch", &self.scratch),
                    ],
                    brief: None,
                    policy: None,
                },
            )
            .await
            .map_err(storage)?;
        let runner = Arc::clone(&agents);
        let run = tokio::spawn(async move {
            let Err(error) = runner
                .run(|actor, error| tracing::warn!(?actor, %error, "agent turn failed"))
                .await;
            tracing::error!(%error, "agent runtime stopped");
        });
        Ok(Hosted { agents, root, run })
    }
}
