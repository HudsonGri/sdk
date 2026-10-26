//! Agent turns on the filesystem: each agent's journal is a conversation-scoped
//! authority over the agent's private volume, and its executor is the stock loop.

use super::{FilesystemExecutionJournal, FilesystemHost, FilesystemJournalBinding};
use crate::{
    Capabilities, Error, OperationId, Result,
    agent::{AgentState, OpenTurn, TurnHost},
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, SchemaRegistry},
    executor::StockExecutor,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture as BoxFuture, StreamProvider};
use std::sync::Arc;

/// Name of the volume binding that holds an agent's journal and staged turn output.
pub const PRIVATE_VOLUME: &str = "private";

/// [`TurnHost`] binding every agent to the stock executor and a journal on its
/// private volume, under a conversation authority named by the agent's id.
pub struct FilesystemTurnHost<P, A, O> {
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    issuer: (String, [u8; 32]),
    executor: StockExecutor,
    schemas: SchemaRegistry,
    maximum_bytes: u64,
    max_steps: u32,
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
        executor: StockExecutor,
        limits: (u64, u32),
    ) -> Self {
        Self {
            stream,
            host,
            issuer,
            executor,
            schemas: SchemaRegistry::new(),
            maximum_bytes: limits.0,
            max_steps: limits.1,
        }
    }

    fn journal(
        &self,
        agent: &AgentState,
    ) -> Result<(FilesystemExecutionJournal<P, A, O>, AuthorityIssuer)> {
        let owner = crate::AgentId::from_bytes(agent.path.id().to_bytes());
        let binding = agent
            .volumes
            .iter()
            .find(|binding| binding.name == PRIVATE_VOLUME)
            .ok_or_else(|| Error::Invalid("agent has no private volume".into()))?;
        let volume = VolumeRef::new(
            self.host.provider.clone(),
            binding.volume.clone(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
            &mut None,
        )?;
        let issuer = AuthorityIssuer::new(
            self.issuer.0.clone(),
            self.issuer.1,
            Authority {
                kind: AggregateKind::Conversation,
                id: agent.path.id().as_str().to_owned(),
            },
        );
        let capabilities = Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]);
        let scope = issuer.issue("agent", &capabilities, Some(owner), None, &mut None)?;
        let journal = FilesystemExecutionJournal::new(
            Arc::clone(&self.stream),
            Arc::clone(&self.host),
            &self.schemas,
            FilesystemJournalBinding {
                volume: &volume,
                verifier: &issuer.verifier(&mut None)?,
                scope: &scope,
                maximum_bytes: self.maximum_bytes,
                io: None,
            },
        )?;
        Ok((journal, issuer))
    }
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
        _turn: OperationId,
    ) -> BoxFuture<'a, Result<OpenTurn>> {
        let open = self.journal(agent).map(|(journal, issuer)| OpenTurn {
            executor: Arc::new(self.executor.clone()),
            journal: Arc::new(journal),
            max_steps: self.max_steps,
            custody: Some(issuer),
        });
        Box::pin(async move { open })
    }
}
