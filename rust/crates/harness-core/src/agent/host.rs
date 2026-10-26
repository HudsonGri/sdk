//! The one agent entry point every host shares: local, wasm, providers and native.
//!
//! An [`AgentHost`] owns one A4 [`Runtime`] for every agent in one account and
//! namespace. Every control is one agent commit: create a root agent, send a
//! record (prompt, cancel, approval, hook) or archive. Agents never run outside
//! the runtime: a commit to an inbox wakes its actor through the actor's follow.
//! Observation needs no API: an agent's status and conversation are streams
//! ([`AgentPath::status`], [`AgentPath::conversation`]) any client follows.

use super::{
    AgentHandler, AgentPath, Error, ForkAdopted, InboxRecord, IntentId, Sender, Submitted,
    TurnDriver, archive, inbox_tail, send, submit,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    guest::Handler,
    local::{Clock, Runtime, Store},
};
use acyclic_stream::{CommitRequest, StreamPath, StreamProvider};
use std::{convert::Infallible, sync::Arc};

/// Outcome of one control commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivered {
    /// Committed exactly once (an exact replay also reports this).
    Committed,
    /// The recipient is archived; nothing was written.
    Closed,
}

/// One runtime and handler for every agent in one account and namespace.
pub struct AgentHost<P: StreamProvider + ?Sized + 'static, D: TurnDriver + 'static> {
    provider: Arc<P>,
    handler: Arc<AgentHandler<P, D>>,
    runtime: Runtime<P>,
    within: ActorReference,
}

impl<P: StreamProvider + ?Sized + 'static, D: TurnDriver + 'static> AgentHost<P, D> {
    /// Host for the agents of `within`'s account and namespace, with actor state
    /// under [`super::ACTORS`] in `provider`'s consistency domain.
    pub fn new(
        provider: Arc<P>,
        driver: D,
        within: ActorReference,
        limits: ActorLimits,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        let store = || -> Result<Store<P>, Error> {
            let root = StreamPath::new(super::ACTORS).map_err(|_| Error::Path("actor root"))?;
            Ok(Store::new(provider.clone(), root))
        };
        let handler = Arc::new(
            AgentHandler::new(store()?, driver, limits)
                .map_err(|_| Error::Record("agent handler manifest"))?,
        );
        let runtime = Runtime::new(store()?, handler.clone() as Arc<dyn Handler>, clock);
        Ok(Self {
            provider,
            handler,
            runtime,
            within,
        })
    }

    /// The shared runtime, for deterministic hosts that `drain` it.
    pub const fn runtime(&self) -> &Runtime<P> {
        &self.runtime
    }

    /// The shared provider session.
    pub const fn provider(&self) -> &Arc<P> {
        &self.provider
    }

    /// Host every agent until the runtime fails: one activation follow and one
    /// timer for all agents. Per-agent failures go to `report` and stay queued.
    pub async fn run(
        &self,
        report: impl Fn(&ActorReference, &acyclic_actors::local::Error),
    ) -> Result<Infallible, acyclic_actors::local::Error> {
        self.runtime.run(report).await
    }

    /// The actor that runs `agent`.
    pub fn actor(&self, agent: &AgentPath) -> Result<ActorReference, Error> {
        AgentHandler::<P, D>::actor(&self.within, &agent.id())
    }

    /// Create root agent `adopted.agent` in one commit, and start hosting it.
    /// Retrying the same `intent` replays exactly and names the same agent.
    pub async fn create(&self, intent: &IntentId, adopted: ForkAdopted) -> Result<AgentPath, Error> {
        let created = self.handler.create_root(&self.within, intent, adopted)?;
        match submit(&*self.provider, created.request).await? {
            Submitted::Committed(_) => {}
            Submitted::Conflict(_) | Submitted::InboxMoved(_) | Submitted::RecipientClosed(_) => {
                return Err(Error::Record("agent exists with another creation"));
            }
        }
        self.host(&created.child)?;
        Ok(created.child)
    }

    /// Append `record` to `to`'s inbox in one conditional commit: a prompt
    /// (`Message`), `Cancel`, an `Approval` response or a `Hook`. The key binds
    /// recipient, sender and record intent: on [`Error::Unknown`] resubmit the
    /// carried request with [`submit`]; calling `send` again re-reads the tail.
    pub async fn send(
        &self,
        to: &AgentPath,
        from: &Sender,
        record: &InboxRecord,
    ) -> Result<Delivered, Error> {
        self.deliver(to, |tail| send(to, tail, from, record)).await
    }

    /// Close `target` and, through its cascade, its open descendants.
    pub async fn archive(
        &self,
        target: &AgentPath,
        by: &Sender,
        id: &IntentId,
    ) -> Result<Delivered, Error> {
        self.deliver(target, |tail| archive(target, tail, by, id)).await
    }

    /// Durably pause or resume one agent. A paused agent consumes nothing.
    pub async fn pause(&self, agent: &AgentPath, paused: bool) -> Result<(), Error> {
        let actor = self.actor(agent)?;
        self.runtime.track(actor.clone()).map_err(actor_error)?;
        self.runtime.pause(&actor, paused).await.map_err(actor_error)
    }

    /// Host an agent created elsewhere (for example after reopening), so its
    /// pending input runs without waiting for activation discovery.
    pub fn host(&self, agent: &AgentPath) -> Result<(), Error> {
        let actor = self.actor(agent)?;
        self.runtime.track(actor.clone()).map_err(actor_error)?;
        self.runtime.wake(&actor);
        Ok(())
    }

    /// One inbox commit, re-decided only when the recipient's tail moved.
    async fn deliver(
        &self,
        to: &AgentPath,
        request: impl Fn(u64) -> Result<CommitRequest, Error>,
    ) -> Result<Delivered, Error> {
        loop {
            let tail = inbox_tail(&*self.provider, to).await?;
            match submit(&*self.provider, request(tail)?).await? {
                Submitted::Committed(_) => return Ok(Delivered::Committed),
                Submitted::RecipientClosed(_) => return Ok(Delivered::Closed),
                Submitted::InboxMoved(_) => {}
                Submitted::Conflict(_) => return Err(Error::Record("inbox commit conflicted")),
            }
        }
    }
}

fn actor_error(error: acyclic_actors::local::Error) -> Error {
    Error::Actor(Box::new(error))
}
