use crate::{DemandError, DemandPolicy, ViewDemand};
use acyclic_stream::{
    BoxProviderFuture, BoxProviderStream, StreamError, StreamPath, session::Client, wire, wire_codec,
};
use bytes::Bytes;
use futures::StreamExt;
use std::{
    collections::BTreeMap,
    future::Future as _,
    task::{Context, Poll},
};

type Follow = BoxProviderStream<'static, Result<wire::SessionFrame, StreamError>>;

enum Subscription {
    Opening(BoxProviderFuture<'static, Result<Follow, StreamError>>),
    Open(Follow),
}

/// Admission failures are distinct from authenticated provider refusals.
#[derive(Debug)]
pub enum WireSessionError {
    /// The replacement view exceeds this session's policy.
    Demand(DemandError),
    /// No full follow was requested for this agent's fork stream.
    NotRequested,
    /// The agent already owns its one full follow.
    AlreadyOpen,
    /// The selected agent has no open follow.
    NotOpen,
    /// A canonical subscription must request full records for one Streams path.
    InvalidSubscription,
    /// Original typed transport, authorization or provider refusal.
    Stream(StreamError),
}

impl std::fmt::Display for WireSessionError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Demand(error) => write!(output, "{error}"),
            Self::NotRequested => output.write_str("agent follow was not requested"),
            Self::AlreadyOpen => output.write_str("agent follow is already open"),
            Self::NotOpen => output.write_str("agent follow is not open"),
            Self::InvalidSubscription => output.write_str("invalid full stream subscription"),
            Self::Stream(error) => write!(output, "{error}"),
        }
    }
}

impl std::error::Error for WireSessionError {}

/// One owner for view-selected logical follows on the canonical Streams pool.
/// The pool alone owns physical connections, cursor recovery and receive credit.
/// This layer adds no queue, reconnect loop or per-agent transport task.
pub struct WireSession {
    client: Client,
    policy: DemandPolicy,
    demand: ViewDemand,
    follows: BTreeMap<StreamPath, Subscription>,
}

impl WireSession {
    /// Take the platform-created pool; native and WASM use the same owner.
    #[must_use]
    pub fn new(client: Client, policy: DemandPolicy) -> Self {
        Self {
            client,
            policy,
            demand: ViewDemand::default(),
            follows: BTreeMap::new(),
        }
    }

    /// Borrow admitted view demand, including lazy child discovery demand.
    #[must_use]
    pub fn demand(&self) -> &ViewDemand {
        &self.demand
    }

    /// Close deselected follows before any newly selected follow can open.
    /// The replacement is rechecked against this session's own policy.
    ///
    /// # Errors
    /// Excess demand leaves both existing demand and follows unchanged.
    pub fn set_view(&mut self, demand: ViewDemand) -> Result<(), WireSessionError> {
        let demand = ViewDemand::new(
            self.policy,
            demand.full().cloned(),
            std::iter::empty(),
            demand.expanded().cloned(),
        )
        .map_err(WireSessionError::Demand)?;
        self.follows.retain(|agent, _| demand.contains_full(agent));
        self.demand = demand;
        Ok(())
    }

    /// Delegate bounded credential registration to the canonical pool.
    ///
    /// # Errors
    /// Preserves the pool's original capacity and transport refusal.
    pub fn register_credential(
        &self,
        slot: u32,
        credential: Bytes,
    ) -> Result<(), WireSessionError> {
        self.client
            .register_credential(slot, credential)
            .map_err(WireSessionError::Stream)
    }

    /// Open the full record stream of one selected agent fork.
    /// The caller supplies the authenticated subscription and original cursor.
    /// An interrupted wait retains its bounded opening in this session. Removing
    /// the interest drops it through the pool's existing cancellation guard.
    ///
    /// # Errors
    /// Refuses unsolicited, duplicate or malformed follows before pool admission;
    /// preserves authenticated provider refusals without inventing revision zero.
    pub async fn open_stream(
        &mut self,
        agent: StreamPath,
        subscription: wire::SessionSubscription,
    ) -> Result<(), WireSessionError> {
        self.begin_stream(agent.clone(), subscription)?;
        futures::future::poll_fn(|context| self.poll_open(&agent, context)).await
    }

    /// Admit a lazy open without holding a mutable borrow across network I/O.
    /// Its pending future counts against the same selected-follow bound and is
    /// dropped with the interest. Polling drives the canonical pool admission.
    ///
    /// # Errors
    /// Rejects unsolicited, duplicate or malformed follows before admission.
    pub fn begin_stream(
        &mut self,
        agent: StreamPath,
        subscription: wire::SessionSubscription,
    ) -> Result<(), WireSessionError> {
        if !self.demand.contains_full(&agent) {
            return Err(WireSessionError::NotRequested);
        }
        if self.follows.contains_key(&agent) {
            return Err(WireSessionError::AlreadyOpen);
        }
        let Some(wire::session_subscription::Kind::Follow(follow)) = &subscription.kind else {
            return Err(WireSessionError::InvalidSubscription);
        };
        if follow.delivery != wire::SessionDelivery::All as i32 || subscription.filter.is_some() {
            return Err(WireSessionError::InvalidSubscription);
        }
        let resume = subscription
            .resume
            .as_ref()
            .ok_or(WireSessionError::InvalidSubscription)?;
        if subscription.channel.is_some() {
            return Err(WireSessionError::InvalidSubscription);
        }
        // Validate before the lazy future takes custody of caller-owned bytes.
        wire_codec::session::validate_follow(follow, resume)
            .map_err(WireSessionError::Stream)?;
        let path = wire_codec::session::stream_path(&follow.resource)
            .map_err(WireSessionError::Stream)?;
        if path != agent {
            return Err(WireSessionError::NotRequested);
        }
        let client = self.client.clone();
        let opening = Box::pin(async move { client.subscribe(subscription).await });
        self.follows.insert(agent, Subscription::Opening(opening));
        Ok(())
    }

    fn poll_open(
        &mut self,
        agent: &StreamPath,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), WireSessionError>> {
        let Some(subscription) = self.follows.get_mut(agent) else {
            return Poll::Ready(Err(WireSessionError::NotOpen));
        };
        let Subscription::Opening(opening) = subscription else {
            return Poll::Ready(Ok(()));
        };
        match opening.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(stream)) => {
                *subscription = Subscription::Open(stream);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                self.follows.remove(agent);
                Poll::Ready(Err(WireSessionError::Stream(error)))
            }
        }
    }

    /// Poll only the requested visible agent; never enumerate the stored swarm.
    /// No frame is copied or buffered here. The shared fold consumes it directly.
    /// Terminal delivery releases its subscription through the pool drop guard.
    pub fn poll_stream(
        &mut self,
        agent: &StreamPath,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<wire::SessionFrame>, WireSessionError>> {
        match self.poll_open(agent, context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let Some(Subscription::Open(stream)) = self.follows.get_mut(agent) else {
            return Poll::Ready(Err(WireSessionError::NotOpen));
        };
        match stream.poll_next_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(frame))) => {
                if matches!(frame.body.as_ref(), Some(wire::session_frame::Body::Closed(_))) {
                    self.follows.remove(agent);
                }
                Poll::Ready(Ok(Some(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.follows.remove(agent);
                Poll::Ready(Err(WireSessionError::Stream(error)))
            }
            Poll::Ready(None) => {
                self.follows.remove(agent);
                Poll::Ready(Ok(None))
            }
        }
    }

    /// Admitted selected follows, including pending opens, independent of swarm size.
    #[must_use]
    pub fn followed_agents(&self) -> usize {
        self.follows.len()
    }
}
