//! Agent records over the one Rust session and its bounded record fold.
//!
//! Both native and browser callers supply the platform's existing authenticated
//! Streams pool. This adapter owns no transport task, queue or recovery loop.

use super::{DemandPolicy, StreamView, ViewDemand, WireSession, WireSessionError};
use crate::agent::{AgentPath, AgentRecord, ConversationEntry, StatusRecord};
use acyclic_stream::{StreamPath, session::Client, wire, wire_codec};
use std::task::{Context, Poll};

/// A provider refusal retains its original machine-readable classification.
#[derive(Debug, thiserror::Error)]
pub enum AgentClientError {
    /// Canonical session admission, transport or authorization failure.
    #[error(transparent)]
    Session(#[from] WireSessionError),
    /// A bounded agent record failed its canonical decoder.
    #[error(transparent)]
    Record(#[from] crate::agent::Error),
    /// A selected stream ended; the client must not invent an idle status.
    #[error("selected agent stream ended: {0}")]
    Closed(StreamPath),
}

/// One observed transition, already applied to the shared bounded Rust fold.
#[derive(Debug)]
pub enum AgentEvent {
    /// A settled turn and its immutable output reference.
    Conversation {
        /// Sequence in the selected conversation stream.
        sequence: u64,
        /// Bounded canonical record; output bytes remain in the filesystem.
        entry: ConversationEntry,
    },
    /// A lifecycle transition from the selected agent's actual status path.
    Status(StatusRecord),
    /// Admission or watermark progress, without a fabricated record.
    Progress,
}

/// One focused agent, with its conversation and status multiplexed on the
/// caller's pool. The same WireSession folds records for native and WASM.
pub struct AgentClient {
    session: WireSession,
    agent: AgentPath,
    conversation: StreamPath,
    status: StreamPath,
    next_status: bool,
}

impl AgentClient {
    /// Follow from explicit exclusive cursors. Callers choose a bounded history
    /// page or retained cursor; opening never snapshots an entire conversation.
    /// The credential must already be registered on `client`.
    pub fn new(
        client: Client,
        agent: AgentPath,
        conversation_from: u64,
        status_from: u64,
        credential: u32,
    ) -> Result<Self, AgentClientError> {
        let conversation = agent.conversation()?;
        let status = agent.status()?;
        let policy = DemandPolicy::default();
        let demand = ViewDemand::new(
            policy,
            [conversation.clone(), status.clone()],
            std::iter::empty(),
            std::iter::empty(),
        )
        .map_err(WireSessionError::Demand)?;
        let mut session = WireSession::new(client, policy);
        session.set_view(demand)?;
        session.begin_stream(
            conversation.clone(),
            subscription(&conversation, conversation_from, credential),
        )?;
        session.begin_stream(status.clone(), subscription(&status, status_from, credential))?;
        Ok(Self { session, agent, conversation, status, next_status: false })
    }

    /// Selected agent identity; status paths come from AgentPath, never suffixes.
    pub const fn agent(&self) -> &AgentPath {
        &self.agent
    }

    /// Borrow the exact shared display window, including its eviction edge.
    pub fn conversation(&self) -> &StreamView {
        self.session.view(&self.conversation).expect("selected conversation owns a view")
    }

    /// Borrow the same core for counters and rendering; no second ClientViews.
    pub const fn session(&self) -> &WireSession {
        &self.session
    }

    /// Fairly poll only the two selected streams. There is no population scan
    /// and no per-agent queue, task or unbounded status cache.
    pub fn poll_next(&mut self, context: &mut Context<'_>) -> Poll<Result<AgentEvent, AgentClientError>> {
        let order = if self.next_status { [true, false] } else { [false, true] };
        for status in order {
            let path = if status { &self.status } else { &self.conversation };
            let frame = match self.session.poll_stream(path, context) {
                Poll::Pending => continue,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(None)) => return Poll::Ready(Err(AgentClientError::Closed(path.clone()))),
                Poll::Ready(Ok(Some(frame))) => frame,
            };
            self.next_status = !status;
            let event = match frame.body {
                Some(wire::session_frame::Body::Data(data)) => {
                    let Some(wire::session_data::Item::Record(record)) = data.item else {
                        return Poll::Ready(Err(WireSessionError::InvalidSubscription.into()));
                    };
                    if status {
                        StatusRecord::decode(&record.value).map(AgentEvent::Status)
                    } else {
                        ConversationEntry::decode(&record.value).map(|entry| AgentEvent::Conversation {
                            sequence: record.sequence,
                            entry,
                        })
                    }
                }
                Some(wire::session_frame::Body::Closed(_)) => {
                    return Poll::Ready(Err(AgentClientError::Closed(path.clone())));
                }
                _ => Ok(AgentEvent::Progress),
            };
            return Poll::Ready(event.map_err(AgentClientError::Record));
        }
        Poll::Pending
    }

    /// Wait without adding a task, channel or independent follow loop.
    pub async fn next(&mut self) -> Result<AgentEvent, AgentClientError> {
        futures::future::poll_fn(|context| self.poll_next(context)).await
    }
}

fn subscription(path: &StreamPath, from: u64, credential: u32) -> wire::SessionSubscription {
    wire::SessionSubscription {
        credential,
        channel: None,
        filter: None,
        resume: Some(wire::SessionCursor { next: from, opaque: bytes::Bytes::new() }),
        kind: Some(wire::session_subscription::Kind::Follow(wire::SessionFollow {
            resource: wire_codec::session::stream_resource(path.as_str()),
            delivery: wire::SessionDelivery::All as i32,
            ..Default::default()
        })),
    }
}
