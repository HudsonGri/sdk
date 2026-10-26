//! Typed agent controls over the same canonical Actors session as observation.
use crate::{AgentControlFailure, AgentReply, CommandError, PreparedCommand, decode_agent_reply};
use acyclic_actors::{domain, wire};
use acyclic_stream::{StreamError, session::Client};
use bytes::Bytes;
use prost::Message as _;
use std::borrow::Cow;

/// A host-published canonical Actors address and dispatch metadata. It contains
/// no command body or reusable per-invocation ordering sequence. Authentication
/// remains the session credential, not this routing value.
pub struct AgentBinding(wire::InvokeActorRequest);

impl AgentBinding {
    /// Admit bounded canonical host metadata before retaining it. The host uses
    /// the same binding for local, Actors and managed deployments.
    pub fn new(encoded: &[u8]) -> Result<Self, CommandError> {
        if encoded.len() > acyclic_stream::MAX_COMMAND_BYTES {
            return Err(CommandError::Capacity);
        }
        let request =
            wire::InvokeActorRequest::decode(encoded).map_err(|_| CommandError::Invalid)?;
        if !request.body.is_empty()
            || request.invocation_order.is_some()
            || request.encode_to_vec().as_slice() != encoded
        {
            return Err(CommandError::Invalid);
        }
        domain::InvokeActorRequest::try_from(request.clone()).map_err(|_| CommandError::Invalid)?;
        Ok(Self(request))
    }

    /// Attach exact admitted command bytes in Rust. No TS routing codec, fresh
    /// intent or provider-specific control path is needed.
    pub fn prepare<'a>(
        &self,
        command: &'a PreparedCommand,
    ) -> Result<PreparedAgentInvocation<'a>, CommandError> {
        PreparedAgentInvocation::new(command, self.encode(command)?)
    }

    /// Own both the original command and invocation across browser awaits.
    pub fn prepare_owned(
        &self,
        command: PreparedCommand,
    ) -> Result<PreparedAgentInvocation<'static>, CommandError> {
        let encoded = self.encode(&command)?;
        let prepared = PreparedAgentInvocation::new(&command, encoded.clone())?;
        drop(prepared);
        Ok(PreparedAgentInvocation {
            command: Cow::Owned(command),
            invocation: encoded,
        })
    }

    fn encode(&self, command: &PreparedCommand) -> Result<Bytes, CommandError> {
        // Body framing adds at most five bytes for this finite command budget.
        // Check before cloning the routing metadata or allocating its body.
        if self.0.encoded_len() + command.as_bytes().len() + 5 > acyclic_stream::MAX_COMMAND_BYTES {
            return Err(CommandError::Capacity);
        }
        let mut invocation = self.0.clone();
        invocation.body = command.as_bytes().to_vec();
        Ok(invocation.encode_to_vec().into())
    }
}

/// Controls use the very same pool and credential as the selected agent fold.
/// Cloning the pool handle creates no connection, task, queue or result index.
pub struct AgentControls {
    client: Client,
    credential: u32,
    binding: AgentBinding,
}

impl AgentControls {
    /// Compose the host's existing session and published binding. Endpoint
    /// bootstrap belongs to the host; this constructor invents no routing.
    pub fn new(client: Client, credential: u32, binding: AgentBinding) -> Self {
        Self {
            client,
            credential,
            binding,
        }
    }

    /// Start bounded observation on this same authenticated pool and slot.
    pub fn observe(
        &self,
        selection: crate::AgentSelection,
    ) -> Result<crate::AgentClient, crate::AgentClientError> {
        crate::AgentClient::new(
            self.client.clone(),
            selection.agent,
            selection.conversation_from,
            selection.status_from,
            self.credential,
        )
    }

    /// Prepare and retain an exact invocation before starting any transport.
    pub fn prepare(
        &self,
        command: PreparedCommand,
    ) -> Result<PreparedAgentInvocation<'static>, CommandError> {
        self.binding.prepare_owned(command)
    }

    /// Execute once. The caller retains the prepared value on every outcome.
    pub async fn execute(
        &self,
        invocation: &PreparedAgentInvocation<'_>,
    ) -> Result<AgentReply, AgentExchangeError> {
        invocation.execute(&self.client, self.credential).await
    }
}

/// An interrupted exchange is distinct from a definitive host refusal. The
/// caller's borrowed invocation remains available with its exact original intent.
#[derive(Debug, thiserror::Error)]
pub enum AgentExchangeError {
    #[error("agent exchange interrupted: {0}")]
    Interrupted(StreamError),
    #[error(transparent)]
    Command(#[from] CommandError),
    #[error(transparent)]
    Control(#[from] AgentControlFailure),
    #[error("actor invocation refused with status {0}")]
    ActorStatus(u32),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentCommand;

    fn invocation(body: Vec<u8>) -> wire::InvokeActorRequest {
        wire::InvokeActorRequest {
            actor_id: "fixture".into(),
            method: "POST".into(),
            url: "https://fixture.invalid/command".into(),
            body,
            ..Default::default()
        }
    }

    #[test]
    fn invocation_retains_exact_command_and_rejects_another_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = PreparedCommand::new(&AgentCommand::Root)?;
        let bytes: Bytes = invocation(command.as_bytes().to_vec())
            .encode_to_vec()
            .into();
        let held = PreparedAgentInvocation::new(&command, bytes.clone())?;
        assert_eq!(held.as_bytes(), &bytes);
        let changed = invocation(b"{}".to_vec()).encode_to_vec().into();
        assert!(matches!(
            PreparedAgentInvocation::new(&command, changed),
            Err(CommandError::Invalid)
        ));
        Ok(())
    }

    #[test]
    fn invocation_does_not_discard_unknown_wire_fields() -> Result<(), Box<dyn std::error::Error>> {
        let command = PreparedCommand::new(&AgentCommand::Root)?;
        let mut encoded = invocation(command.as_bytes().to_vec()).encode_to_vec();
        encoded.extend_from_slice(&[0xf8, 0x07, 0x01]); // unknown varint field 127
        assert!(matches!(
            PreparedAgentInvocation::new(&command, encoded.into()),
            Err(CommandError::Invalid)
        ));
        Ok(())
    }

    #[test]
    fn binding_preserves_routing_and_original_owned_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let template = invocation(Vec::new());
        let binding = AgentBinding::new(&template.encode_to_vec())?;
        let command = PreparedCommand::from_bytes(b"{ \"kind\" : \"root\" }".to_vec())?;
        let original = command.as_bytes().to_vec();
        let prepared = binding.prepare_owned(command)?;
        let request = wire::InvokeActorRequest::decode(prepared.as_bytes().clone())?;
        assert_eq!(request.body, original);
        assert_eq!(request.actor_id, template.actor_id);
        assert_eq!(request.url, template.url);
        assert_eq!(request.method, template.method);
        Ok(())
    }

    #[test]
    fn binding_cannot_reuse_an_order_or_hide_a_command_body() {
        let body = invocation(b"{\"kind\":\"root\"}".to_vec());
        assert!(matches!(
            AgentBinding::new(&body.encode_to_vec()),
            Err(CommandError::Invalid)
        ));
        let mut ordered = invocation(Vec::new());
        ordered.invocation_order = Some(wire::InvocationOrder {
            session: "0".repeat(32),
            sequence: 1,
        });
        assert!(matches!(
            AgentBinding::new(&ordered.encode_to_vec()),
            Err(CommandError::Invalid)
        ));
    }
}

impl AgentExchangeError {
    /// Stable classification without flattening an owned uncertain request.
    pub fn code(&self) -> &str {
        match self {
            Self::Interrupted(_) => "agent_exchange_interrupted",
            Self::Command(cause) => cause.code(),
            Self::Control(cause) => cause.code(),
            Self::ActorStatus(_) => "actor_invocation_refused",
        }
    }
}

/// One exact command bound to its canonical Actors invocation. Host composition
/// supplies the actor/method binding; neither the client nor transport invents a
/// backend-specific route. The command remains caller-owned across every await.
pub struct PreparedAgentInvocation<'a> {
    command: Cow<'a, PreparedCommand>,
    invocation: Bytes,
}

impl<'a> PreparedAgentInvocation<'a> {
    /// Validate the canonical Actors envelope before opening a session channel.
    /// Its body must be the exact admitted command bytes, including the intent.
    pub fn new(command: &'a PreparedCommand, encoded: Bytes) -> Result<Self, CommandError> {
        if encoded.len() > acyclic_stream::MAX_COMMAND_BYTES {
            return Err(CommandError::Capacity);
        }
        let invocation = wire::InvokeActorRequest::decode(encoded.as_ref())
            .map_err(|_| CommandError::Invalid)?;
        if invocation.encode_to_vec().as_slice() != encoded.as_ref()
            || &invocation.body[..] != command.as_bytes()
        {
            return Err(CommandError::Invalid);
        }
        domain::InvokeActorRequest::try_from(invocation).map_err(|_| CommandError::Invalid)?;
        Ok(Self {
            command: Cow::Borrowed(command),
            invocation: encoded,
        })
    }

    /// The original encoded invocation, retained unchanged after interruption.
    pub fn as_bytes(&self) -> &Bytes {
        &self.invocation
    }

    /// Execute once on the caller's existing session. This method installs no
    /// queue, follow loop, retry, result index or backend-specific error mapping.
    pub async fn execute(
        &self,
        session: &Client,
        credential: u32,
    ) -> Result<AgentReply, AgentExchangeError> {
        let bytes = session
            .actor_call(
                &self.invocation,
                credential,
                acyclic_stream::wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES,
            )
            .await
            .map_err(AgentExchangeError::Interrupted)?;
        let response =
            wire::InvokeActorResponse::decode(bytes.as_ref()).map_err(|_| CommandError::Invalid)?;
        if response.encode_to_vec().as_slice() != bytes.as_ref() {
            return Err(CommandError::Invalid.into());
        }
        let response =
            domain::InvokeActorResponse::try_from(response).map_err(|_| CommandError::Invalid)?;
        if response.status() != 200 {
            return Err(AgentExchangeError::ActorStatus(response.status()));
        }
        let command = PreparedCommand::decode(self.command.as_bytes())?;
        decode_agent_reply(response.body(), &command)?.map_err(AgentExchangeError::Control)
    }
}
