//! Typed agent controls over the same canonical Actors session as observation.
use crate::{AgentControlFailure, AgentReply, CommandError, PreparedCommand, decode_agent_reply};
use acyclic_actors::{domain, wire};
use acyclic_stream::{StreamError, session::Client};
use bytes::Bytes;
use prost::Message as _;

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
    command: &'a PreparedCommand,
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
            command,
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
