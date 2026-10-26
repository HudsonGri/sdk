//! Admission and ownership of frontend commands, using the public Rust codec.

use acyclic_harness_client::{
    AgentControlFailure, AgentExchangeError, CommandError, MAX_AGENT_COMMAND_BYTES,
    MAX_AGENT_REPLY_BYTES, PreparedAgentInvocation, PreparedCommand, decode_agent_reply,
};
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

fn command_error(cause: CommandError) -> JsValue {
    let error = js_sys::Error::new(&cause.to_string());
    error.set_name(cause.code());
    error.into()
}

/// One immutable admitted command. The caller owns it until the exchange has a
/// definitive result; transport interruption never creates a replacement intent.
#[wasm_bindgen]
pub struct WasmAgentCommand {
    command: PreparedCommand,
}

#[wasm_bindgen]
impl WasmAgentCommand {
    /// Canonical pre-copy limits, shared with endpoint conformance drivers.
    #[wasm_bindgen(js_name = maximumCommandBytes)]
    pub fn maximum_command_bytes() -> u32 {
        MAX_AGENT_COMMAND_BYTES as u32
    }
    #[wasm_bindgen(js_name = maximumInvocationBytes)]
    pub fn maximum_invocation_bytes() -> u32 {
        acyclic_stream::MAX_COMMAND_BYTES as u32
    }
    #[wasm_bindgen(js_name = maximumReplyBytes)]
    pub fn maximum_reply_bytes() -> u32 {
        MAX_AGENT_REPLY_BYTES as u32
    }

    /// Snapshot only after the encoded byte length passes the Rust limit.
    #[wasm_bindgen(constructor)]
    pub fn new(encoded: &Uint8Array) -> Result<WasmAgentCommand, JsValue> {
        let (bytes, ()) = super::bytes::snapshot(encoded, |length| {
            if length > MAX_AGENT_COMMAND_BYTES as u64 {
                Err(command_error(CommandError::Capacity))
            } else {
                Ok(())
            }
        })?;
        let command = PreparedCommand::from_bytes(bytes).map_err(command_error)?;
        Ok(Self { command })
    }

    /// An owned copy; callers cannot mutate the request retained in WASM.
    pub fn bytes(&self) -> Vec<u8> {
        self.command.as_bytes().to_vec()
    }

    /// Decode through the canonical Rust schema with exact integer values.
    #[wasm_bindgen(unchecked_return_type = "AgentCommand")]
    pub fn value(&self) -> Result<JsValue, JsValue> {
        let value = PreparedCommand::decode(self.command.as_bytes()).map_err(command_error)?;
        super::json::to_js_admitted(&value)
    }

    /// Pure preflight for all fixture/client inputs before any dispatch starts.
    #[wasm_bindgen(js_name = validateInvocation)]
    pub fn validate_invocation(&self, encoded: &Uint8Array) -> Result<(), JsValue> {
        let (bytes, ()) = super::bytes::snapshot(encoded, |length| {
            if length > acyclic_stream::MAX_COMMAND_BYTES as u64 {
                Err(command_error(CommandError::Capacity))
            } else {
                Ok(())
            }
        })?;
        PreparedAgentInvocation::new(&self.command, bytes.into())
            .map(|_| ())
            .map_err(command_error)
    }

    /// Execute this command once on the same pool used by AgentClient and
    /// filesystem follows. The host binding supplies the canonical Actors
    /// invocation; Rust refuses a body different from this held command.
    #[wasm_bindgen(unchecked_return_type = "AgentReply")]
    pub async fn execute(
        &self,
        stream: &acyclic_stream::WasmStream,
        invocation: &Uint8Array,
        credential: u32,
    ) -> Result<JsValue, JsValue> {
        let (bytes, ()) = super::bytes::snapshot(invocation, |length| {
            if length > acyclic_stream::MAX_COMMAND_BYTES as u64 {
                Err(command_error(CommandError::Capacity))
            } else {
                Ok(())
            }
        })?;
        let invocation =
            PreparedAgentInvocation::new(&self.command, bytes.into()).map_err(command_error)?;
        let session = stream.session_client().ok_or_else(|| {
            let error = js_sys::Error::new("an authenticated session is required");
            error.set_name("unsupported");
            JsValue::from(error)
        })?;
        match invocation.execute(&session, credential).await {
            Ok(reply) => super::json::to_js_admitted(&reply),
            Err(AgentExchangeError::Control(AgentControlFailure::Unknown(request))) => Err(
                super::agent_client::actor_error(crate::agent::Error::Unknown(request)),
            ),
            Err(AgentExchangeError::ActorStatus(status)) => {
                let error = js_sys::Error::new("actor invocation refused");
                error.set_name("actor_invocation_refused");
                js_sys::Reflect::set(
                    error.as_ref(),
                    &JsValue::from_str("status"),
                    &JsValue::from_f64(f64::from(status)),
                )?;
                Err(error.into())
            }
            Err(cause) => {
                let error = js_sys::Error::new(&cause.to_string());
                error.set_name(cause.code());
                Err(error.into())
            }
        }
    }

    /// Decode the host result against this exact original command. An uncertain
    /// result throws the shared typed error with its owned protobuf request.
    #[wasm_bindgen(unchecked_return_type = "AgentReply")]
    pub fn reply(&self, encoded: &Uint8Array) -> Result<JsValue, JsValue> {
        let (bytes, ()) = super::bytes::snapshot(encoded, |length| {
            if length > MAX_AGENT_REPLY_BYTES as u64 {
                Err(command_error(CommandError::Capacity))
            } else {
                Ok(())
            }
        })?;
        let command = PreparedCommand::decode(self.command.as_bytes()).map_err(command_error)?;
        match decode_agent_reply(&bytes, &command).map_err(command_error)? {
            Ok(reply) => super::json::to_js_admitted(&reply),
            Err(AgentControlFailure::Unknown(request)) => Err(super::agent_client::actor_error(
                crate::agent::Error::Unknown(request),
            )),
            Err(cause) => {
                let error = js_sys::Error::new(&cause.to_string());
                error.set_name(cause.code());
                Err(error.into())
            }
        }
    }
}
