//! Admission and ownership of frontend commands, using the public Rust codec.

use acyclic_harness_client::{
    AgentControlFailure, CommandError, MAX_AGENT_COMMAND_BYTES, MAX_AGENT_REPLY_BYTES,
    PreparedCommand, decode_agent_reply,
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
