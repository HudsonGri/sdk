//! Backend-neutral agent controls. Host dispatch supplies authenticated identity;
//! no request can assert its sender, approval responder or account membership.

use acyclic_harness_core::{
    agent::{AgentPath, IntentId},
    interaction::ApprovalBinding,
    model::ModelContent,
};
use serde::{Deserialize, Serialize};
use std::io::Write;

/// Finite encoded command budget, before decoding or starting any host work.
pub const MAX_COMMAND_BYTES: usize = 64 * 1024;
const MAX_CURSOR_BYTES: usize = 4096;

/// Requested page custody. Both ceilings are enforced by the host before it
/// returns a page; the cursor binds the exact query, authorization and cut.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "PageWire", into = "PageWire")]
pub struct PageRequest {
    items: u32,
    bytes: u32,
    cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageWire {
    #[serde(default = "default_items")]
    items: u32,
    #[serde(default = "default_bytes")]
    bytes: u32,
    #[serde(default)]
    cursor: Option<String>,
}

const fn default_items() -> u32 {
    32
}

const fn default_bytes() -> u32 {
    32 * 1024
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            items: default_items(),
            bytes: default_bytes(),
            cursor: None,
        }
    }
}

impl PageRequest {
    /// Validate finite page ceilings and retain the host's opaque cursor intact.
    pub fn new(items: u32, bytes: u32, cursor: Option<String>) -> Result<Self, CommandError> {
        if items == 0
            || items > 128
            || bytes == 0
            || bytes > MAX_COMMAND_BYTES as u32
            || cursor
                .as_ref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES)
        {
            return Err(CommandError::Capacity);
        }
        Ok(Self { items, bytes, cursor })
    }

    /// Maximum returned entries, including status/approval projections.
    #[must_use]
    pub const fn items(&self) -> u32 {
        self.items
    }

    /// Maximum encoded page bytes, including framing and the next cursor.
    #[must_use]
    pub const fn bytes(&self) -> u32 {
        self.bytes
    }

    /// Host-owned exclusive continuation; the client never edits it.
    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

impl TryFrom<PageWire> for PageRequest {
    type Error = CommandError;

    fn try_from(page: PageWire) -> Result<Self, Self::Error> {
        Self::new(page.items, page.bytes, page.cursor)
    }
}

impl From<PageRequest> for PageWire {
    fn from(page: PageRequest) -> Self {
        Self {
            items: page.items,
            bytes: page.bytes,
            cursor: page.cursor,
        }
    }
}

/// The same controls for local and hosted sessions. The host authorizes each
/// operation before provider I/O and stages content through its existing owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentCommand {
    /// Discover the root admitted by this session's credential.
    Root,
    /// Stage canonical user input and send one message with caller-owned intent.
    Prompt {
        /// Exact recipient; selection changes cannot retarget a prepared command.
        agent: AgentPath,
        /// Reuse with the same body after an uncertain delivery.
        id: IntentId,
        /// The canonical harness content type, including immutable file inputs.
        body: ModelContent,
    },
    /// Stop the current input without reporting it as successfully settled.
    Cancel {
        /// Exact recipient.
        agent: AgentPath,
        /// Stable cancellation identity.
        id: IntentId,
    },
    /// Spawn through the host's existing fork/volume publication mechanism.
    Spawn {
        /// Exact parent.
        parent: AgentPath,
        /// Stable spawning identity; the host derives the child identity.
        id: IntentId,
        /// Brief staged by the host, not a client-created volume grant.
        brief: ModelContent,
    },
    /// Discover only one bounded page of direct children.
    Children {
        /// Expanded tree node.
        agent: AgentPath,
        /// Whole-page limits and the unchanged host continuation.
        #[serde(default)]
        page: PageRequest,
    },
    /// Read pending approvals through the host's authorized interaction reader.
    Approvals {
        /// Agent whose approvals are being displayed.
        agent: AgentPath,
        /// Whole-page limits and the unchanged host continuation.
        #[serde(default)]
        page: PageRequest,
    },
    /// Decide the exact displayed action, then use canonical approval delivery.
    Decide {
        /// Agent that owns the request.
        agent: AgentPath,
        /// Immutable approval identity.
        id: IntentId,
        /// Prevents a decision from authorizing a different action.
        binding: ApprovalBinding,
        /// Human decision; the responder is derived from verified authority.
        approved: bool,
        /// Optional explanation, charged to the command byte limit.
        reason: Option<String>,
    },
}

impl AgentCommand {
    fn validate(&self) -> Result<(), CommandError> {
        match self {
            Self::Prompt { body, .. } | Self::Spawn { brief: body, .. } => body
                .validate_user_input(&mut None)
                .map_err(|_| CommandError::Invalid),
            Self::Decide { binding, .. } => binding.validate().map_err(|_| CommandError::Invalid),
            _ => Ok(()),
        }
    }
}

/// Immutable admitted bytes retained by the caller while a command is in flight.
/// Transport loss cannot turn an old intent into a newly reconstructed command.
pub struct PreparedCommand(Vec<u8>);

/// Typed local refusal before transport or host execution begins.
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// A command, page or cursor exceeds its finite budget.
    #[error("client command capacity exceeded")]
    Capacity,
    /// The command is malformed or contains fields outside this protocol.
    #[error("invalid client command")]
    Invalid,
}

impl CommandError {
    /// Stable classification shared by native and WASM adapters.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Capacity => "client_command_capacity",
            Self::Invalid => "invalid_client_command",
        }
    }
}

impl PreparedCommand {
    /// Encode without ever retaining an oversized serialization buffer.
    pub fn new(command: &AgentCommand) -> Result<Self, CommandError> {
        command.validate()?;
        let mut writer = CommandWriter {
            // One admitted buffer; geometric growth cannot exceed the budget.
            bytes: Vec::with_capacity(MAX_COMMAND_BYTES),
            full: false,
        };
        if serde_json::to_writer(&mut writer, command).is_err() {
            return Err(if writer.full {
                CommandError::Capacity
            } else {
                CommandError::Invalid
            });
        }
        Ok(Self(writer.bytes))
    }

    /// Decode only after bounding the entire untrusted envelope.
    pub fn decode(bytes: &[u8]) -> Result<AgentCommand, CommandError> {
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(CommandError::Capacity);
        }
        let command: AgentCommand =
            serde_json::from_slice(bytes).map_err(|_| CommandError::Invalid)?;
        command.validate()?;
        Ok(command)
    }

    /// Exact owned request body; intent and payload stay unchanged on transport.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

struct CommandWriter {
    bytes: Vec<u8>,
    full: bool,
}

impl Write for CommandWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_COMMAND_BYTES - self.bytes.len() {
            self.full = true;
            return Err(std::io::Error::other("client command capacity exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_cannot_assert_a_principal() {
        let command = br#"{"kind":"cancel","agent":"harness/agents/00000000000000000000000000000001","id":"cancel-1","principal":"administrator"}"#;
        assert!(PreparedCommand::decode(command).is_err());
    }

    #[test]
    fn opaque_page_and_finite_defaults_survive_decode() -> Result<(), Box<dyn std::error::Error>> {
        let page: PageRequest = serde_json::from_str(r#"{"cursor":"cut/opaque+token="}"#)?;
        assert_eq!(page.items(), 32);
        assert_eq!(page.bytes(), 32 * 1024);
        assert_eq!(page.cursor(), Some("cut/opaque+token="));
        assert!(serde_json::from_str::<PageRequest>(r#"{"items":0}"#).is_err());
        assert!(serde_json::from_str::<PageRequest>(r#"{"bytes":4294967295}"#).is_err());
        Ok(())
    }

    #[test]
    fn encoded_expansion_is_bounded_before_dispatch() -> Result<(), Box<dyn std::error::Error>> {
        let command = AgentCommand::Prompt {
            agent: AgentPath::try_from("harness/agents/00000000000000000000000000000001".to_owned())?,
            id: IntentId::try_from("prompt-1")?,
            // JSON escaping expands each byte; raw text length is insufficient.
            body: ModelContent::Text("\n".repeat(MAX_COMMAND_BYTES / 2)),
        };
        assert!(matches!(
            PreparedCommand::new(&command),
            Err(CommandError::Capacity)
        ));
        assert!(matches!(
            PreparedCommand::decode(&vec![b' '; MAX_COMMAND_BYTES + 1]),
            Err(CommandError::Capacity)
        ));
        Ok(())
    }

    #[test]
    fn prepared_prompt_preserves_the_intent_and_body() -> Result<(), Box<dyn std::error::Error>> {
        let command = AgentCommand::Prompt {
            agent: AgentPath::try_from("harness/agents/00000000000000000000000000000001".to_owned())?,
            id: IntentId::try_from("prompt-1")?,
            body: ModelContent::Text("exact input".into()),
        };
        let prepared = PreparedCommand::new(&command)?;
        let AgentCommand::Prompt { id, body, .. } = PreparedCommand::decode(prepared.as_bytes())?
        else {
            return Err("expected prompt command".into());
        };
        assert_eq!(id.as_str(), "prompt-1");
        assert_eq!(body, ModelContent::Text("exact input".into()));
        Ok(())
    }
}
