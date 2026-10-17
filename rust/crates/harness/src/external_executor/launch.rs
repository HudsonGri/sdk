//! A native executor declaration retained as an ordinary admitted input file.
use super::{ChildSession, ExternalAction, ExternalHost, ExternalRequest, normalize, volume};
use crate::{Error, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Native arguments are literal argv entries, never shell interpolation.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Argument {
    Text(String),
    SessionFile,
    SessionId,
    Prompt,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Executor {
    pub session: ChildSession,
    pub executable: PathBuf,
    pub arguments: Vec<Argument>,
    pub session_bytes: String,
    pub timeout_ms: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Spawn {
    pub id: String,
    pub prompt: String,
    pub executor: Executor,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Launch<T = ExternalRequest> {
    pub parent: TaskId,
    pub request: T,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PiHeader {
    #[serde(rename = "type")]
    kind: String,
    version: u32,
    id: String,
    timestamp: String,
    cwd: String,
    #[serde(
        default,
        rename = "parentSession",
        skip_serializing_if = "Option::is_none"
    )]
    parent_session: Option<String>,
}

pub(crate) fn requested(request: &ExternalRequest) -> bool {
    matches!(&request.action, ExternalAction::RunTool {name, arguments, ..}
        if name == "acyclic_spawn" &&
            ["id", "prompt", "executor"].iter().any(|key| arguments.get(key).is_some()))
}

impl Spawn {
    pub(crate) fn from_request(request: &ExternalRequest) -> Result<Self> {
        let ExternalAction::RunTool { arguments, .. } = &request.action else {
            return Err(invalid());
        };
        if !requested(request) {
            return Err(invalid());
        }
        let spawn: Self = Self::deserialize(arguments).map_err(|_| invalid())?;
        if !super::request::valid_text(&spawn.id)
            || !super::request::valid_text(&spawn.prompt)
            || !spawn.executor.session.valid()
            || !spawn.executor.executable.is_absolute()
            || spawn
                .executor
                .executable
                .as_os_str()
                .as_encoded_bytes()
                .contains(&0)
            || spawn
                .executor
                .executable
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
            || spawn.executor.timeout_ms == 0
            || spawn
                .executor
                .arguments
                .iter()
                .any(|argument| matches!(argument, Argument::Text(text) if text.contains('\0')))
            || !spawn
                .executor
                .arguments
                .iter()
                .any(|argument| matches!(argument, Argument::Prompt))
        {
            return Err(invalid());
        }
        if spawn.executor.session.host == ExternalHost::Pi {
            let header: PiHeader =
                volume::decode(spawn.executor.session_bytes.as_bytes()).map_err(|_| invalid())?;
            if header.kind != "session"
                || header.id != spawn.executor.session.session_id
                || uuid::Uuid::parse_str(&header.id).is_err()
                || header.version == 0
                || !super::request::valid_text(&header.timestamp)
                || !super::request::valid_text(&header.cwd)
                || header.parent_session.is_some()
                || spawn.executor.session.caller.is_some()
            {
                return Err(invalid());
            }
        }
        Ok(spawn)
    }

    pub(crate) fn fork(&self, source: &ExternalRequest) -> ExternalRequest {
        let action = ExternalAction::Fork {
            child_id: self.id.clone(),
            kind: None,
            child_session: self.executor.session.clone(),
        };
        ExternalRequest {
            operation_id: normalize::operation(
                source.host,
                &source.session_id,
                &source.caller,
                &action,
            ),
            host: source.host,
            session_id: source.session_id.clone(),
            turn_id: source.turn_id.clone(),
            caller: source.caller.clone(),
            action,
        }
    }

    pub(crate) fn arguments(&self, session: &Path) -> Result<Vec<String>> {
        self.executor
            .arguments
            .iter()
            .map(|argument| {
                Ok(match argument {
                    Argument::Text(text) => text.clone(),
                    Argument::SessionFile => session.to_str().ok_or_else(invalid)?.into(),
                    Argument::SessionId => self.executor.session.session_id.clone(),
                    Argument::Prompt => self.prompt.clone(),
                })
            })
            .collect()
    }

    pub(crate) fn attachment(
        &self,
        source: &ExternalRequest,
        workspace: &str,
    ) -> Result<ExternalRequest> {
        self.fork(source).child_attachment(workspace)
    }

    pub(crate) fn seed(&self, root: &Path) -> Result<Vec<u8>> {
        if self.executor.session.host != ExternalHost::Pi {
            return Ok(self.executor.session_bytes.as_bytes().to_vec());
        }
        let mut header: PiHeader =
            volume::decode(self.executor.session_bytes.as_bytes()).map_err(|_| invalid())?;
        header.cwd = root.to_str().ok_or_else(invalid)?.into();
        let mut bytes = volume::encode(&header).map_err(|_| invalid())?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

impl Launch {
    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, Spawn)> {
        let launch: Self = volume::decode(bytes).map_err(|_| invalid())?;
        if !launch.request.valid() {
            return Err(invalid());
        }
        let spawn = Spawn::from_request(&launch.request)?;
        Ok((launch, spawn))
    }
}

pub(crate) fn process_operation(child: TaskId) -> crate::OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.native-process-task/v1\0");
    hash.update(&child.into_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    crate::OperationId::from_bytes(id)
}

fn invalid() -> Error {
    Error::Invalid("invalid native executor declaration".into())
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn fuzz(bytes: &[u8]) {
    if let Ok((launch, spawn)) = Launch::decode(bytes) {
        let encoded = volume::encode(&launch).unwrap();
        assert_eq!(Launch::decode(&encoded).unwrap(), (launch, spawn));
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
