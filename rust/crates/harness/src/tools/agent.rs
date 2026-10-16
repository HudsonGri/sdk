//! The default agent tool translates calls into task-volume files and live queries.
use crate::{
    Error, Result,
    conversation::Limits,
    runtime::{RuntimeScope, ToolContext},
    task_volume::{
        ChildFile, ChildRequest, ChildState, ChildStatus, Component, Control, FileKey,
        FileQuerySource, FileValue, Query, TaskFiles,
    },
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

mod binding;

/// File operations accepted by the default agent tool.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "command", rename_all = "lowercase", deny_unknown_fields)]
pub enum AgentCommand {
    /// Publish an immutable child request.
    Spawn {
        /// Reference scoped to the calling parent's private volume.
        reference: Component,
        /// Markdown child prompt.
        prompt: String,
        /// Optional model name within the configured provider.
        model: Option<String>,
        /// Optional attenuated tool selection.
        tools: Option<Vec<String>>,
    },
    /// Add one immutable inbox message.
    Send {
        /// Child reference.
        reference: Component,
        /// Message body.
        message: String,
    },
    /// Follow the child's status until its current work settles.
    Wait {
        /// Child reference.
        reference: Component,
    },
    /// Read the child's status.
    Status {
        /// Child reference.
        reference: Component,
    },
    /// Publish a lifecycle intent.
    Control {
        /// Child reference.
        reference: Component,
        /// Lifecycle action.
        action: Control,
    },
    /// Read active child references and their status.
    List,
}

pub(super) fn input_schema() -> Value {
    json!({
        "type":"object", "required":["command"],
        "properties":{
            "command":{"type":"string","enum":["spawn","send","wait","status","control","list"]},
            "reference":{"type":"string","minLength":1},
            "prompt":{"type":"string","minLength":1},
            "message":{"type":"string","minLength":1},
            "model":{"type":"string","minLength":1},
            "tools":{"type":"array","items":{"type":"string","minLength":1}},
            "action":{"type":"string","enum":["pause","resume","interrupt","cancel","archive"]}
        }, "additionalProperties":false
    })
}

pub(super) struct AgentExecutor;

impl ToolExecutor for AgentExecutor {
    fn authorize(
        &self,
        _: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "agent files require ordinary task execution",
            });
        }
        let _ = work;
        let context =
            context.ok_or_else(|| Error::Unauthorized("agent requires its task context".into()))?;
        if invocation.name != "agent"
            || context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || !context
                .effective_scope()
                .grants()
                .contains("tool:call:agent")
        {
            return Err(Error::Unauthorized(
                "agent call differs from its task authority".into(),
            ));
        }
        if context.task().owned_mailbox.is_none() {
            return Err(Error::Unauthorized(
                "agent requires an execution owner".into(),
            ));
        }
        if context.task().harness.task_files.is_none() {
            return Err(Error::Unsupported("agent task files are not bound".into()));
        }
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context =
                context.ok_or_else(|| Error::Unauthorized("agent context is absent".into()))?;
            let command: AgentCommand =
                serde_json::from_value(invocation.arguments).map_err(invalid)?;
            if let AgentCommand::Spawn { tools, .. } = &command {
                crate::task_volume::child_scope(context.effective_scope(), tools.as_deref())?;
            }
            let files = binding::files(&context).await?;
            let spawn = match &command {
                AgentCommand::Spawn { reference, .. } => Some(reference.clone()),
                _ => None,
            };
            let result = access(
                files.as_ref(),
                command,
                invocation.operation_id,
                context.effective_scope().limits(),
                false,
                context.task().owned_mailbox.as_deref(),
            )
            .await
            .and_then(|value| {
                value
                    .map(|value| ToolResult { value })
                    .ok_or(Error::Indeterminate(invocation.operation_id))
            })?;
            match spawn {
                Some(reference) => admission_status(
                    files.as_ref(),
                    &reference,
                    context.effective_scope().limits(),
                    true,
                )
                .await?
                .map(|value| ToolResult { value })
                .ok_or(Error::Indeterminate(invocation.operation_id)),
                None => Ok(result),
            }
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context =
                context.ok_or_else(|| Error::Unauthorized("agent context is absent".into()))?;
            let command: AgentCommand =
                serde_json::from_value(invocation.arguments).map_err(invalid)?;
            if let AgentCommand::Spawn { tools, .. } = &command {
                crate::task_volume::child_scope(context.effective_scope(), tools.as_deref())?;
            }
            let files = binding::files(&context).await?;
            let spawn = match &command {
                AgentCommand::Spawn { reference, .. } => Some(reference.clone()),
                _ => None,
            };
            let result = access(
                files.as_ref(),
                command,
                invocation.operation_id,
                context.effective_scope().limits(),
                true,
                context.task().owned_mailbox.as_deref(),
            )
            .await
            .map(|result| result.map(|value| ToolResult { value }))?;
            match (spawn, result) {
                (Some(reference), Some(_)) => admission_status(
                    files.as_ref(),
                    &reference,
                    context.effective_scope().limits(),
                    false,
                )
                .await
                .map(|result| result.map(|value| ToolResult { value })),
                (_, result) => Ok(result),
            }
        })
    }
}

async fn access<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    command: AgentCommand,
    operation: crate::OperationId,
    limits: Limits,
    reconcile_only: bool,
    owner: Option<&dyn crate::runtime::OwnedTaskMailbox>,
) -> Result<Option<Value>> {
    let maximum_bytes = usize::try_from(limits.render_bytes).map_err(invalid)?;
    let (reference, write, wait) = match command {
        AgentCommand::List => {
            let snapshot = files
                .observe_files(Query::Children, None, limits.attachments, maximum_bytes)
                .await
                .map_err(storage)?
                .snapshot
                .ok_or_else(|| Error::Conflict("child snapshot is absent".into()))?;
            let mut children = BTreeMap::<_, Vec<_>>::new();
            for file in snapshot.files {
                if let FileKey::Child { reference, .. } = &file.key {
                    children.entry(reference.clone()).or_default().push(file);
                }
            }
            let mut active = Vec::new();
            let archived = json!(ChildState::Archived);
            let cancelled = json!(ChildState::Cancelled);
            for (reference, files) in children {
                let status = status(&reference, &files)?;
                let terminal = status.get("state") == Some(&archived)
                    || status.get("state") == Some(&cancelled);
                if !terminal || status.get("settled") != Some(&Value::Bool(true)) {
                    active.push(status);
                }
            }
            return Ok(Some(json!({"agents":active})));
        }
        AgentCommand::Spawn {
            reference,
            prompt,
            model,
            tools,
        } => {
            if prompt.is_empty() {
                return Err(invalid("child prompt is empty"));
            }
            let request = ChildRequest {
                prompt,
                model,
                tools,
            };
            let bytes = request.encode();
            ChildRequest::parse(&bytes, bytes.len() as u64)?;
            (reference, Some((ChildFile::Request, bytes)), false)
        }

        AgentCommand::Send { reference, message } => {
            if message.is_empty() {
                return Err(invalid("child message is empty"));
            }
            (
                reference,
                Some((
                    ChildFile::Inbox(Component::new(operation.to_string()).map_err(storage)?),
                    message.into_bytes(),
                )),
                false,
            )
        }
        AgentCommand::Control { reference, action } => (
            reference,
            Some((ChildFile::Control, action.bytes().to_vec())),
            false,
        ),
        AgentCommand::Status { reference } => (reference, None, false),
        AgentCommand::Wait { reference } => (reference, None, true),
    };
    let mut observation = files
        .observe_files(
            Query::Child(reference.clone()),
            None,
            limits.attachments,
            maximum_bytes,
        )
        .await
        .map_err(storage)?;
    let snapshot = observation
        .snapshot
        .take()
        .ok_or_else(|| Error::Conflict("child snapshot is absent".into()))?;
    let mut values = snapshot.files;
    let request_exists = values.iter().any(|file| {
        matches!(
            file.key,
            FileKey::Child {
                file: ChildFile::Request,
                ..
            }
        )
    });
    if !request_exists && !matches!(write, Some((ChildFile::Request, _))) {
        return Err(Error::NotFound(format!("agents/{}", reference.as_str())));
    }
    if let Some((file, bytes)) = write {
        if bytes.len() as u64 > limits.file_bytes {
            return Err(invalid("child file exceeds the calling scope"));
        }
        let mut file = FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file,
            },
            bytes,
        );
        if matches!(
            file.key,
            FileKey::Child {
                file: ChildFile::Request,
                ..
            }
        ) {
            file.requester = owner.map(crate::runtime::OwnedTaskMailbox::task_id);
        }
        if reconcile_only {
            // A newer lifecycle write must never be overwritten during replay.
            // Immutable request/inbox identities can be recognized exactly.
            if !values.contains(&file) {
                return Ok(None);
            }
        } else {
            match owner {
                Some(owner) => {
                    let (condition, deadline) = owner.file_publication_condition().await?;
                    files
                        .write_if(
                            snapshot.generation,
                            &file,
                            owner.task_id(),
                            condition,
                            deadline,
                        )
                        .await
                }
                None => files.write(snapshot.generation, &file).await,
            }
            .map_err(storage)?;
        }
        values.retain(|previous| previous.key != file.key);
        values.push(file);
    }
    loop {
        let result = status(&reference, &values)?;
        if !wait || result.get("settled") == Some(&Value::Bool(true)) {
            return Ok(Some(result));
        }
        let change = observation
            .changes
            .next()
            .await
            .ok_or_else(|| Error::Conflict("child follow ended".into()))?
            .map_err(storage)?;
        if let Some(file) = change.file {
            values.retain(|previous| previous.key != file.key);
            values.push(file);
        }
    }
}

async fn admission_status<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    reference: &Component,
    limits: Limits,
    wait: bool,
) -> Result<Option<Value>> {
    let mut observation = files
        .observe_files(
            Query::Child(reference.clone()),
            None,
            limits.attachments,
            usize::try_from(limits.render_bytes).map_err(invalid)?,
        )
        .await
        .map_err(storage)?;
    let mut values = observation
        .snapshot
        .take()
        .ok_or_else(|| Error::Conflict("child admission snapshot missing".into()))?
        .files;
    loop {
        if values.iter().any(|file| matches!(&file.key,
            FileKey::Child { reference: candidate, file: ChildFile::Status } if candidate == reference)) {
            return status(reference, &values).map(Some);
        }
        if !wait {
            return Ok(None);
        }
        let change = observation
            .changes
            .next()
            .await
            .ok_or_else(|| Error::Conflict("child admission follow ended".into()))?
            .map_err(storage)?;
        values.retain(|file| !change.removed.contains(&file.key));
        for file in change.file.into_iter().chain(change.files) {
            values.retain(|previous| previous.key != file.key);
            values.push(file);
        }
    }
}

fn status(reference: &Component, files: &[FileValue]) -> Result<Value> {
    let mut state = ChildState::Running;
    let mut execution = None;
    // Requests and inbox files are immutable and cannot be deleted. Their
    // cardinality therefore detects every input accepted since a settled turn.
    let inputs = files
        .iter()
        .filter(|file| {
            matches!(
                &file.key,
                FileKey::Child {
                    file: ChildFile::Request | ChildFile::Inbox(_),
                    ..
                }
            )
        })
        .count() as u64;
    for file in files {
        match &file.key {
            FileKey::Child {
                file: ChildFile::Control,
                ..
            } => {
                state = state.apply(Control::parse(&file.bytes).map_err(storage)?);
            }
            FileKey::Child {
                file: ChildFile::Status,
                ..
            } => {
                execution = Some(ChildStatus::parse(&file.bytes).map_err(storage)?);
            }
            _ => {}
        }
    }
    let settled = execution.is_some_and(|execution| {
        execution.settled
            && execution.state == state
            && (matches!(
                state,
                ChildState::Cancelled | ChildState::Archived | ChildState::Interrupted
            ) || execution.inputs == inputs)
    });
    Ok(json!({"reference":reference,"state":state,"settled":settled,"execution":execution}))
}

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}
fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
#[path = "agent/tests.rs"]
mod tests;
