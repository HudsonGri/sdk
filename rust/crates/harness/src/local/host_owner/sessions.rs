//! Native session mailboxes use the same files as `GraphCoder` child agents.
use super::*;
use crate::task_volume::{
    ChildFile, ChildRequest, ChildState, ChildStatus, ChildTurnResult, Component, Control, FileKey,
    FileValue,
};
use crate::{Outcome, executor::TurnOutput};

/// Coordination only. Native session processes are owned by the plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionAction {
    /// Admit a child with its immutable initial input.
    Spawn {
        /// Initial prompt.
        prompt: String,
    },
    /// Publish an immutable follow-up input.
    Send {
        /// Message body.
        prompt: String,
    },
    /// Claim the next input, after the preceding process is quiescent.
    Next,
    /// Capture and settle a process that has exited, including failed writes.
    Complete {
        /// Claimed input sequence.
        turn: u64,
        /// Native outcome.
        outcome: Outcome<TurnOutput>,
    },
    /// Save an opaque native resume id; never a transcript or credential.
    SaveSession {
        /// Native id.
        id: String,
    },
    /// Publish a native-host lifecycle intent in the shared control file.
    Control {
        /// Requested lifecycle state.
        control: Control,
    },
    /// Read the live shared mailbox and native resume reference.
    Observe,
    /// After fencing the old process custody, settle an orphaned turn.
    Recover,
}

#[derive(Clone)]
struct Session {
    parent: TaskId,
    child: TaskId,
    reference: Component,
    status: Option<ChildStatus>,
    state: ChildState,
    delivered: u64,
}
impl Session {
    fn key(&self, file: ChildFile) -> FileKey {
        FileKey::Child {
            reference: self.reference.clone(),
            file,
        }
    }
}

impl HostWorkspaceOwner {
    pub(super) async fn session(
        &mut self,
        operation: OperationId,
        parent: TaskId,
        reference: &str,
        action: SessionAction,
    ) -> Result<Value> {
        self.attach(parent).await?;
        let reference = Component::new(reference).map_err(storage)?;
        let child = self
            .admit_session(operation, parent, &reference, &action)
            .await?;
        let session = self.read_session(parent, child, reference).await?;
        match action {
            SessionAction::Send { prompt } => {
                if prompt.is_empty() || prompt.len() > 32_768 {
                    return Err(Error::Invalid("native message exceeds input bound".into()));
                }
                self.publish(
                    parent,
                    FileValue::new(
                        session.key(ChildFile::Inbox(
                            Component::new(operation.to_string()).map_err(storage)?,
                        )),
                        prompt.into_bytes(),
                    ),
                )
                .await?;
            }
            SessionAction::SaveSession { id } => self.save_native_session(child, id).await?,
            SessionAction::Control { control } => {
                self.control_session(operation, &session, control).await?;
            }
            SessionAction::Next => return self.next_session(&session).await,
            SessionAction::Complete { turn, outcome } => {
                self.complete_session(operation, &session, turn, outcome)
                    .await?;
            }
            SessionAction::Recover if session.status.is_some_and(|status| !status.settled) => {
                let turn = session
                    .status
                    .ok_or_else(|| Error::Invalid("native status absent".into()))?
                    .inputs;
                self.complete_session(operation, &session, turn, Outcome::Cancelled)
                    .await?;
            }
            SessionAction::Spawn { .. } | SessionAction::Observe | SessionAction::Recover => {}
        }
        self.observe_session(&session).await
    }

    async fn admit_session(
        &mut self,
        operation: OperationId,
        parent: TaskId,
        reference: &Component,
        action: &SessionAction,
    ) -> Result<TaskId> {
        let files = self.files(parent).await?;
        let key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        };
        let request = files
            .snapshot(&Query::File(key), 1)
            .await
            .map_err(storage)?;
        let prompt = match action {
            SessionAction::Spawn { prompt } if request.files.is_empty() => prompt.clone(),
            _ => {
                ChildRequest::parse(
                    &request
                        .files
                        .first()
                        .ok_or_else(|| Error::NotFound("native child request absent".into()))?
                        .bytes,
                    65_536,
                )?
                .prompt
            }
        };
        if prompt.is_empty() || prompt.len() > 32_768 {
            return Err(Error::Invalid("native prompt exceeds input bound".into()));
        }
        if let SessionAction::Spawn { prompt: supplied } = action
            && supplied != &prompt
        {
            return Err(Error::Conflict(
                "child reference already has another prompt".into(),
            ));
        }
        if request.files.is_empty() {
            let attached = self.attached(parent)?;
            attached
                .workspace
                .capture(&attached.context, operation)
                .await?;
        }
        let child = if let Some(request) = request.files.first() {
            self.local.admit_child_task(&files, request).await?
        } else {
            self.local
                .admit_host_child_request(
                    &self.attached(parent)?.context,
                    reference.as_str(),
                    &prompt,
                )
                .await?
        };
        self.attach_retained(child, !request.files.is_empty())
            .await?;
        self.attached(parent)?
            .workspace
            .name_child(
                &self.attached(parent)?.context,
                &self.attached(child)?.context,
                &self.attached(child)?.workspace,
                reference.as_str(),
            )
            .await?;
        Ok(child)
    }

    async fn read_session(
        &self,
        parent: TaskId,
        child: TaskId,
        reference: Component,
    ) -> Result<Session> {
        let files = self.files(parent).await?;
        let snapshot = files
            .inner
            .child_lifecycle(Some(&reference))
            .await
            .map_err(storage)?;
        let find = |kind: ChildFile| {
            snapshot.files.iter().find(|file| {
                file.key
                    == FileKey::Child {
                        reference: reference.clone(),
                        file: kind.clone(),
                    }
            })
        };
        let status = find(ChildFile::Status)
            .map(|file| ChildStatus::parse(&file.bytes))
            .transpose()
            .map_err(storage)?;
        let control = find(ChildFile::Control)
            .map(|file| Control::parse(&file.bytes))
            .transpose()
            .map_err(storage)?;
        let state = control.map_or(ChildState::Running, |control| {
            ChildState::Running.apply(control)
        });
        let delivered = find(ChildFile::Delivered)
            .map(|file| serde_json::from_slice::<(TaskId, u64)>(&file.bytes).map(|value| value.1))
            .transpose()
            .map_err(storage)?
            .unwrap_or(0);
        Ok(Session {
            parent,
            child,
            reference,
            status,
            state,
            delivered,
        })
    }

    async fn save_native_session(&self, child: TaskId, id: String) -> Result<()> {
        if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
            return Err(Error::Invalid("native resume id invalid".into()));
        }
        let key = super::events::context_key(child, "session")?;
        let previous = self
            .files(child)
            .await?
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        let previous = previous
            .files
            .first()
            .map(|file| serde_json::from_slice::<Value>(&file.bytes))
            .transpose()
            .map_err(storage)?;
        let session = previous
            .filter(|value| value.get("id").and_then(Value::as_str) == Some(id.as_str()))
            .unwrap_or_else(|| json!({"id":id}));
        self.publish(
            child,
            FileValue::new(key, serde_json::to_vec(&session).map_err(storage)?),
        )
        .await
    }

    async fn control_session(
        &self,
        operation: OperationId,
        session: &Session,
        control: Control,
    ) -> Result<()> {
        let cancelled_before_start = session.status.is_none()
            && matches!(
                control,
                Control::Interrupt | Control::Cancel | Control::Archive
            );
        let mut current = session.clone();
        current.state = ChildState::Running.apply(control);
        if cancelled_before_start {
            let status = ChildStatus {
                task: session.child,
                state: ChildState::Running,
                settled: false,
                inputs: 1,
            };
            self.publish(
                session.parent,
                FileValue::new(
                    session.key(ChildFile::Status),
                    serde_json::to_vec(&status).map_err(storage)?,
                ),
            )
            .await?;
            current.status = Some(status);
        }
        self.publish(
            session.parent,
            FileValue::new(session.key(ChildFile::Control), control.bytes().to_vec()),
        )
        .await?;
        if cancelled_before_start {
            self.complete_session(operation, &current, 1, Outcome::Cancelled)
                .await?;
        }
        if let Some(status) = session.status.filter(|value| value.settled) {
            self.publish(
                session.parent,
                FileValue::new(
                    session.key(ChildFile::Status),
                    serde_json::to_vec(&ChildStatus {
                        state: current.state,
                        ..status
                    })
                    .map_err(storage)?,
                ),
            )
            .await?;
        }
        Ok(())
    }

    async fn next_session(&self, session: &Session) -> Result<Value> {
        if session.status.is_some_and(|value| !value.settled) {
            return Err(Error::Conflict(
                "native session turn is already claimed".into(),
            ));
        }
        if session.state != ChildState::Running {
            return Ok(Value::Null);
        }
        let files = self.files(session.parent).await?;
        let Some(input) = files
            .inner
            .child_input(&session.reference, session.delivered)
            .await
            .map_err(storage)?
        else {
            return Ok(Value::Null);
        };
        let prompt = if matches!(
            input.key,
            FileKey::Child {
                file: ChildFile::Request,
                ..
            }
        ) {
            ChildRequest::parse(&input.bytes, 65_536)?.prompt
        } else {
            String::from_utf8(input.bytes.to_vec()).map_err(storage)?
        };
        let turn = session
            .delivered
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("native input sequence overflow".into()))?;
        let status = ChildStatus {
            task: session.child,
            state: session.state,
            settled: false,
            inputs: turn,
        };
        self.publish(
            session.parent,
            FileValue::new(
                session.key(ChildFile::Status),
                serde_json::to_vec(&status).map_err(storage)?,
            ),
        )
        .await?;
        self.publish(
            session.parent,
            FileValue::new(
                session.key(ChildFile::Delivered),
                serde_json::to_vec(&(session.child, turn)).map_err(storage)?,
            ),
        )
        .await?;
        let saved = self
            .files(session.child)
            .await?
            .snapshot(
                &Query::File(super::events::context_key(session.child, "session")?),
                1,
            )
            .await
            .map_err(storage)?;
        Ok(
            json!({"task":session.child,"cwd":self.attached(session.child)?.workspace.path(),"turn":turn,"prompt":prompt,"session":saved.files.first().map(|file| serde_json::from_slice::<Value>(&file.bytes)).transpose().map_err(storage)?}),
        )
    }

    async fn observe_session(&self, session: &Session) -> Result<Value> {
        let files = self.files(session.parent).await?;
        let snapshot = files
            .inner
            .child_lifecycle(Some(&session.reference))
            .await
            .map_err(storage)?;
        let status = snapshot
            .files
            .iter()
            .find(|file| file.key == session.key(ChildFile::Status))
            .map(|file| ChildStatus::parse(&file.bytes))
            .transpose()
            .map_err(storage)?;
        let control = snapshot
            .files
            .iter()
            .find(|file| file.key == session.key(ChildFile::Control))
            .map(|file| Control::parse(&file.bytes))
            .transpose()
            .map_err(storage)?;
        let results = snapshot
            .files
            .iter()
            .filter(|file| {
                matches!(
                    file.key,
                    FileKey::Child {
                        file: ChildFile::Result(_),
                        ..
                    }
                )
            })
            .map(|file| serde_json::from_slice::<Value>(&file.bytes))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(storage)?;
        let pending = files
            .inner
            .child_input_count(&session.reference)
            .await
            .map_err(storage)?
            .saturating_sub(status.map_or(0, |value| value.inputs));
        Ok(
            json!({"task":session.child,"cwd":self.attached(session.child)?.workspace.path(),"status":status,"control":control,"results":results,"pending":pending}),
        )
    }

    async fn complete_session(
        &self,
        operation: OperationId,
        session: &Session,
        turn: u64,
        outcome: Outcome<TurnOutput>,
    ) -> Result<()> {
        let status = session
            .status
            .ok_or_else(|| Error::Conflict("native completion has no claim".into()))?;
        if status.task != session.child || status.inputs != turn {
            return Err(Error::Conflict(
                "native completion differs from its claim".into(),
            ));
        }
        let attached = self.attached(session.child)?;
        attached
            .workspace
            .capture(&attached.context, operation)
            .await?;
        let files = self.files(session.parent).await?;
        let acknowledged = files
            .snapshot(&Query::File(session.key(ChildFile::Delivered)), 1)
            .await
            .map_err(storage)?;
        let delivered = acknowledged
            .files
            .first()
            .map(|file| serde_json::from_slice::<(TaskId, u64)>(&file.bytes))
            .transpose()
            .map_err(storage)?
            .map_or(0, |(_, turn)| turn);
        if delivered < turn {
            self.publish(
                session.parent,
                FileValue::new(
                    session.key(ChildFile::Delivered),
                    serde_json::to_vec(&(session.child, turn)).map_err(storage)?,
                ),
            )
            .await?;
        }
        let result = files
            .snapshot(&Query::File(session.key(ChildFile::Result(turn))), 1)
            .await
            .map_err(storage)?;
        if result.files.is_empty() {
            self.publish(
                session.parent,
                FileValue::new(
                    session.key(ChildFile::Result(turn)),
                    serde_json::to_vec(&ChildTurnResult {
                        task: session.child,
                        turn,
                        outcome,
                    })
                    .map_err(storage)?,
                ),
            )
            .await?;
        }
        self.publish(
            session.parent,
            FileValue::new(
                session.key(ChildFile::Status),
                serde_json::to_vec(&ChildStatus {
                    state: session.state,
                    settled: true,
                    ..status
                })
                .map_err(storage)?,
            ),
        )
        .await
    }
}
