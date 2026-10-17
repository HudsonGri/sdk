use super::*;
use crate::task_volume::{
    Channel, Component, FileKey, MODEL_CHUNK_BYTES, MODEL_CHUNK_STRIDE, TaskFile, Write,
};

#[derive(Serialize)]
struct ModelIdentity<'a> {
    provider: &'a str,
    name: &'a str,
    revision: &'a str,
}

#[derive(Serialize)]
struct TurnRequest<'a> {
    model: ModelIdentity<'a>,
    request: &'a FileRef,
}

#[derive(Serialize)]
#[serde(untagged)]
enum TurnCompletion<'a> {
    Completed { metadata: &'a serde_json::Value },
    Failed { error: &'a str },
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn publish_model_outcome_file(
        &self,
        operation: OperationId,
        step: u32,
        outcome: std::result::Result<&serde_json::Value, (usize, &Error)>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let Some(files) = &self.task_files else {
            return Ok(());
        };
        self.require_operation(operation, io)?;
        let turn = <Self as ExecutionJournal>::model_file_turn(self, operation, step, io)?.await?;
        let (owner, _) = self.owner.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "model completion has no task owner",
            )
        })?;
        let message = outcome
            .err()
            .map(|(_, error)| {
                crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(format_args!("{error}"), work)
                })
            })
            .transpose()?;
        let _message_hold = io
            .zip(message.as_ref())
            .map(|(io, message)| io.guard_owned(message.capacity() as u64))
            .transpose()?;
        let file = match outcome {
            Ok(_) => TaskFile::Completion(turn),
            Err((sequence, _)) => TaskFile::Error {
                turn,
                sequence: sequence as u64,
            },
        };
        let completion = match outcome {
            Ok(metadata) => TurnCompletion::Completed { metadata },
            Err(_) => TurnCompletion::Failed {
                error: message.as_deref().ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model failure lost its diagnostic",
                    )
                })?,
            },
        };
        let bytes = match io {
            Some(io) => io.encode(&completion, files.maximum_bytes(), true)?,
            None => crate::contract::canonical_json_bytes(&completion)?.into(),
        };
        let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let task = Component::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?)?;
        self.publish_model_file(&Write::new(FileKey::Task { task, file }, bytes), io)
            .await
    }

    pub(super) fn model_publication(
        operation: OperationId,
        step: u32,
        io: Option<&PreparationIo>,
    ) -> Result<OperationId> {
        let digest = crate::executor::preparation_digest(&("model-turn", operation, step), io)?;
        let mut identity = [0; 16];
        identity.copy_from_slice(&digest[..16]);
        Ok(OperationId::from_bytes(identity))
    }

    pub(super) async fn publish_model_request(
        &self,
        operation: OperationId,
        step: u32,
        request: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let Some(files) = &self.task_files else {
            return Ok(());
        };
        let Some((owner, _)) = &self.owner else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "model request has no task owner",
            ));
        };
        let task = Component::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?)?;
        let publication = Self::model_publication(operation, step, io)?;
        let generation = files.inner.head().await?;
        let turn = files
            .inner
            .published_turn(&task, publication)
            .await?
            .unwrap_or(generation.position);
        let (_request_hold, prepared) = {
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let prepared =
                crate::executor::load_model_request(self, request, owner.input_limits(), io)
                    .await?;
            let hold = io
                .map(|io| io.guard_owned(io.owned_since(before)?))
                .transpose()?;
            (hold, prepared)
        };
        let model = &prepared.request().model;
        let header = TurnRequest {
            model: ModelIdentity {
                provider: &model.provider,
                name: &model.name,
                revision: &model.revision,
            },
            request,
        };
        let bytes = match io {
            Some(io) => io.encode(&header, self.maximum_payload_bytes, true)?,
            None => crate::contract::canonical_json_bytes(&header)?.into(),
        };
        let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let mut write = Write::new(
            FileKey::Task {
                task,
                file: TaskFile::TurnRequest(turn),
            },
            bytes,
        );
        write.publication = Some(publication);
        self.publish_model_file(&write, io).await
    }

    pub(super) async fn stage_model_delta(
        &self,
        operation: OperationId,
        step: u32,
        sequence: usize,
        event: &ModelEvent,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        let files = self.task_files.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "model task files are not bound",
            )
        })?;
        let Some((owner, _)) = &self.owner else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "model chunk publication requires its task owner",
            ));
        };
        let (delta, channel) = match event {
            ModelEvent::Content { delta } => (delta, Channel::Text),
            ModelEvent::Reasoning { delta } => (delta, Channel::Reasoning),
            _ => {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model event is not a text delta",
                ));
            }
        };
        if delta.len() as u64 > files.maximum_bytes() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model chunk exceeds task file bound",
            ));
        }
        let task = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?;
        let task = Component::new(task)?;
        let turn = files
            .inner
            .published_turn(&task, Self::model_publication(operation, step, io)?)
            .await?
            .ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "model chunk has no published request",
                )
            })?;
        let base = u64::try_from(sequence)
            .ok()
            .and_then(|sequence| sequence.checked_mul(MODEL_CHUNK_STRIDE))
            .ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model chunk sequence overflow",
                )
            })?;
        // A large provider event must remain recoverable even if the process
        // dies after its first part. Stage its original boundary before exposing
        // any part. Small deltas refer directly to their sole raw chunk.
        let mut reference = if delta.len() > MODEL_CHUNK_BYTES {
            let key = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("model:{step}:{sequence}"), work)
            })?;
            Some(crate::executor::stage_event_json(self, operation, &key, event, io).await?)
        } else {
            None
        };
        let mut offset = 0;
        let mut part = 0u64;
        loop {
            let mut end = offset.saturating_add(MODEL_CHUNK_BYTES).min(delta.len());
            while !delta.is_char_boundary(end) {
                end -= 1;
            }
            if part >= MODEL_CHUNK_STRIDE {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model chunk part overflow",
                ));
            }
            let bytes = crate::executor::stock_string_copy(&delta[offset..end], io)?.into_bytes();
            let bytes = crate::executor::with_history_io(io, |work| {
                crate::store::operations::owned_bytes_from_retained_vec(bytes, work)
                    .map(|value| value.0)
            })?;
            let _bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            let mut write = Write::new(
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::Chunk {
                        turn,
                        sequence: base + part,
                    },
                },
                bytes,
            );
            write.channel = Some(channel);
            self.publish_model_file(&write, io).await?;
            if reference.is_none() {
                reference = Some(self.model_delta_reference(&write, io)?);
            }
            if end == delta.len() {
                return reference.ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model chunk lost its reference",
                    )
                });
            }
            offset = end;
            part += 1;
        }
    }

    fn model_delta_reference(&self, write: &Write, io: Option<&PreparationIo>) -> Result<FileRef> {
        let (
            Some(files),
            FileKey::Task {
                task,
                file: TaskFile::Chunk { turn, sequence },
            },
            Some(channel),
        ) = (&self.task_files, &write.key, write.channel)
        else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model delta is not a task chunk",
            ));
        };
        crate::executor::with_history_io(io, |work| {
            let media = match channel {
                Channel::Text => crate::executor::TEXT_DELTA,
                Channel::Reasoning => crate::executor::REASONING_DELTA,
            };
            let descriptor =
                crate::conversation::FileDescriptor::from_bytes(&write.bytes, media, work)?;
            let path = crate::executor::preparation_format(
                format_args!(".tasks/{}/turns/{turn}/chunks/{sequence}", task.as_str()),
                work,
            )?;
            let version = crate::executor::preparation_format(
                format_args!(
                    "task-chunk:{}",
                    blake3::Hash::from_bytes(*descriptor.sha256())
                ),
                work,
            )?;
            let display = crate::executor::preparation_format(format_args!("chunk"), work)?;
            FileRef::new(
                files.volume().copy(work)?,
                path,
                version,
                descriptor,
                display,
                work,
            )
        })
    }

    pub(super) async fn recover_published_model_delta(
        &self,
        operation: OperationId,
        step: u32,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let Some(files) = &self.task_files else {
            return Ok(());
        };
        self.require_operation(operation, io)?;
        let (owner, _) = self.owner.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "model recovery has no task owner",
            )
        })?;
        let task = Component::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?)?;
        let Some(turn) = files
            .inner
            .published_turn(&task, Self::model_publication(operation, step, io)?)
            .await?
        else {
            return Ok(());
        };
        let Some((index, file)) = files.inner.latest_model_file(&task, turn).await? else {
            return Ok(());
        };
        // The stock producer awaits the referring append before accepting the
        // next delta, so only its final accepted chunk can lack an observation.
        // Reading that file repairs the crash boundary without publishing it again.
        let _hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let key = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("model:{step}:{index}"), work)
        })?;
        let reference = match self.staged_ref_with_io(operation, &key, io).await {
            Ok(reference) => {
                let (event, _event_hold) =
                    crate::executor::load_model_event(self, &reference, io).await?;
                let index = usize::try_from(index).map_err(|_| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "model event index overflow",
                    )
                })?;
                match event {
                    ModelEvent::ToolCall(ref call) => {
                        self.publish_tool_arguments(operation, step, index, call, io)
                            .await?;
                        reference
                    }
                    _ => {
                        self.stage_model_delta(operation, step, index, &event, io)
                            .await?
                    }
                }
            }
            Err(error) if error.code() == crate::wire::ErrorCode::NotFound => {
                if !matches!(file.key, FileKey::Task { file: TaskFile::Chunk { sequence, .. }, .. }
                    if sequence % MODEL_CHUNK_STRIDE == 0)
                {
                    return Err(canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model chunk lost its provider event",
                    ));
                }
                self.model_delta_reference(&file, io)?
            }
            Err(error) => return Err(error),
        };
        _hold = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        self.append_validated(
            operation,
            None,
            &key,
            &ExecutionEvent::Model {
                step,
                purpose: crate::executor::ModelPurpose::Response,
                event: reference,
            },
            io,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn validate_model_file_position(
        &self,
        operation: OperationId,
        event: &ExecutionEvent,
        summary: &ExecutionSummary,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let ExecutionEvent::Model {
            step,
            purpose,
            event: reference,
        } = event
        else {
            return Ok(());
        };
        if !matches!(
            reference.descriptor().media_type(),
            crate::executor::TEXT_DELTA | crate::executor::REASONING_DELTA
        ) {
            return Ok(());
        }
        let (Some(files), Some(_)) = (&self.task_files, &self.owner) else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "model chunk has no task binding",
            ));
        };
        let key = FileKey::parse(reference.path())?;
        let FileKey::Task {
            task,
            file: TaskFile::Chunk { turn, sequence },
        } = key
        else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model event is not a chunk",
            ));
        };
        let admitted = summary
            .models
            .get(&(*step, *purpose))
            .and_then(Option::as_ref)
            .map(|admission| admission.count as u64);
        if *purpose != crate::executor::ModelPurpose::Response
            || admitted != Some(sequence / MODEL_CHUNK_STRIDE)
            || sequence % MODEL_CHUNK_STRIDE != 0
            || files
                .inner
                .published_turn(&task, Self::model_publication(operation, *step, io)?)
                .await?
                != Some(turn)
        {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "model chunk reordered its admitted stream",
            ));
        }
        Ok(())
    }

    pub(super) async fn load_task_delta(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        if !matches!(
            reference.descriptor().media_type(),
            crate::executor::TEXT_DELTA | crate::executor::REASONING_DELTA
        ) {
            return Ok(None);
        }
        crate::executor::with_history_io(io, |work| reference.validate(work))?;
        let expected_version = blake3::Hash::from_bytes(*reference.descriptor().sha256()).to_hex();
        if reference.version().strip_prefix("task-chunk:") != Some(expected_version.as_str()) {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model chunk version changed",
            ));
        }
        let files = self
            .task_files
            .as_ref()
            .filter(|files| files.volume() == reference.volume())
            .ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "model chunk belongs to another task volume",
                )
            })?;
        crate::executor::with_history_io(io, |work| {
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.scope,
                files.volume(),
                VolumeOperation::Read,
                self.maximum_payload_bytes,
                work,
            )
        })?;
        let key = FileKey::parse(reference.path())?;
        let mut task_id = [0; 36];
        let own_task = if let Some((owner, _)) = &self.owner {
            Some(
                uuid::Uuid::from_bytes(owner.task_id().into_bytes())
                    .hyphenated()
                    .encode_lower(&mut task_id),
            )
        } else {
            None
        };
        if !matches!(&key, FileKey::Task { task, file: TaskFile::Chunk { .. } }
            if Some(task.as_str()) == own_task.as_deref())
        {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model delta does not address a chunk",
            ));
        }
        let file = files
            .inner
            .snapshot(&crate::task_volume::Query::File(key), 1)
            .await?
            .files
            .pop()
            .ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::NotFound,
                    "model chunk is missing",
                )
            })?;
        let media = match file.channel {
            Some(Channel::Text) => crate::executor::TEXT_DELTA,
            Some(Channel::Reasoning) => crate::executor::REASONING_DELTA,
            None => {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "model chunk has no channel",
                ));
            }
        };
        if media != reference.descriptor().media_type() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "model chunk channel changed",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            reference.descriptor().verify(&file.bytes, work)
        })?;
        Ok(Some(file.bytes))
    }

    pub(super) async fn publish_model_file(
        &self,
        write: &Write,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let files = self.task_files.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "model task files are not bound",
            )
        })?;
        let (owner, _) = self.owner.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "model task owner is not bound",
            )
        })?;
        let generation = files.inner.head().await?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let native = None;
        crate::task_volume::publish(
            &files.inner,
            generation,
            write,
            || async {
                let mut history = io.map(PreparationIo::history_io);
                let mut work = history
                    .as_mut()
                    .map(|work| work as &mut dyn crate::store::HistoryIo);
                let condition = owner
                    .condition(crate::distributed::JournalWrite::Settlement, &mut work)
                    .await?;
                Ok::<_, crate::Error>((vec![condition], owner.deadline_epoch_ms()))
            },
            native,
        )
        .await?;
        Ok(())
    }
}
