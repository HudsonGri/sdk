use super::*;
use crate::task_volume::{Channel, Component, FileKey, TaskFile, Write};

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

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
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
        let task = Component::new(owner.task_id().to_string())?;
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
        let bytes = crate::executor::stock_string_copy(delta, io)?.into_bytes();
        let bytes = crate::executor::with_history_io(io, |work| {
            crate::store::operations::owned_bytes_from_retained_vec(bytes, work)
                .map(|value| value.0)
        })?;
        let mut write = Write::new(
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::Chunk {
                    turn,
                    sequence: sequence as u64,
                },
            },
            bytes,
        );
        write.channel = Some(channel);
        self.publish_model_file(&write, io).await?;
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
        let own_task = self
            .owner
            .as_ref()
            .map(|(owner, _)| owner.task_id().to_string());
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

    async fn publish_model_file(&self, write: &Write, io: Option<&PreparationIo>) -> Result<()> {
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
