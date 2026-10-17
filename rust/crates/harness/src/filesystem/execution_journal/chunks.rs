//! Streamed response deltas stored once, as live task chunks.
//!
//! A task-owned journal never stages a response text or reasoning delta as a
//! private file. The observation's [`FileRef`] addresses the chunk that the
//! same fenced transaction appends, and the canonical JSON view of the model
//! event is rebuilt from the chunk's channel and bytes on every read.
use super::*;
use crate::task_volume::{Channel, Component, FileKey, TaskFile};

/// Canonical JSON view of a stored delta, serialized exactly as [`ModelEvent`].
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum DeltaView<'a> {
    Content { delta: &'a str },
    Reasoning { delta: &'a str },
}

impl<'a> DeltaView<'a> {
    fn of(model: &'a ModelEvent) -> Option<(Self, Channel)> {
        match model {
            ModelEvent::Content { delta } => Some((Self::Content { delta }, Channel::Text)),
            ModelEvent::Reasoning { delta } => {
                Some((Self::Reasoning { delta }, Channel::Reasoning))
            }
            _ => None,
        }
    }
}

const CHUNK_VERSION: &str = "task-chunk";

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Whether this journal records `model` as its own live chunk.
    pub(super) fn records_chunk(
        &self,
        purpose: crate::executor::ModelPurpose,
        model: &ModelEvent,
    ) -> bool {
        self.task_files.is_some()
            && self.owner.is_some()
            && purpose == crate::executor::ModelPurpose::Response
            && DeltaView::of(model).is_some()
    }

    /// Commits one response delta and its observation in one transaction.
    pub(super) async fn append_delta(
        &self,
        operation: OperationId,
        claim: &str,
        step: u32,
        model: &ModelEvent,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if crate::test_support::accounted::delta_rewrite_enabled() {
            self.rewrite_transcript(operation, claim, step, model, io)
                .await?;
        }
        loop {
            let sequence = self.next_chunk_sequence(operation, step, io).await?;
            let (reference, json) = self.delta_reference(operation, step, sequence, model, io)?;
            let json_hold = io.map(|io| io.guard_bytes(&json)).transpose()?.flatten();
            let event = ExecutionEvent::Model {
                step,
                purpose: crate::executor::ModelPurpose::Response,
                event: reference,
            };
            match self
                .append_validated(operation, None, claim, &event, Some(&json), io)
                .await
            {
                // A peer published first: name the task's next chunk and retry.
                Err(crate::Error::TaskFiles(error))
                    if matches!(*error, crate::task_volume::Error::SequenceConflict(_)) => {}
                outcome => {
                    drop(json_hold);
                    return outcome.map(drop);
                }
            }
        }
    }

    /// The task-wide publication sequence this execution's next chunk takes.
    async fn next_chunk_sequence(
        &self,
        execution: OperationId,
        step: u32,
        io: Option<&PreparationIo>,
    ) -> Result<u64> {
        let (Some(files), Some((owner, _))) = (&self.task_files, &self.owner) else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "journal does not record live response chunks",
            ));
        };
        let task = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?;
        match files
            .inner
            .next_chunk_key(Component::new(task)?, execution, u64::from(step), io)
            .await?
        {
            FileKey::Task {
                file: TaskFile::Chunk { sequence, .. },
                ..
            } => Ok(sequence),
            _ => Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "task volume named no chunk",
            )),
        }
    }

    // Negative control only: re-adds the per-delta whole-transcript private
    // file that this module eliminates.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    async fn rewrite_transcript(
        &self,
        operation: OperationId,
        claim: &str,
        step: u32,
        model: &ModelEvent,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let (
            Some(files),
            Some((owner, _)),
            Some(ModelEvent::Content { delta } | ModelEvent::Reasoning { delta }),
        ) = (&self.task_files, &self.owner, Some(model))
        else {
            return Ok(());
        };
        let query = crate::task_volume::Query::ExecutionChunks {
            task: crate::task_volume::Component::new(owner.task_id().to_string())?,
            turn: u64::from(step),
            execution: operation,
        };
        let mut transcript = Vec::new();
        for file in files.inner.snapshot(&query, 1 << 16).await?.files {
            transcript.extend_from_slice(&file.bytes);
        }
        transcript.extend_from_slice(delta.as_bytes());
        self.stage_with_io(
            operation,
            &format!("{claim}:transcript"),
            &transcript,
            "text/plain",
            io,
        )
        .await
        .map(drop)
    }

    pub(super) fn delta_reference(
        &self,
        execution: OperationId,
        step: u32,
        sequence: u64,
        model: &ModelEvent,
        io: Option<&PreparationIo>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let (Some(files), Some((owner, _)), Some((view, _))) =
            (&self.task_files, &self.owner, DeltaView::of(model))
        else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "journal does not record live response chunks",
            ));
        };
        let json = self.encode_view(&view, io)?;
        let reference = crate::executor::with_history_io(io, |work| {
            let path = crate::executor::preparation_format(
                format_args!(
                    ".tasks/{}/turns/{step}/chunks/{execution}/{sequence}",
                    owner.task_id()
                ),
                work,
            )?;
            let version =
                crate::executor::preparation_format(format_args!("{CHUNK_VERSION}"), work)?;
            let display = crate::executor::preparation_format(format_args!("chunk"), work)?;
            let descriptor =
                crate::conversation::FileDescriptor::from_bytes(&json, "application/json", work)?;
            FileRef::new(
                files.volume().copy(work)?,
                path,
                version,
                descriptor,
                display,
                work,
            )
        })?;
        Ok((reference, json))
    }

    fn encode_view(
        &self,
        view: &DeltaView<'_>,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        match io {
            Some(io) => io.encode(view, self.maximum_payload_bytes, true),
            None => {
                let json = crate::contract::canonical_json_bytes(view)?;
                if json.len() as u64 > self.maximum_payload_bytes {
                    return Err(Error::Invalid(
                        "execution journal payload exceeds limit".into(),
                    ));
                }
                Ok(json.into())
            }
        }
    }

    /// The task chunk addressed by `reference`, when it addresses one.
    fn chunk_key(&self, reference: &FileRef) -> Option<Result<FileKey>> {
        let files = self.task_files.as_ref()?;
        if reference.volume() != files.volume() || !reference.path().starts_with(".tasks/") {
            return None;
        }
        Some(
            FileKey::parse(reference.path())
                .map_err(crate::Error::from)
                .and_then(|key| match &key {
                    FileKey::Task {
                        file: TaskFile::Chunk { .. },
                        ..
                    } if reference.version() == CHUNK_VERSION => Ok(key),
                    _ => Err(Error::Unauthorized(
                        "journal content is not a task chunk".into(),
                    )),
                }),
        )
    }

    /// Reads a committed chunk under the task-volume grant as its JSON view.
    pub(super) async fn load_chunk(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let Some(key) = self.chunk_key(reference) else {
            return Ok(None);
        };
        let key = key.map_err(|_| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "journal content is not a task chunk",
            )
        })?;
        let Some(files) = &self.task_files else {
            return Ok(None);
        };
        let maximum = io
            .map(|io| io.remaining().map(|budget| budget.bytes_encoded))
            .transpose()?
            .unwrap_or(u64::MAX);
        crate::executor::with_history_io(io, |work| {
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.scope,
                files.volume(),
                VolumeOperation::Read,
                maximum,
                work,
            )
        })?;
        let file = files.inner.journal_file(&key, io).await?.ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::NotFound,
                "task chunk was not published",
            )
        })?;
        let delta = std::str::from_utf8(file.bytes.as_ref()).map_err(|_| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "task chunk is not UTF-8",
            )
        })?;
        let view = match file.channel {
            Some(Channel::Text) => DeltaView::Content { delta },
            Some(Channel::Reasoning) => DeltaView::Reasoning { delta },
            _ => {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "task chunk has no delta channel",
                ));
            }
        };
        let json = self.encode_view(&view, io)?;
        crate::executor::with_history_io(io, |work| reference.descriptor().verify(&json, work))?;
        Ok(Some(json))
    }

    /// Prepares the chunk record joined to a delta observation's commit.
    pub(super) async fn prepare_chunk(
        &self,
        execution: OperationId,
        event: &ExecutionEvent,
        model: Option<&ModelEvent>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<crate::task_volume::JournalFile>> {
        let (Some(files), Some((owner, _))) = (&self.task_files, &self.owner) else {
            return Ok(None);
        };
        let ExecutionEvent::Model {
            step,
            purpose,
            event: reference,
        } = event
        else {
            return Ok(None);
        };
        let response = *purpose == crate::executor::ModelPurpose::Response;
        let view = model.and_then(DeltaView::of);
        let key = match self.chunk_key(reference) {
            None if response && view.is_some() => {
                return Err(canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "task response delta must be recorded as its live chunk",
                ));
            }
            None => return Ok(None),
            Some(key) => key,
        };
        let (
            Ok(key),
            true,
            Some((_, channel)),
            Some(ModelEvent::Content { delta } | ModelEvent::Reasoning { delta }),
        ) = (key, response, view, model)
        else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "task chunk reference is not a response delta",
            ));
        };
        let task = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?;
        if !matches!(
            &key,
            FileKey::Task {
                task: chunk_task,
                file: TaskFile::Chunk {
                    turn,
                    execution: chunk_execution,
                    ..
                },
            } if chunk_task.as_str() == task
                && *turn == u64::from(*step)
                && *chunk_execution == execution
        ) {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "task chunk belongs to another execution",
            ));
        }
        if delta.len() as u64 > files.maximum_bytes() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "assistant chunk exceeds task file bound",
            ));
        }
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let copied = if crate::test_support::accounted::chunk_copy_enabled() {
            Some(crate::executor::with_history_io(io, |work| {
                crate::wire_codec::CodecQuota::produce(work, |quota| {
                    crate::core::copy_copy_vector(delta.as_bytes(), quota)
                })
                .and_then(|(bytes, _)| {
                    crate::store::operations::owned_bytes_from_retained_vec(bytes, work)
                        .map(|(bytes, _)| bytes)
                })
            })?)
        } else {
            None
        };
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let payload = copied.as_deref().unwrap_or(delta.as_bytes());
        #[cfg(not(all(test, not(target_arch = "wasm32"))))]
        let payload = delta.as_bytes();
        files
            .inner
            .prepare_named_chunk(key, channel, payload, io)
            .await
    }
}

/// Retry identity of a journal event. A delta's chunk takes the task's next
/// publication sequence when it is appended, so a retried claim is the same
/// delta when its step, purpose, volume and content descriptor match.
pub(super) fn identity_digest(
    event: &ExecutionEvent,
    maximum: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    #[derive(Serialize)]
    struct ChunkIdentity<'a> {
        step: u32,
        purpose: crate::executor::ModelPurpose,
        volume: &'a VolumeRef,
        descriptor: &'a crate::conversation::FileDescriptor,
    }
    match event {
        ExecutionEvent::Model {
            step,
            purpose,
            event: reference,
        } if reference.version() == CHUNK_VERSION => crate::executor::json::canonical_json_digest(
            &ChunkIdentity {
                step: *step,
                purpose: *purpose,
                volume: reference.volume(),
                descriptor: reference.descriptor(),
            },
            maximum,
            work,
        ),
        _ => crate::executor::json::canonical_json_digest(event, maximum, work),
    }
}
