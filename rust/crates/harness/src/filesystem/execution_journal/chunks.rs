//! Chunk projection prepared for the canonical observation's transaction.
use super::*;
use crate::task_volume::{Channel, Component, FileKey, FileValue, TaskFile};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn prepare_chunk(
        &self,
        sequence: u64,
        event: &ExecutionEvent,
        model: Option<&ModelEvent>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<crate::task_volume::JournalFile>> {
        let (Some(files), Some((owner, _))) = (&self.task_files, &self.owner) else {
            return Ok(None);
        };
        let ExecutionEvent::Model {
            step,
            purpose: crate::executor::ModelPurpose::Response,
            ..
        } = event
        else {
            return Ok(None);
        };
        let (channel, delta) = match model {
            Some(ModelEvent::Content { delta }) => (Channel::Text, delta),
            Some(ModelEvent::Reasoning { delta }) => (Channel::Reasoning, delta),
            _ => return Ok(None),
        };
        let bytes = crate::executor::with_history_io(io, |work| {
            crate::wire_codec::CodecQuota::produce(work, |quota| {
                crate::core::copy_copy_vector(delta.as_bytes(), quota)
            })
            .and_then(|(bytes, _)| {
                crate::store::operations::owned_bytes_from_retained_vec(bytes, work)
                    .map(|(bytes, _)| bytes)
            })
        })?;
        let task = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?;
        let value = FileValue {
            publication: None,
            key: FileKey::Task {
                task: Component::new(task)?,
                file: TaskFile::Chunk {
                    turn: u64::from(*step),
                    sequence,
                },
            },
            channel: Some(channel),
            bytes,
        };
        files.inner.prepare_journal_file(&value, io).await
    }
}
