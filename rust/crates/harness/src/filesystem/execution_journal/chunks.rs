//! Disposable chunk files published only after their canonical journal append.
use super::*;
use crate::task_volume::{Channel, Component, FileKey, FileValue, TaskFile};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// The single canonical append: every accepted append, metered or
    /// unmetered, conditional or not, projects its visible response chunk.
    /// A refused compare-and-append projects nothing.
    pub(super) async fn append_projected(
        &self,
        operation: OperationId,
        expected_tail: Option<u64>,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let accepted = self
            .append_validated(operation, expected_tail, key, event, io)
            .await?;
        if accepted {
            self.publish_chunk(operation, key, event, io).await?;
        }
        Ok(accepted)
    }

    pub(super) async fn publish_chunk(
        &self,
        operation: OperationId,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let (Some(files), Some((owner, _))) = (&self.task_files, &self.owner) else {
            return Ok(());
        };
        let ExecutionEvent::Model {
            step,
            purpose: crate::executor::ModelPurpose::Response,
            ..
        } = event
        else {
            return Ok(());
        };
        let Some(model) = self.verify_event_refs_with_io(event, io).await? else {
            return Ok(());
        };
        let (channel, delta) = match model {
            ModelEvent::Content { delta } => (Channel::Text, delta),
            ModelEvent::Reasoning { delta } => (Channel::Reasoning, delta),
            _ => return Ok(()),
        };
        let sequence = key
            .rsplit(':')
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(|| Error::Invalid("model event has no canonical ordinal".into()))?;
        let task = Component::new(owner.task_id().to_string())?;
        let turn = files.model_turn(operation, *step, &task, io).await?;
        let value = FileValue {
            requester: None,
            publication: None,
            key: FileKey::Task {
                task,
                file: TaskFile::Chunk { turn, sequence },
            },
            channel: Some(channel),
            bytes: delta.into_bytes().into(),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
        loop {
            let head = files.inner.head().await?;
            match files
                .inner
                .append_if(head, &value, Vec::new(), None, work)
                .await
            {
                Ok(_) => return Ok(()),
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}
