//! Disposable chunk files published only after their canonical journal append.
use super::*;
use crate::task_volume::{Channel, Component, FileKey, FileValue, TaskFile};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn append_projected(
        &self,
        operation: OperationId,
        expected_tail: u64,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let accepted = self
            .append_validated(operation, Some(expected_tail), key, event, io)
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
        let turn = self.model_turn(operation, *step, &task, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
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
        loop {
            let head = files.inner.head().await?;
            let mut observer = io.map(PreparationIo::history_io);
            let mut history = observer
                .as_mut()
                .map(|observer| observer as &mut dyn crate::store::HistoryIo);
            let condition = owner
                .condition(crate::distributed::JournalWrite::Fresh, &mut history)
                .await?;
            let _conditions_hold;
            let mut conditions = Vec::new();
            PreparationIo::reserve(&mut conditions, 1, io)?;
            _conditions_hold = io
                .map(|io| {
                    io.guard_owned(
                        (conditions.capacity()
                            * std::mem::size_of::<acyclic_stream::CommitCondition>())
                            as u64,
                    )
                })
                .transpose()?;
            conditions.push(condition);
            match files
                .inner
                .append_if(head, &value, conditions, owner.deadline_epoch_ms(), work)
                .await
            {
                Ok(_) => return Ok(()),
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Resolves a full canonical model identity by indexed lookup, never history scan.
    /// The empty immutable binding's accepted position is its chronological turn.
    pub(super) async fn model_turn(
        &self,
        operation: OperationId,
        step: u32,
        task: &Component,
        io: Option<&PreparationIo>,
    ) -> Result<u64> {
        let (Some(files), Some((owner, _))) = (&self.task_files, &self.owner) else {
            return Err(Error::Invalid("model projection has no task owner".into()));
        };
        // Allocate an immutable full-operation binding before exposing any chunk.
        // Its accepted task-file position supplies chronological identity even
        // for caller-chosen UUIDs and repeated child executions at model step 0.
        let binding = FileValue::new(
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::ModelTurn {
                    operation: operation.into_bytes(),
                    step,
                },
            },
            acyclic_stream::OwnedBytes::default(),
        );
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
        loop {
            let (position, head) = files.inner.file_position(&binding.key).await?;
            if let Some(position) = position {
                return Ok(position);
            }
            let mut observer = io.map(PreparationIo::history_io);
            let mut history = observer
                .as_mut()
                .map(|observer| observer as &mut dyn crate::store::HistoryIo);
            let condition = owner
                .condition(crate::distributed::JournalWrite::Fresh, &mut history)
                .await?;
            let _conditions_hold;
            let mut conditions = Vec::new();
            PreparationIo::reserve(&mut conditions, 1, io)?;
            _conditions_hold = io
                .map(|io| {
                    io.guard_owned(
                        (conditions.capacity()
                            * std::mem::size_of::<acyclic_stream::CommitCondition>())
                            as u64,
                    )
                })
                .transpose()?;
            conditions.push(condition);
            match files
                .inner
                .append_if(head, &binding, conditions, owner.deadline_epoch_ms(), work)
                .await
            {
                Ok(accepted) => return Ok(accepted.position),
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}
