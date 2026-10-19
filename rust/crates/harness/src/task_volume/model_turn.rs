//! Shared chronological identity for text and tool projections of one model turn.
use super::{Component, FileKey, FileValue, TaskFile, TaskFiles};
use crate::{OperationId, Result, durable_host::TaskJournalOwner, filesystem::PreparationIo};
use acyclic_stream::StreamProvider;

impl<P: StreamProvider> TaskFiles<P> {
    /// Resolve the full execution identity through the immutable task-file index.
    /// Executor projections follow accepted canonical records; direct tool
    /// contexts use their independently admitted runtime operation identity.
    pub(crate) async fn model_turn(
        &self,
        owner: &TaskJournalOwner<P>,
        operation: OperationId,
        step: u32,
        task: &Component,
        io: Option<&PreparationIo>,
    ) -> Result<u64> {
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
            let (position, head) = self.inner.file_position(&binding.key).await?;
            if let Some(position) = position {
                return Ok(position);
            }
            let mut observer = io.map(PreparationIo::history_io);
            let mut history = observer
                .as_mut()
                .map(|observer| observer as &mut dyn crate::store::HistoryIo);
            let (condition, deadline) = owner
                .file_condition(crate::distributed::JournalWrite::Settlement, &mut history)
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
            match self
                .inner
                .append_if(head, &binding, conditions, deadline, work)
                .await
            {
                Ok(accepted) => return Ok(accepted.position),
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}
