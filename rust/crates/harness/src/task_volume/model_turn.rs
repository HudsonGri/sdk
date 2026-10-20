//! Shared chronological identity for the text and tool projections of one model turn.
use super::{Component, FileKey, FileValue, TaskFile, TaskFiles};
use crate::{OperationId, Result, filesystem::PreparationIo};
use acyclic_stream::StreamProvider;

impl<P: StreamProvider> TaskFiles<P> {
    /// Resolves one complete execution identity (operation and model step) to
    /// its visible turn through the task-file index, never a history scan.
    ///
    /// A task runs several executions (each received child turn, each resumed
    /// prompt), and every one starts at model step zero. The first projection
    /// of an execution step appends an immutable, empty binding; its accepted
    /// position is the turn. Positions are injective and ordered by acceptance,
    /// and a replay or retry resolves the same binding.
    pub(crate) async fn model_turn(
        &self,
        operation: OperationId,
        step: u32,
        task: &Component,
        io: Option<&PreparationIo>,
    ) -> Result<u64> {
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
        let work = {
            let _ = io;
            None
        };
        loop {
            let (position, head) = self.inner.file_position(&binding.key).await?;
            if let Some(position) = position {
                return Ok(position);
            }
            match self
                .inner
                .append_if(head, &binding, Vec::new(), None, work)
                .await
            {
                Ok(accepted) => return Ok(accepted.position),
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}
