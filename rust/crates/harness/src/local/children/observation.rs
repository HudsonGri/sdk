//! Read-only lookup shares admission's identity and ownership check.
use super::*;
use crate::task_volume::{ChildObservation, ChildTurnResult, Component, TaskFiles};
use acyclic_stream::StreamProvider;

impl LocalHarness {
    pub(super) async fn admitted_child<P: StreamProvider + ?Sized>(
        &self,
        files: &TaskFiles<P>,
        reference: &Component,
        parent: TaskId,
    ) -> Result<Option<TaskId>> {
        let operation = admission::child_operation(files.volume(), reference.as_str(), b"child")?;
        let task = TaskId::from_bytes(operation.into_bytes());
        let admitted = match crate::runtime::observe_task_admission(
            self.runtime.task_host().as_ref(),
            task,
            None,
        )
        .await
        {
            Ok(record) => record,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if admitted.task_id() != task || !crate::filesystem::child_agent::is_child(&admitted) {
            return Err(Error::Conflict(
                "child request task definition changed".into(),
            ));
        }
        if admitted.parent != Some(parent) {
            return Err(Error::Conflict("child parent changed".into()));
        }
        Ok(Some(task))
    }

    /// Reads one already admitted direct child of the authenticated parent.
    /// This cannot prepare resources, create a fork, publish branch metadata or
    /// admit an unknown child. `None` is bounded replay progress, never absence.
    pub async fn observe_admitted_child(
        &self,
        reference: &Component,
        parent: TaskId,
        after: u64,
        maximum: usize,
    ) -> Result<Option<(TaskId, ChildObservation)>> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "child observation requires a result bound".into(),
            ));
        }
        let task = self
            .admitted_child(self.task_files.as_ref(), reference, parent)
            .await?
            .ok_or_else(|| Error::NotFound("child is not admitted".into()))?;
        let Some(observation) = self
            .task_files
            .inner
            .observe_child(reference, after, maximum)
            .await
            .map_err(storage)?
        else {
            return Ok(None);
        };
        if observation.status.is_some_and(|status| status.task != task) {
            return Err(Error::Conflict(
                "child status has a foreign admission".into(),
            ));
        }
        for file in &observation.results {
            let result = ChildTurnResult::parse(&file.bytes).map_err(storage)?;
            if result.task != task {
                return Err(Error::Conflict(
                    "child result has a foreign admission".into(),
                ));
            }
        }
        Ok(Some((task, observation)))
    }
}
