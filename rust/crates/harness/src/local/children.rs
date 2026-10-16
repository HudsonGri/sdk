use super::*;
use crate::{executor::TurnOutput, runtime::TaskSpawner, scheduler::LeaseFence};

#[cfg(feature = "native-execution")]
mod admission;
#[cfg(feature = "native-execution")]
mod commands;
#[cfg(feature = "native-execution")]
mod controller;
#[cfg(feature = "native-execution")]
mod lazy;
#[cfg(feature = "native-execution")]
mod preparation;

impl LocalHarness {
    pub(super) async fn submit_child_turn(
        &self,
        operation: OperationId,
        prompt: &str,
        parent: (TaskId, &RuntimeScope, &LeaseFence),
    ) -> Result<Admission<RuntimeTask<TurnOutput>>> {
        let (parent, scope, fence) = parent;
        let limits = scope.limits();
        if prompt.is_empty() || prompt.len() as u64 > limits.file_bytes {
            return Err(Error::Invalid(
                "local prompt is empty or exceeds the file limit".into(),
            ));
        }
        let (receive, model) = self
            .stage_turn_with_limits(operation, prompt, limits)
            .await?;
        let definition = self
            .runtime
            .harness()
            .task::<serde_json::Value, TurnOutput>("acyclic.stock_turn@1")?;
        let record = self
            .runtime
            .harness()
            .registered_admission(
                operation,
                &definition.identity().name,
                &definition.identity().version,
                serde_json::json!({"receive":receive,"model":model}),
                Some((parent, scope)),
                None,
                None,
            )?
            .0;
        match self
            .runtime
            .task_host()
            .admit_owned(record, fence.clone())
            .await?
        {
            Admission::Accepted(task) => Ok(Admission::Accepted(
                self.runtime.harness().attach(task, &definition).await?,
            )),
            Admission::Rejected { reason } => Ok(Admission::Rejected { reason }),
            Admission::Indeterminate { operation_id } => {
                Ok(Admission::Indeterminate { operation_id })
            }
        }
    }
}
