//! Registered children use the same worker, task gate and provider follows.
use super::*;
use crate::runtime::TaskStateProvider as _;
use futures::StreamExt as _;

impl LocalHarness {
    pub(crate) async fn external_child(
        &self,
        request: &ExternalRequest,
        result: &Value,
    ) -> Result<()> {
        if !matches!(&request.action, ExternalAction::RunTool {name, ..} if name == "acyclic_spawn")
        {
            return Ok(());
        }
        let Some(value) = result.get("Succeeded") else {
            return Ok(());
        };
        let child = TaskId::parse(
            value
                .as_str()
                .ok_or_else(|| Error::Invalid("spawn result is not a task ID".into()))?,
        )?;
        let admission = self.runtime.task_host().observe_admission(child).await?;
        if admission.parent != Some(request.task_id()) {
            return Err(Error::Unauthorized(
                "spawn result is not this task's child".into(),
            ));
        }
        // Opening the child's provider-backed task subtree is idempotent. The
        // owner's existing children follow discovers it, including after reopen.
        // No parent task gate is held while acquiring this child's binding.
        self.external_volumes(child).await?;
        Ok(())
    }

    pub(crate) fn registered_until<'a>(
        &'a self,
        task: TaskId,
        stopped: impl Future<Output = Result<()>> + Send + 'a,
    ) -> impl Future<Output = Result<bool>> + 'a {
        Box::pin(async move {
            let admission = match self.runtime.task_host().observe_admission(task).await {
                Ok(admission) => admission,
                // A native client's first request stream precedes admission.
                Err(Error::NotFound(_)) => return Ok(false),
                Err(error) => return Err(error),
            };
            if admission.task.name == "acyclic.command_session"
                && admission
                    .input
                    .pointer("/command/kind")
                    .and_then(Value::as_str)
                    == Some(crate::external_executor::LIFECYCLE)
            {
                return Ok(false);
            }
            drop(admission);
            crate::external_executor::progress::follow(
                None,
                |revision| self.registered_step(task, revision),
                async {
                    Ok(self
                        .runtime
                        .task_host()
                        .workflow_changes()
                        .await?
                        .map(|record| record.map(|_| ()).map_err(storage)))
                },
                stopped,
            )
            .await?;
            Ok(true)
        })
    }

    fn registered_step(
        &self,
        task: TaskId,
        previous: Option<u64>,
    ) -> impl Future<Output = Result<(Option<u64>, Option<()>)>> + '_ {
        Box::pin(async move {
            let (revision, terminal) = {
                let (_binding, _) = self.binding(task).await?;
                let revision = self.runtime.task_host().workflow_revision(task).await?;
                if previous == Some(revision) {
                    return Ok((previous, None));
                }
                let (cursor, terminal) =
                    crate::external_executor::progress::at_revision(previous, revision, || async {
                        if self.model.is_some() {
                            let commands = self.commands()?;
                            self.advance(task, &commands).await?;
                        } else {
                            self.advance(task, &self.runtime.commands()).await?;
                        }
                        self.runtime.task_host().outcome(task).await
                    })
                    .await?;
                (cursor, terminal)
            };
            // A registered child can admit its own children. Discover these
            // from the durable hierarchy after releasing its task gate.
            let mut after = None;
            loop {
                let children = self
                    .runtime
                    .harness()
                    .children(task, None, after, 64)
                    .await?;
                for child in children.entries {
                    self.external_volumes(child.task_id).await?;
                }
                after = children.next_after;
                if after.is_none() {
                    break;
                }
            }
            Ok((
                revision,
                match terminal {
                    Some(
                        crate::Outcome::Succeeded(_)
                        | crate::Outcome::Failed { .. }
                        | crate::Outcome::Cancelled,
                    ) => Some(()),
                    _ => None,
                },
            ))
        })
    }
}

#[cfg(all(test, feature = "test-support", not(acyclic_harness_loom)))]
mod tests;
