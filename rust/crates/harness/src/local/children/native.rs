//! Native attachments retain their identity through the stock child preparation.
use super::*;
use crate::{
    conversation::FileRef,
    external_executor::ChildAdmission,
    resource_preparation::ResourcePreparationRequest,
    runtime::{TaskAdmissionRecord, TaskContext},
    task_volume::Component,
};
use acyclic_stream::BoxProviderFuture;

impl ChildAdmission for LocalHarness {
    fn cancel_preparation<'a>(
        &'a self,
        parent: TaskId,
        fence: &'a LeaseFence,
        operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let host = self.runtime.task_host();
            host.journal_owner(parent, fence, None)
                .await?
                .verify(true, &mut None)
                .await?;
            let preparation = admission::child_operation(
                self.task_files.volume(),
                &operation.to_string(),
                b"preparation",
            )?;
            for phase in [preparation, operation] {
                let Some((task, retained)) = host.reconcile_admission(phase).await? else {
                    continue;
                };
                if retained.parent != Some(parent) {
                    return Err(Error::Unauthorized(
                        "cancelled preparation parent changed".into(),
                    ));
                }
                let state = host
                    .cancel_child_owned(parent, fence, task, phase, &mut None)
                    .await?;
                if state.reservation.is_some() {
                    // The executing worker must retire its own lease. Never
                    // adopt an observed reservation to fabricate settlement.
                    return Err(Error::Indeterminate(phase));
                }
            }
            Ok(())
        })
    }

    fn native_workspace(&self, task: TaskId) -> Result<PathBuf> {
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("native child has no workspace".into()))?;
        Ok(crate::external_executor::launch::process_directory(
            &workspace.mount_directory,
            task,
        ))
    }
    fn admit<'a>(
        &'a self,
        context: &'a TaskContext,
        fence: &'a LeaseFence,
        reference: &'a Component,
        child: TaskAdmissionRecord,
        inputs: Vec<FileRef>,
        reconcile: bool,
    ) -> BoxProviderFuture<'a, Result<TaskId>> {
        Box::pin(async move {
            if !Arc::ptr_eq(&context.harness, self.runtime.harness()) {
                return Err(Error::Unauthorized(
                    "native child uses another runtime".into(),
                ));
            }
            let parent = context.durable_task_id().ok_or_else(|| {
                Error::Unauthorized("native child parent has no admitted task".into())
            })?;
            self.runtime
                .task_host()
                .journal_owner(parent, fence, None)
                .await?
                .verify(reconcile, &mut None)
                .await?;
            let task = self
                .admit_child(
                    &self.task_files,
                    reference,
                    parent,
                    admission::ChildInput::Native {
                        context,
                        fence,
                        child,
                        inputs,
                        reconcile,
                    },
                )
                .await?;
            if let Some(cancellation) = context
                .owned_mailbox
                .as_ref()
                .and_then(|owner| owner.invocation_cancellation())
            {
                // Retained admissions take an early return too. Reconciliation
                // must never turn a cancelled, partly admitted launch into a
                // successful spawn when publication is retried.
                cancellation.check()?;
            }
            Ok(task)
        })
    }
}

pub(super) fn validate_template(
    context: &TaskContext,
    child: &TaskAdmissionRecord,
) -> Result<RuntimeScope> {
    child.validate(&mut None)?;
    if context.durable_task_id().is_none()
        || child.parent != context.durable_task_id()
        || !child.resource_proofs.is_empty()
        || !context.scope().run_limits().allows(&child.run_limits)
    {
        return Err(Error::Unauthorized(
            "native child template differs from its parent".into(),
        ));
    }
    context.scope().narrow(child.grants.clone(), child.limits)
}

pub(super) fn verify_retained(
    context: &TaskContext,
    template: &TaskAdmissionRecord,
    inputs: &[FileRef],
    admitted: &TaskAdmissionRecord,
    prepared: &ResourcePreparationRequest,
    volumes: [&VolumeRef; 4],
) -> Result<()> {
    let narrowed = validate_template(context, template)?;
    let mut expected = template.clone();
    expected.grants = admission::child_grants(context, &narrowed, volumes, inputs)?;
    expected.resource_proofs = prepared.child.resource_proofs.clone();
    if &prepared.child != admitted || prepared.child != expected || prepared.input_files != inputs {
        return Err(Error::Conflict("native child preparation changed".into()));
    }
    Ok(())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
