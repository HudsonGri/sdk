use super::*;
use crate::conversation::VolumeClass;
use acyclic_fs::facade::FsWork as _;

impl CheckoutTools {
    pub(super) fn child_project<'a>(
        &self,
        context: &'a ToolContext,
    ) -> Result<Option<&'a VolumeRef>> {
        let scope = context.effective_scope();
        if scope.grants().contains(&self.read_grant) && scope.grants().contains(&self.write_grant) {
            return Ok(None);
        }
        let task = context.task();
        let owner = task
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("child workspace has no original owner".into()))?;
        let (child, fence) = owner.task_binding();
        if !task.original_task_binding_matches(child, fence) {
            return Err(Error::Unauthorized("child workspace owner differs".into()));
        }
        let access = context
            .task()
            .scope()
            .created_access
            .as_ref()
            .ok_or_else(|| {
                Error::Unauthorized("workspace has no authenticated child allocation".into())
            })?;
        if access.operation.into_bytes() != child.into_bytes() {
            return Err(Error::Unauthorized(
                "child workspace seal belongs to another task".into(),
            ));
        }
        let mut selected = None;
        for binding in &access.content {
            if binding.volume.class() == VolumeClass::Project
                && [VolumeOperation::Read, VolumeOperation::Write]
                    .into_iter()
                    .map(|operation| binding.volume.capability(operation, &mut None))
                    .collect::<Result<Vec<_>>>()?
                    .iter()
                    .all(|grant| scope.grants().contains(grant))
            {
                if selected.replace(&binding.volume).is_some() {
                    return Err(Error::Unauthorized(
                        "child project binding is ambiguous".into(),
                    ));
                }
            }
        }
        selected
            .map(Some)
            .ok_or_else(|| Error::Unauthorized("child project is not writable".into()))
    }

    pub(super) async fn child_fs(
        &self,
        command: &str,
        context: &ToolContext,
        project: &VolumeRef,
    ) -> Result<ToolResult> {
        let mut argv = crate::tools::parse_command(command)?;
        if argv.first().is_some_and(|arg| arg == "git") {
            argv.remove(0);
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let project = self
            .workspace
            .filesystem
            .open_workspace(
                project.storage_name(&mut None)?,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(storage)?;
        let repository =
            acyclic_fs::GitCompatRepository::new(project.id(), self.workspace.store.clone());
        let (selected, _) = repository.checkout_selection().await.map_err(storage)?;
        let workspace = if selected == project.id() {
            project.clone()
        } else {
            acyclic_fs::WorkspaceResolver::resolve(
                &(
                    self.workspace.filesystem.clone(),
                    acyclic_fs::WorkspaceGraph::new(self.workspace.store.clone()),
                ),
                selected,
            )
            .await
            .map_err(storage)?
        };
        let view = self
            .workspace
            .view()
            .open_related(
                workspace,
                &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await
            .map_err(storage)?;
        let result = crate::tools::git::run_with_guard(
            crate::tools::git::Workspace {
                filesystem: &self.workspace.filesystem,
                store: &self.workspace.store,
                view: crate::tools::git::View::Checkout(&view),
                root: &self.workspace.path,
                repository: project.id(),
                lease: None,
                checkout: None,
                author: "LocalHarness",
                authored_at_seconds: i64::try_from(self.clock.now_unix_millis() / 1000)
                    .unwrap_or(i64::MAX),
            },
            &argv,
            || async {
                context
                    .task()
                    .owned_mailbox
                    .as_ref()
                    .ok_or_else(|| Error::Unauthorized("child fs has no owner".into()))?
                    .verify_owner(None)?
                    .await
            },
        )
        .await?;
        let command = result
            .command
            .ok_or_else(|| Error::Conflict("child fs recovered a pending transition".into()))?;
        Ok(ToolResult {
            value: serde_json::json!({"result":command}),
        })
    }
}
