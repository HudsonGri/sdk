//! Workspace coordination for hosts that own their agents and tool execution.
//!
//! This facade opens no model provider, executor, service or process. Task
//! contexts come from the caller's existing shared harness task runtime.
use super::*;
use crate::runtime::{OwnedTaskMailbox, TaskContext};
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{HeldRoot, LazyWorkspaceStore as _, LocalCoreStateStore};

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

/// Reserved mount namespace within an already writable checkout.
pub const HOST_VIEW_ROOT: &str = ".acyclic-sdk/host-views";

/// Physical bindings selected by the trusted native host.
pub struct HostWorkspaceAttachment {
    /// Held checkout root, shared with the host's source-identity validation.
    pub root: HeldRoot,
    /// Existing writable root admitted by the host's sandbox. Passing a root
    /// here does not grant sandbox access; the host must admit it first.
    pub admitted_mount_root: HeldRoot,
    /// Shared harness control directory, separate from the checkout.
    pub state_directory: PathBuf,
    /// Existing authenticated project volume.
    pub project: VolumeRef,
    /// Existing authenticated scratch volume.
    pub scratch: VolumeRef,
}

/// An admitted task's workspace, with no execution authority of its own.
pub struct HostWorkspace {
    workspace: Arc<LocalWorkspace>,
    task: TaskId,
    mount_root: Arc<HeldRoot>,
    project: VolumeRef,
}

impl HostWorkspace {
    /// Attaches a root task without constructing an agent composition.
    pub async fn attach(
        filesystem: &acyclic_fs::LocalFs,
        store: LocalCoreStateStore,
        context: &TaskContext,
        attachment: HostWorkspaceAttachment,
    ) -> Result<Self> {
        let owner = original_owner(context)?;
        owner.verify_owner(None)?.await?;
        if owner.admission().0.parent.is_some() {
            return Err(Error::Unauthorized("host root task has a parent".into()));
        }
        if attachment.project.class() != VolumeClass::Project
            || attachment.scratch.class() != VolumeClass::AgentPrivate
        {
            return Err(Error::Invalid(
                "host workspace volume classes differ".into(),
            ));
        }
        for volume in [&attachment.project, &attachment.scratch] {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                if !context
                    .scope()
                    .grants()
                    .contains(&volume.capability(operation, &mut None)?)
                {
                    return Err(Error::Unauthorized(
                        "host workspace volume is not granted".into(),
                    ));
                }
            }
        }
        let root = attachment.root.path();
        let canonical_root = std::fs::canonicalize(root).map_err(storage)?;
        let mounts =
            std::fs::canonicalize(attachment.admitted_mount_root.path()).map_err(storage)?;
        if mounts.starts_with(&canonical_root) && mounts != canonical_root.join(HOST_VIEW_ROOT) {
            return Err(Error::Invalid(
                "in-checkout host mounts require the reserved host-view root".into(),
            ));
        }
        verify_root(&attachment.admitted_mount_root)?;
        let directory = owner::separated_directory(&attachment.state_directory, Some(root))?;
        workspace::volumes::require_available(acyclic_fs::probe_native_mount())?;
        acyclic_fs::initialize_native_mount_services().map_err(storage)?;
        let attach = workspace::Attach {
            volume: &attachment.project,
            scratch: &attachment.scratch,
            directory: &directory,
            operation: workspace::attach_operation(owner.task_binding().0),
            native_mounts: true,
        };
        let mut workspace = workspace::open(
            filesystem,
            &attach,
            attachment.root,
            true,
            workspace::OwnerState {
                store,
                processes: None,
            },
        )
        .await?;
        workspace.mount_directory = attachment.admitted_mount_root.path().to_path_buf();
        owner.verify_owner(None)?.await?;
        Ok(Self {
            workspace: Arc::new(workspace),
            task: owner.task_binding().0,
            mount_root: Arc::new(attachment.admitted_mount_root),
            project: attachment.project,
        })
    }

    /// Cwd to give the native host before it starts executing this task.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.workspace.path
    }

    /// Shared task identity; no separate plugin task index is created.
    #[must_use]
    pub const fn task(&self) -> TaskId {
        self.task
    }

    /// Records completed native-host writes in the selected shared view.
    /// The caller must first quiesce native writes for this task. This method
    /// neither executes a tool nor grants access to another task's files.
    pub async fn capture(&self, context: &TaskContext, operation: OperationId) -> Result<()> {
        use super::checkout_tools::{capture, reconciliation};
        let owner = self.verify(context).await?;
        verify_root(&self.mount_root)?;
        let mut state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let (selected, pending) = self.workspace.selected_view().await?;
        let view = selected.as_ref().unwrap_or_else(|| self.workspace.view());
        let verify = || async { owner.verify_owner(None)?.await };
        reconciliation::recover_pending(&self.workspace, view, pending, &verify).await?;
        state.active = selected;
        let physical = self
            .workspace
            .selected_workspace(&mut state, owner.as_ref())
            .await?;
        if let Some(mount) = &physical.mounted {
            mount
                .sync_with_permit(
                    acyclic_fs::PublicationPermit::Unrestricted,
                    &mount.native_work()?,
                    |_| async { owner.verify_owner(None)?.await },
                )
                .await
                .map_err(|failure| failure.cause)?;
        } else {
            let configuration = capture::configuration(&physical)?;
            let super::checkout_tools::State {
                active, watcher, ..
            } = &mut *state;
            let selected = active.as_ref().unwrap_or_else(|| self.workspace.view());
            capture::finish_owed_write(
                (&configuration.0, &configuration.1),
                (&physical, selected),
                watcher,
                acyclic_fs::PublicationPermit::Unrestricted,
                &verify,
            )
            .await?;
            capture::record_changes(
                (&configuration.0, &configuration.1),
                (&physical, selected),
                watcher,
                (operation, 1),
                acyclic_fs::PublicationPermit::Unrestricted,
                &verify,
            )
            .await?;
        }
        self.verify(context).await?;
        verify_root(&self.mount_root)
    }

    /// Idempotently forks an authenticated direct child from the selected view.
    /// Capture the parent's host-authored work before calling this boundary.
    pub async fn fork(&self, parent: &TaskContext, child: &TaskContext) -> Result<Self> {
        self.open_child(parent, child, false).await
    }

    /// Reopens an existing fork; a missing fork is never silently created.
    pub async fn reopen(&self, parent: &TaskContext, child: &TaskContext) -> Result<Self> {
        self.open_child(parent, child, true).await
    }

    async fn open_child(
        &self,
        parent: &TaskContext,
        child: &TaskContext,
        existing: bool,
    ) -> Result<Self> {
        let parent_owner = self.verify(parent).await?;
        let child_owner = original_owner(child)?;
        child_owner.verify_owner(None)?.await?;
        let task = child_owner.task_binding().0;
        if child_owner.admission().0.parent != Some(self.task) {
            return Err(Error::Unauthorized(
                "host child belongs to another parent".into(),
            ));
        }
        verify_root(&self.mount_root)?;
        let _state =
            crate::sync::owned::lock_owned(workspace::StateArc::clone(&self.workspace.state)).await;
        let allocated = child.scope().created_access.is_some();
        let project = if allocated {
            workspace::created_project(child, child.scope())?
        } else {
            &self.project
        };
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            if !child
                .scope()
                .grants()
                .contains(&project.capability(operation, &mut None)?)
            {
                return Err(Error::Unauthorized(
                    "host child project is not granted".into(),
                ));
            }
        }
        if existing {
            let cancellation = acyclic_fs::CancellationToken::new();
            let name = if allocated {
                project.storage_name(&mut None)?
            } else {
                format!("task-{task}")
            };
            let id = self
                .workspace
                .filesystem
                .workspace_id(&name)
                .map_err(storage)?;
            if self
                .workspace
                .store
                .load_lazy_workspace(
                    id,
                    &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
                )
                .await
                .map_err(storage)?
                .is_none()
            {
                return Err(Error::NotFound(
                    "host child workspace has no retained fork".into(),
                ));
            }
        }
        let (selected, pending) = self.workspace.selected_view().await?;
        let selected = selected.as_ref().unwrap_or_else(|| self.workspace.view());
        super::checkout_tools::reconciliation::recover_pending(
            &self.workspace,
            selected,
            pending,
            &|| async { parent_owner.verify_owner(None)?.await },
        )
        .await?;
        let (view, mounts) = if allocated {
            let view = self.workspace.project_view(project).await?;
            let mounts = self
                .workspace
                .created_mounts(project, child, child.scope())
                .await?;
            (view, Some(mounts))
        } else {
            (
                self.workspace
                    .fork_task(task, selected, child_owner)
                    .await?,
                None,
            )
        };
        let workspace = self
            .workspace
            .mount_task_view(view, child_owner, mounts, self.mount_root.path())
            .await?;
        parent_owner.verify_owner(None)?.await?;
        child_owner.verify_owner(None)?.await?;
        verify_root(&self.mount_root)?;
        Ok(Self {
            workspace,
            task,
            mount_root: Arc::clone(&self.mount_root),
            project: project.clone(),
        })
    }

    async fn verify<'a>(&self, context: &'a TaskContext) -> Result<&'a Arc<dyn OwnedTaskMailbox>> {
        let owner = original_owner(context)?;
        if owner.task_binding().0 != self.task {
            return Err(Error::Unauthorized(
                "host workspace belongs to another task".into(),
            ));
        }
        owner.verify_owner(None)?.await?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            if !context
                .scope()
                .grants()
                .contains(&self.project.capability(operation, &mut None)?)
            {
                return Err(Error::Unauthorized(
                    "host workspace project is not granted".into(),
                ));
            }
        }
        if acyclic_fs::capture_root_identity(&self.workspace.path).map_err(storage)?
            != self.workspace.identity
        {
            return Err(Error::Unauthorized(
                "host workspace root was replaced".into(),
            ));
        }
        Ok(owner)
    }
}

fn original_owner(context: &TaskContext) -> Result<&Arc<dyn OwnedTaskMailbox>> {
    context.owned_task_binding(None)?;
    context
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("host task owner is absent".into()))
}

fn verify_root(root: &HeldRoot) -> Result<()> {
    if acyclic_fs::capture_root_identity(root.path()).map_err(storage)? != root.identity() {
        return Err(Error::Unauthorized(
            "host-admitted mount root was replaced".into(),
        ));
    }
    Ok(())
}
