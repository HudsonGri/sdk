//! Both model and direct calls select the admitted task's sparse fork.
use super::*;
use acyclic_fs::{
    CancellationToken, IdempotencyKey, LazyWorkspaceStore as _, WorkBudget,
    kernel::KernelAccounting,
};

impl CheckoutTools {
    pub(super) async fn select(
        &self,
        context: &ToolContext,
    ) -> Result<Option<child_views::ChildLease<Self>>> {
        let (task, _) = context.task().owned_task_binding(None)?;
        let owner = context
            .task()
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("tool task owner missing".into()))?;
        owner.verify_owner(None)?.await?;
        let admission = owner.admission().0;
        let task = if admission.task.name == "acyclic.direct_tool" {
            admission.parent.unwrap_or(task)
        } else {
            task
        };
        if admission.parent.is_none() && task == owner.task_binding().0 {
            return Ok(None);
        }
        self.task_view(context, task, owner).await
    }

    // A task's view is constructed once, under its own gate in this owner's
    // index. Concurrent selectors of one task share it; other tasks and the
    // parent's running command never wait for it.
    async fn task_view(
        &self,
        context: &ToolContext,
        task: TaskId,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
    ) -> Result<Option<child_views::ChildLease<Self>>> {
        let views = &self.workspace.children;
        if let Some(view) = child_views::ChildViews::get(views, task) {
            return Ok(Some(view));
        }
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let ancestors = context.task().ancestors(task)?;
        futures::pin_mut!(ancestors);
        let (mut found, mut root) = (None, task);
        while let Some(candidate) = futures::TryStreamExt::try_next(&mut ancestors).await? {
            root = candidate;
            let id = self
                .workspace
                .filesystem
                .workspace_id(format!("task-{candidate}"))
                .map_err(storage)?;
            if views.contains(candidate)
                || self
                    .workspace
                    .store
                    .load_lazy_workspace(id, &mut accounting)
                    .await
                    .map_err(storage)?
                    .is_some()
            {
                found = Some(candidate);
                break;
            }
        }
        let parent = match found {
            None if root == task => return Ok(None),
            Some(found) if found == task => {
                // A durable fork of this task exists: reopen it.
                return child_views::ChildViews::acquire(
                    views,
                    task,
                    async || {
                        let view = self.fork_task(task, self.workspace.view(), owner).await?;
                        self.mount_task_view(view, owner, None, &self.signed).await
                    },
                    async |tools: &Self| tools.retire_view().await,
                )
                .await
                .map(Some);
            }
            Some(found) => Some(
                Box::pin(self.task_view(context, found, owner))
                    .await?
                    .ok_or_else(|| Error::Conflict("task ancestor has no view".into()))?,
            ),
            None => None,
        };
        // Only this fork pins the parent's selected branch, under that branch
        // owner's guard, through the durable, idempotent fork.
        let create = async || {
            let view = if let Some(parent) = &parent {
                serialized(&parent.state, async |state| {
                    from_selected(
                        state.active.as_ref(),
                        parent.workspace.view(),
                        async |view| self.fork_task(task, view, owner).await,
                    )
                    .await
                })
                .await?
            } else {
                serialized(&self.state, async |state| {
                    from_selected(state.active.as_ref(), self.workspace.view(), async |view| {
                        self.fork_task(task, view, owner).await
                    })
                    .await
                })
                .await?
            };
            self.mount_task_view(view, owner, None, &self.signed).await
        };
        child_views::ChildViews::acquire(views, task, create, async |tools: &Self| {
            tools.retire_view().await
        })
        .await
        .map(Some)
    }

    async fn fork_task(
        &self,
        task: TaskId,
        view: &workspace::View,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
    ) -> Result<workspace::View> {
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        owner.verify_owner(None)?.await?;
        let name = format!("task-{task}");
        // Reopening a task retains its original fork even if the
        // parent's selected branch changed after child admission.
        let id = self
            .workspace
            .filesystem
            .workspace_id(&name)
            .map_err(storage)?;
        let saved = self
            .workspace
            .store
            .load_lazy_workspace(id, &mut accounting)
            .await
            .map_err(storage)?;
        let original = if let Some(saved) = saved {
            let source = saved
                .parent_workspace_id
                .ok_or_else(|| Error::Conflict("task fork has no parent".into()))?;
            if source == view.workspace().id() {
                None
            } else {
                let source = acyclic_fs::WorkspaceResolver::resolve(
                    &(
                        self.workspace.filesystem.clone(),
                        acyclic_fs::WorkspaceGraph::new(self.workspace.store.clone()),
                    ),
                    source,
                )
                .await
                .map_err(storage)?;
                Some(
                    view.open_related(source, &mut accounting)
                        .await
                        .map_err(storage)?,
                )
            }
        } else {
            None
        };
        let view = original.as_ref().unwrap_or(view);
        let (view, _) = view
            .fork(
                &name,
                IdempotencyKey::from_bytes(task.into_bytes()),
                // This lazy fork has no mount-operation lease. The
                // authority still rejects an active reservation.
                PublicationPermit::Unrestricted,
                &mut accounting,
            )
            .await
            .map_err(storage)?;
        owner.verify_owner(None)?.await?;
        Ok(view)
    }

    pub(super) async fn mount_task_view(
        &self,
        view: workspace::View,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
        mounts: Option<Arc<workspace::Mounts>>,
        signed: &Scope,
    ) -> Result<Self> {
        // Mount retirement may outlive the previous owner. Each
        // live mount needs its own namespace so retiring it cannot
        // detach a replacement owner's mount at the same path.
        let (path, shared) = task_view_destination(&self.workspace.mount_directory)?;
        let mut workspace = LocalWorkspace {
            #[cfg(all(test, not(acyclic_harness_loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(acyclic_harness_loom)))]
            mount_advances: std::sync::atomic::AtomicUsize::new(0),
            state: StateArc::new(AsyncMutex::new(State::new(
                view.workspace().id(),
                Arc::clone(&self.workspace.mounts),
            ))),
            children: StateArc::new(Default::default()),
            mounts: mounts.unwrap_or_else(|| Arc::clone(&self.workspace.mounts)),
            context_line: self.workspace.context_line().into(),
            processes: self.workspace.processes.clone(),
            path: path.clone(),
            fresh: false,
            import_source: false,
            mount_directory: self.workspace.mount_directory.clone(),
            view,
            mounted: None,
            // Captured from the mounted root below, before any use.
            identity: acyclic_fs::NativeRootIdentity::from_bytes([0; 16]),
            filesystem: self.workspace.filesystem.clone(),
            store: self.workspace.store.clone(),
            mount_authority: self
                .workspace
                .mount_authority
                .as_ref()
                .map(|authority| authority.for_scope(signed))
                .transpose()?,
        };
        let (active, pending) = workspace.selected_view().await?;
        let selected = active.as_ref().unwrap_or_else(|| workspace.view());
        let initial_view = selected.workspace().id();
        let options = task_view_options(workspace.mount_options(selected.workspace()).await?, shared);
        let mounted = Arc::new(selected.mount(&path, options).await.map_err(storage)?);
        workspace.path = path;
        workspace.identity = acyclic_fs::capture_root_identity(&workspace.path).map_err(storage)?;
        workspace.mounted = Some(mounted);
        workspace.mounts = workspace.mounts_at(&workspace.path).await?;
        workspace.context_line = workspace.mounts.context_line();
        reconciliation::recover_pending(
            &workspace,
            active.as_ref().unwrap_or_else(|| workspace.view()),
            pending,
            &|| async { owner.verify_owner(None)?.await },
        )
        .await?;
        workspace.state = StateArc::new(AsyncMutex::new(State {
            active,
            initial_view,
            workspace: None,
            watcher: None,
            mounts: Arc::clone(&workspace.mounts),
            initialized: true,
        }));
        let workspace = Arc::new(workspace);
        let (capture_options, capture_policy) = capture::configuration(&workspace)?;
        Ok(Self {
            signed: signed.clone(),
            workspace: Arc::clone(&workspace),
            read_grant: self.read_grant.clone(),
            write_grant: self.write_grant.clone(),
            capture_options,
            capture_policy,
            clock: Arc::clone(&self.clock),
            task_files: Arc::clone(&self.task_files),
            state: StateArc::clone(&workspace.state),
            selected: true,
        })
    }
    // The caller owns this checkout's state guard. A sparse mount follows one
    // workspace; switching selects a separate mount instead of staging across
    // the mount's filesystem boundary. Failed mounts can be retried from state.
    pub(super) async fn selected_workspace(
        &self,
        state: &mut State,
        owner: &dyn crate::runtime::OwnedTaskMailbox,
    ) -> Result<Arc<LocalWorkspace>> {
        if self.workspace.mounted.is_none() {
            return Ok(self.workspace.clone());
        }
        let view = state
            .active
            .as_ref()
            .unwrap_or_else(|| self.workspace.view());
        let id = view.workspace().id();
        if id == state.initial_view {
            state.workspace = None;
            state.mounts = Arc::clone(&self.workspace.mounts);
            return Ok(self.workspace.clone());
        }
        if let Some((selected, workspace)) = &state.workspace
            && *selected == id
        {
            return Ok(workspace.clone());
        }
        owner.verify_owner(None)?.await?;
        let (path, shared) = task_view_destination(&self.workspace.mount_directory)?;
        let options = task_view_options(self.workspace.mount_options(view.workspace()).await?, shared);
        let mounted = Arc::new(view.mount(&path, options).await.map_err(storage)?);
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let view = self
            .workspace
            .view()
            .open_related(
                self.workspace.view().workspace().clone(),
                &mut KernelAccounting::NativeOriginal {
                    work: &work,
                    cancellation: &cancellation,
                },
            )
            .await
            .map_err(storage)?;
        let mounts = self.workspace.mounts_at(&path).await?;
        let workspace = Arc::new(LocalWorkspace {
            #[cfg(all(test, not(acyclic_harness_loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(acyclic_harness_loom)))]
            mount_advances: std::sync::atomic::AtomicUsize::new(0),
            context_line: mounts.context_line(),
            processes: self.workspace.processes.clone(),
            mounts: Arc::clone(&mounts),
            // This cached physical handle must not retain its owning cache.
            state: StateArc::new(AsyncMutex::new(State::new(
                view.workspace().id(),
                Arc::clone(&mounts),
            ))),
            children: StateArc::new(Default::default()),
            identity: acyclic_fs::capture_root_identity(&path).map_err(storage)?,
            path,
            fresh: false,
            import_source: false,
            mount_directory: self.workspace.mount_directory.clone(),
            view,
            mounted: Some(mounted),
            filesystem: self.workspace.filesystem.clone(),
            store: self.workspace.store.clone(),
            mount_authority: self.workspace.mount_authority.clone(),
        });
        owner.verify_owner(None)?.await?;
        state.mounts = mounts;
        state.workspace = Some((id, workspace.clone()));
        Ok(workspace)
    }
}

pub(super) async fn from_selected<T, V>(
    active: Option<&V>,
    original: &V,
    create: impl AsyncFnOnce(&V) -> Result<T>,
) -> Result<T> {
    create(active.unwrap_or(original)).await
}

/// A fresh destination for one task view's mount, and whether it is a view
/// of the shared instance. On Linux every task view in the process is a
/// namespace of the one shared native instance, so a view costs no mount,
/// thread or descriptor of its own; elsewhere, and in the scale run's
/// negative control, each view mounts its own instance on a fresh directory.
fn task_view_destination(mount_directory: &std::path::Path) -> Result<(std::path::PathBuf, bool)> {
    let name = crate::identity::fresh_uuid().to_string();
    #[cfg(all(target_os = "linux", test))]
    let shared =
        crate::native_environment::variable(crate::local::children::UNSHARED_TASK_VIEWS).is_none();
    #[cfg(all(target_os = "linux", not(test)))]
    let shared = true;
    #[cfg(target_os = "linux")]
    if shared {
        let root = acyclic_fs::shared_view_root().map_err(storage)?;
        return Ok((root.join(name), true));
    }
    std::fs::create_dir_all(mount_directory).map_err(storage)?;
    let path = mount_directory.join(name);
    std::fs::create_dir(&path).map_err(storage)?;
    Ok((path, false))
}

/// A task view's mount options: the checkout's authority-granted options, as
/// a view of the shared instance when its destination is one.
fn task_view_options(options: acyclic_fs::MountOptions, shared: bool) -> acyclic_fs::MountOptions {
    if shared { options.shared() } else { options }
}
