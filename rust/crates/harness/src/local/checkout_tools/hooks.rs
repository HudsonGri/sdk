use super::*;
use crate::hooks::{HookEvent, HookOutput, Hooks};

impl CheckoutTools {
    pub(super) async fn configured_hooks(
        &self,
        configuration: (&Hooks, [u8; 32]),
        event: HookEvent,
        input: serde_json::Value,
        registry: &ToolRegistry,
        context: ToolContext,
        reconcile_only: bool,
    ) -> Result<HookOutput> {
        let (hooks, digest) = configuration;
        let scope = context.effective_scope();
        if !scope.grants().contains(&self.read_grant) || !scope.grants().contains(&self.write_grant)
        {
            return Err(Error::Unauthorized(
                "hooks require the selected project grants".into(),
            ));
        }
        let owner = context.task().ordinary_journal_owner::<LocalStream>()?;
        if !reconcile_only {
            super::super::context::binding::authorize(
                &self.task_files.inner,
                owner.task_id(),
                &digest,
                scope.limits().file_bytes,
            )
            .await?;
        }
        let operation = context.operation_id();
        crate::hooks::effects::run_once::<_, _, _, _, _, true>(
            &self.task_files.inner,
            (owner.task_id(), operation, event.name()),
            scope.limits().file_bytes,
            !reconcile_only,
            || async {
                let condition = owner
                    .condition(
                        if reconcile_only {
                            crate::distributed::JournalWrite::Settlement
                        } else {
                            crate::distributed::JournalWrite::Fresh
                        },
                        &mut None,
                    )
                    .await?;
                Ok((vec![condition], owner.deadline_epoch_ms()))
            },
            || async {
                let selected = if self.selected {
                    None
                } else {
                    self.select(&context).await?
                };
                let selected = selected.as_deref().unwrap_or(self);
                selected
                    .execute_hooks(hooks, event, input, registry, &context)
                    .await
            },
        )
        .await
    }

    async fn execute_hooks(
        &self,
        hooks: &Hooks,
        event: HookEvent,
        input: serde_json::Value,
        registry: &ToolRegistry,
        context: &ToolContext,
    ) -> Result<HookOutput> {
        serialized(&self.state, async |mut state| {
            let owner = context
                .task()
                .owned_mailbox
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("hook task has no owner".into()))?;
            owner.verify_owner(None)?.await?;
            let workspace = self.selected_workspace(&mut state, owner.as_ref()).await?;
            let view = state
                .active
                .as_ref()
                .unwrap_or_else(|| self.workspace.view());
            if reconciliation::recover(&workspace, view, &|| async {
                owner.verify_owner(None)?.await
            })
            .await?
            {
                state.watcher = None;
            }
            workspace::volumes::sync(&state.mounts).await?;
            {
                let State {
                    active, watcher, ..
                } = &mut *state;
                let view = active.as_ref().unwrap_or_else(|| self.workspace.view());
                self.capture(
                    (&workspace, view),
                    watcher,
                    context.operation_id(),
                    0,
                    PublicationPermit::Unrestricted,
                    Some(Arc::clone(owner)),
                )
                .await?;
            }
            workspace::volumes::project(&state.mounts).await?;
            // Each blocking worker retains this guard through cancellation and
            // child-tree reaping; dropping the caller cannot switch its view.
            let held = StateArc::new(state);
            let check_workspace = Arc::clone(&workspace);
            let check_owner = Arc::clone(owner);
            let runtime = tokio::runtime::Handle::current();
            let check = retain_guard(StateArc::clone(&held), move || {
                if acyclic_fs::capture_root_identity(&check_workspace.path)
                    .map_err(std::io::Error::other)?
                    != check_workspace.identity
                {
                    return Err(std::io::Error::other("hook workspace root was replaced"));
                }
                runtime.block_on(async {
                    tokio::time::timeout(
                        std::time::Duration::from_millis(u64::from(output::CONTROL_TIMEOUT_MS)),
                        check_owner
                            .verify_owner(None)
                            .map_err(std::io::Error::other)?,
                    )
                    .await
                    .map_err(std::io::Error::other)?
                    .map_err(std::io::Error::other)
                })
            });
            let result = hooks
                .run_checked(
                    event,
                    (&workspace.path, &workspace.path),
                    input,
                    registry,
                    check,
                )
                .await;
            let mut state = StateArc::try_unwrap(held)
                .map_err(|_| Error::Conflict("hook cleanup still owns its selected view".into()))?;
            workspace::volumes::sync(&state.mounts).await?;
            let State {
                active, watcher, ..
            } = &mut *state;
            let view = active.as_ref().unwrap_or_else(|| self.workspace.view());
            self.capture(
                (&workspace, view),
                watcher,
                context.operation_id(),
                1,
                PublicationPermit::Unrestricted,
                Some(Arc::clone(owner)),
            )
            .await?;
            owner.verify_owner(None)?.await?;
            result
        })
        .await
    }
}

// Only the owner can recover the guard after every worker has dropped its
// callback. Workers retain ownership without reading or locking the state.
fn retain_guard<T: Send + Sync + 'static>(
    guard: StateArc<OwnedMutexGuard<T>>,
    check: impl Fn() -> std::io::Result<()> + Send + Sync + 'static,
) -> Arc<dyn Fn() -> std::io::Result<()> + Send + Sync> {
    Arc::new(move || {
        let _guard = &guard;
        check()
    })
}

#[cfg(all(test, acyclic_harness_loom))]
mod models;
