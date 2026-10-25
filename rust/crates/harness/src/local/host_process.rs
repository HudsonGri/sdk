//! Physical custody shared by native host adapters and their workspace owner.
//!
//! The lock precedes claiming a turn and outlives its process tree. Settlement
//! takes that same lock and fences orphaned trees before capturing any files.
//! There is no adapter journal or caller-supplied quiescence assertion.
use super::*;
use acyclic_native_runtime::{FileLock, FileLockMode, ProcessCustody, ProcessTreeStorage};

/// Runs the interactive native root with its inherited console and process
/// group. Native hosts retain terminal/job control; managed children use the
/// separate, custodied session API below. Turn-end hooks capture root turns.
pub fn run_root_session(
    cwd: &Path,
    host: &str,
    program: &std::ffi::OsStr,
    arguments: &[String],
) -> Result<std::process::ExitStatus> {
    use super::host_owner::HostAction;
    let location = super::host_files::request(
        cwd,
        HostAction::Open {
            host: host.into(),
            session: OperationId::new().to_string(),
        },
    )?;
    let task: TaskId = serde_json::from_value(
        location
            .get("task")
            .cloned()
            .ok_or_else(|| Error::Invalid("native root task absent".into()))?,
    )
    .map_err(storage)?;
    let status = std::process::Command::new(program)
        .args(arguments)
        .current_dir(cwd)
        .env(
            "ACYCLIC_HOST_ARGS",
            serde_json::to_string(arguments).map_err(storage)?,
        )
        .env("ACYCLIC_WORKSPACE", cwd)
        .env("ACYCLIC_TASK", task.to_string())
        .status()
        .map_err(storage)?;
    super::host_files::request(cwd, HostAction::Capture { task })?;
    Ok(status)
}

/// Exclusive right to start one task's native session processes.
pub struct HostProcessLease {
    task: TaskId,
    view: PathBuf,
    custody: Arc<ProcessCustody>,
    _ownership: FileLock,
    _root: acyclic_fs::HeldRoot,
}

/// Physical proof held until capture and result publication both finish.
///
/// Its fields are private and it has no serialization or cloning operation.
pub struct QuiescentHostProcess(HostProcessLease);

impl HostProcessLease {
    /// Claims exactly one child input under its physical execution lease.
    /// The task and cwd come from the live harness; adapters cannot construct
    /// an execution lease from caller-supplied task/view strings.
    pub fn claim_child(
        cwd: &Path,
        parent: TaskId,
        reference: &str,
    ) -> Result<Option<(Self, serde_json::Value)>> {
        use super::host_owner::{HostAction, SessionAction};
        let request = |action| {
            super::host_files::request(
                cwd,
                HostAction::Session {
                    parent,
                    reference: reference.into(),
                    action,
                },
            )
        };
        let observed = request(SessionAction::Observe)?;
        let task = serde_json::from_value(
            observed
                .get("task")
                .cloned()
                .ok_or_else(|| Error::Invalid("native child task absent".into()))?,
        )
        .map_err(storage)?;
        let view = observed
            .get("cwd")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Invalid("native child view absent".into()))?;
        let lease = Self::acquire(
            &super::host_files::workspace_root(cwd)?,
            task,
            Path::new(view),
        )?;
        let turn = request(SessionAction::Next)?;
        if turn.is_null() {
            return Ok(None);
        }
        if turn.get("task") != observed.get("task") || turn.get("cwd") != observed.get("cwd") {
            return Err(Error::Conflict(
                "step=claim native process: admitted task or view changed; recovery: acyclic agents".into(),
            ));
        }
        Ok(Some((lease, turn)))
    }

    /// Acquires the task's workspace-local custody before claiming an input.
    /// A live owner refuses acquisition; a dead owner's trees are fenced using
    /// the runtime's existing cross-platform process containment implementation.
    pub(super) fn acquire(workspace: &Path, task: TaskId, view: &Path) -> Result<Self> {
        let workspace = std::fs::canonicalize(workspace).map_err(storage)?;
        let view = std::fs::canonicalize(view).map_err(storage)?;
        if !view.starts_with(&workspace) {
            return Err(Error::Unauthorized(
                "step=admit native process view: outside workspace; recovery: reopen the task through acyclic host".into(),
            ));
        }
        let root = acyclic_fs::hold_root(&workspace)
            .map_err(storage)?
            .create_directories(
                &PathBuf::from(".acyclic-sdk/host-processes").join(task.to_string()),
            )
            .map_err(storage)?;
        let ownership = FileLock::try_acquire(
            root.open_file(Path::new("owner.lock"), true).map_err(storage)?,
            FileLockMode::Exclusive,
        )
        .map_err(|error| Error::Conflict(format!(
            "step=acquire native process custody for {task}: {error}; recovery: acyclic --task {task} agents"
        )))?;
        // The exclusive lock is retained throughout the fence and all later
        // execution. Another process cannot start while settlement captures.
        let custody = ProcessCustody::acquire(root.path()).map_err(storage)?;
        Ok(Self {
            task,
            view,
            custody,
            _ownership: ownership,
            _root: root,
        })
    }

    /// Funds the process slot on the supplied original invocation, before any
    /// physical spawn. The lease moves with that slot and cannot escape it.
    pub fn process(
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<HostSessionProcess> {
        let tree = ProcessTreeStorage::new(&work.runtime_meter()).map_err(storage)?;
        Ok(HostSessionProcess {
            tree,
            lease: Some(self),
        })
    }

    /// An unstarted lease already fenced every preceding physical owner.
    pub fn quiescent(self) -> QuiescentHostProcess {
        QuiescentHostProcess(self)
    }
}

impl QuiescentHostProcess {
    /// Refuses applying one task's physical proof to another view or claim.
    pub fn verify(&self, task: TaskId, view: &Path) -> Result<()> {
        if self.0.task != task || self.0.view != view {
            return Err(Error::Conflict(
                "step=verify native quiescence: task or view differs; recovery: reopen the original task through acyclic host".into(),
            ));
        }
        Ok(())
    }
}

/// A native host process, with its exact workspace lease and original funding.
/// The tree retires before the lease, including a partially failed spawn.
pub struct HostSessionProcess {
    tree: ProcessTreeStorage,
    lease: Option<HostProcessLease>,
}

impl HostSessionProcess {
    /// Starts in the exact admitted view. Hosts retain their tools and policy.
    pub fn spawn(&mut self, command: &mut std::process::Command) -> Result<()> {
        let lease = self.lease.as_ref().ok_or_else(|| Error::Conflict(
            "step=spawn native session: process already released; recovery: reopen the task through acyclic host".into()
        ))?;
        if command.get_current_dir() != Some(lease.view.as_path()) {
            return Err(Error::Unauthorized(
                "step=spawn native session: cwd differs from admitted view; recovery: reopen the task through acyclic host".into(),
            ));
        }
        command
            .env("ACYCLIC_WORKSPACE", &lease.view)
            .env("ACYCLIC_TASK", lease.task.to_string());
        self.tree
            .spawn(command, Some(&lease.custody))
            .map_err(storage)
    }

    /// Queues a prompt on the originally funded native stdin owner.
    pub fn queue_stdin(&mut self, bytes: Vec<u8>) -> Result<()> {
        self.live_tree()?.queue_stdin(bytes).map_err(storage)
    }

    /// Advances the existing native stdin pump.
    pub fn pump_stdin(&mut self) -> Result<bool> {
        self.live_tree()?.pump_stdin().map_err(storage)
    }

    /// Verifies delivery even after physical retirement.
    pub fn require_stdin_complete(&self) -> Result<()> {
        self.tree.require_stdin_complete().map_err(storage)
    }

    /// Reads through the existing bounded native output owner.
    pub fn read_stdout(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.live_tree()?.read_stdout(bytes).map_err(storage)
    }

    /// Observes native exit without releasing descendant custody.
    pub fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>> {
        self.live_tree()?.try_wait().map_err(storage)
    }

    /// Stops descendants using the one runtime containment implementation.
    pub fn terminate_descendants(&mut self) -> Result<()> {
        self.live_tree()?.terminate_descendants().map_err(storage)
    }

    fn live_tree(&mut self) -> Result<&mut ProcessTreeStorage> {
        if self.lease.is_none() {
            return Err(Error::Conflict(
                "step=access native session: process already released; recovery: reopen the task through acyclic host".into(),
            ));
        }
        Ok(&mut self.tree)
    }

    /// Releases execution custody only after the complete tree is reaped.
    /// Publication reacquires that lease and retains it through the commit.
    pub fn release(&mut self) -> Result<()> {
        self.tree.terminate().map_err(storage)?;
        if !self.tree.is_reaped() {
            return Err(Error::Conflict(
                "step=release native process: containment remains active; recovery: reopen the original task through acyclic host".into(),
            ));
        }
        self.lease.take();
        Ok(())
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;

    #[test]
    fn failed_spawn_keeps_physical_custody_until_shared_retirement() -> Result<()> {
        let root = tempfile::tempdir().map_err(storage)?;
        let task = TaskId::new();
        let view = root.path().canonicalize().map_err(storage)?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut process = HostProcessLease::acquire(&view, task, &view)?.process(&work)?;
        let mut command = std::process::Command::new(view.join("absent-native-program"));
        command.current_dir(&view);
        assert!(process.spawn(&mut command).is_err());
        assert!(HostProcessLease::acquire(&view, task, &view).is_err());
        process.release()?;
        assert!(
            process.spawn(&mut command).is_err(),
            "retired owner cannot spawn again"
        );
        HostProcessLease::acquire(&view, task, &view)?
            .quiescent()
            .verify(task, &view)?;
        Ok(())
    }

    #[test]
    fn native_process_admission_rejects_a_different_view() -> Result<()> {
        let root = tempfile::tempdir().map_err(storage)?;
        let outside = tempfile::tempdir().map_err(storage)?;
        let task = TaskId::new();
        assert!(HostProcessLease::acquire(root.path(), task, outside.path()).is_err());
        let view = root.path().canonicalize().map_err(storage)?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut process = HostProcessLease::acquire(&view, task, &view)?.process(&work)?;
        let mut command = std::process::Command::new("must-not-execute");
        command.current_dir(outside.path());
        let error = process
            .spawn(&mut command)
            .expect_err("foreign cwd admitted");
        assert!(error.to_string().contains("cwd differs from admitted view"));
        process.release()?;
        Ok(())
    }
}
