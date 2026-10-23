//! Read-only git must not create control-plane publications.
use super::*;
use acyclic_fs::OperationWindowStore as _;

#[tokio::test]
async fn clean_status_does_not_append_core_state_frames() -> Result<()> {
    status_cost(true).await
}

#[tokio::test]
async fn root_clean_status_does_not_append_core_state_frames() -> Result<()> {
    status_cost(false).await
}

struct StatusFixture {
    harness: LocalHarness,
    tools: Arc<CheckoutTools>,
    task: Arc<crate::runtime::TaskContext>,
    _directory: tempfile::TempDir,
    state: tempfile::TempDir,
}

impl StatusFixture {
    async fn open(child: bool) -> Result<Self> {
        let (harness, context, directory, state) = fixture(None).await?;
        let workspace = harness
            .workspace()
            .ok_or_else(|| Error::NotFound("checkout".into()))?;
        let task = if child {
            child_task(
                &harness,
                context
                    .task()
                    .durable_task_id()
                    .ok_or_else(|| Error::Invalid("fixture task is not durable".into()))?,
            )
            .await?
        } else {
            Arc::new(context.task().clone())
        };
        let root = Arc::new(
            CheckoutTools::new(
                Arc::clone(workspace),
                &harness.project,
                Arc::new(acyclic_stream::SystemUnixMillisClock),
                Arc::clone(&harness.task_files),
            )
            .await?,
        );
        let selecting =
            ToolContext::new(Arc::clone(&task), OperationId::new(), "status-select", None)?;
        let tools = root.select(&selecting).await?.unwrap_or(root);
        Ok(Self {
            harness,
            tools,
            task,
            _directory: directory,
            state,
        })
    }

    async fn command(&self, index: u32, command: &str) -> Result<ToolResult> {
        let operation_id = OperationId::new();
        let call_id = format!("status-{index}");
        let invocation = crate::tool::ToolInvocation {
            operation_id,
            call_id: call_id.clone(),
            name: "fs".into(),
            arguments: serde_json::json!({"command": command}),
        };
        let context = ToolContext::new(Arc::clone(&self.task), operation_id, &call_id, None)?;
        let result = self.tools.execute(invocation, Some(context)).await?;
        assert!(result.value.get("error").is_none(), "{result:?}");
        assert!(
            result.value.get("result").is_some(),
            "command did not execute: {result:?}"
        );
        Ok(result)
    }

    async fn inspect_native_change(&self, first: &ToolResult) -> Result<()> {
        std::fs::write(
            self.tools.workspace.path.join("status-baseline"),
            b"changed native bytes\n",
        )
        .map_err(storage)?;
        let before = durable_files(self.state.path())?;
        let changed = self.command(5, "status").await?;
        assert_diff(&self.command(102, "git diff").await?, 1)?;
        let shown = decode(&self.command(103, "git show HEAD:status-baseline").await?)?;
        let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
            kind,
            value,
        }) = shown
        else {
            return Err(Error::Invalid("expected committed file result".into()));
        };
        assert_eq!(kind, "file");
        assert_eq!(
            value["text"], "committed bytes\n",
            "show must retain committed bytes even when HEAD names the live lazy cut"
        );
        let (
            acyclic_fs::GitCommandOutput::Status(first),
            acyclic_fs::GitCommandOutput::Status(changed),
        ) = (decode(first)?, decode(&changed)?)
        else {
            return Err(Error::Invalid("expected typed status results".into()));
        };
        assert_eq!(
            first.dirty,
            acyclic_fs::GitDirtyState::Clean,
            "unchanged physical content must compare clean against committed HEAD"
        );
        assert_eq!(
            changed.dirty,
            acyclic_fs::GitDirtyState::Dirty,
            "native changes must compare dirty against committed HEAD"
        );
        if changed.branch.starts_with("agents/") {
            assert_diff(
                &self
                    .command(104, &format!("git diff {}", changed.branch))
                    .await?,
                0,
            )?;
        }
        assert_ne!(
            first.workspace, changed.workspace,
            "read-only status must inspect native changes"
        );
        assert_eq!(
            durable_files(self.state.path())?,
            before,
            "dirty status and diff must leave every durable state file unchanged"
        );
        Ok(())
    }
}

fn decode(result: &ToolResult) -> Result<acyclic_fs::GitCommandOutput> {
    serde_json::from_value(
        result
            .value
            .get("result")
            .cloned()
            .ok_or_else(|| Error::Invalid("missing Git result".into()))?,
    )
    .map_err(storage)
}

async fn status_cost(child: bool) -> Result<()> {
    let case = StatusFixture::open(child).await?;
    let workspace = &case.tools.workspace;
    std::fs::write(workspace.path.join("status-baseline"), b"committed bytes\n")
        .map_err(storage)?;
    case.command(100, "git commit -m status-baseline").await?;
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    let result = schedule.scope(status_reads(&case, child)).await;
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    let writes = schedule.durable_writes();
    eprintln!(
        "STATUS_DURABLE_WRITES child={child} operations={}",
        writes.len()
    );
    assert!(
        writes.is_empty(),
        "read-only Git must perform no durable writes: {writes:?}"
    );
    case.harness.disconnect_unfinished().await?;
    result
}

async fn status_reads(case: &StatusFixture, child: bool) -> Result<()> {
    let workspace = &case.tools.workspace;
    let status = async |index| case.command(index, "status").await;
    let log = workspace
        .store
        .root()
        .join("core-state-log-v1")
        .join(format!("{}.json", "0".repeat(32)));
    let windows = workspace.filesystem.operation_window_store();
    let workspace_id = workspace.view().workspace().id();
    let before_window = windows.load(workspace_id).await.map_err(storage)?;
    let before_revision = before_window.as_ref().map_or(0, |window| window.revision);
    let before = read_log(&log)?;
    let before_files = durable_files(case.state.path())?;
    let first = status(0).await?;
    assert_diff(&case.command(101, "git diff").await?, 0)?;
    let before_frames = before.iter().filter(|byte| **byte == b'\n').count();
    let started = std::time::Instant::now();
    for index in 1..=4 {
        status(index).await?;
    }
    let elapsed = started.elapsed();
    assert_eq!(
        durable_files(case.state.path())?,
        before_files,
        "cold and warm status must leave every durable state file unchanged"
    );
    case.inspect_native_change(&first).await?;
    let after_window = windows.load(workspace_id).await.map_err(storage)?;
    let after_revision = after_window.as_ref().map_or(0, |window| window.revision);
    eprintln!(
        "STATUS_WINDOW child={child} calls=4 before_revision={before_revision} after_revision={after_revision}"
    );
    let after = read_log(&log)?;
    let after_frames = after.iter().filter(|byte| **byte == b'\n').count();
    eprintln!(
        "STATUS_COST child={child} calls=4 before_frames={before_frames} after_frames={after_frames} before_bytes={} after_bytes={} elapsed_us={}",
        before.len(),
        after.len(),
        elapsed.as_micros()
    );
    assert!(
        after == before,
        "clean read-only git must leave the core-state journal unchanged"
    );
    assert_eq!(
        after_revision, before_revision,
        "clean read-only git must not publish operation-window leases"
    );
    Ok(())
}

fn assert_diff(result: &ToolResult, expected: u64) -> Result<()> {
    let decoded: acyclic_fs::GitCommandOutput = serde_json::from_value(
        result
            .value
            .get("result")
            .cloned()
            .ok_or_else(|| Error::Invalid("missing diff result".into()))?,
    )
    .map_err(storage)?;
    let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
        kind,
        value,
    }) = decoded
    else {
        return Err(Error::Invalid("expected typed diff result".into()));
    };
    assert_eq!(kind, "diff");
    eprintln!(
        "NATIVE_DIFF expected={expected} observed={}",
        value["fileChanges"]
    );
    assert_eq!(
        value["fileChanges"].as_u64(),
        Some(expected),
        "native diff must compare the immutable HEAD with the live physical view"
    );
    Ok(())
}

fn read_log(path: &std::path::Path) -> Result<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(cause) => Err(storage(cause)),
    }
}

#[derive(Debug, Eq, PartialEq)]
enum DurableEntry {
    File(Vec<u8>),
    Link(std::path::PathBuf),
}

fn durable_files(
    root: &std::path::Path,
) -> Result<std::collections::BTreeMap<std::path::PathBuf, DurableEntry>> {
    let mut files = std::collections::BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).map_err(storage)? {
            let entry = entry.map_err(storage)?;
            let kind = entry.file_type().map_err(storage)?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_symlink() {
                files.insert(
                    entry.path(),
                    DurableEntry::Link(std::fs::read_link(entry.path()).map_err(storage)?),
                );
            } else if kind.is_file() {
                let bytes = std::fs::read(entry.path()).map_err(|cause| {
                    Error::Storage(format!("snapshot read {:?}: {cause}", entry.path()))
                })?;
                files.insert(entry.path(), DurableEntry::File(bytes));
            }
        }
    }
    Ok(files)
}

async fn child_task(
    harness: &LocalHarness,
    parent: crate::TaskId,
) -> Result<Arc<crate::runtime::TaskContext>> {
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) = harness
        .submit_to(operation, Some(parent), "child status")
        .await?
    else {
        return Err(Error::Invalid("child status task is not durable".into()));
    };
    let worker = crate::distributed::Worker {
        id: "status-cost".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        return Err(Error::Invalid("child status task was not claimed".into()));
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    Ok(Arc::new(
        harness
            .runtime
            .task_context(
                task_id,
                crate::scheduler::LeaseFence::from(&lease.reservation),
            )
            .await?,
    ))
}
