use super::*;
use crate::{
    distributed::{WorkPull, Worker},
    runtime::RuntimeTask,
    scheduler::LeaseFence,
};

struct Fixture {
    checkout: tempfile::TempDir,
    state: tempfile::TempDir,
    harness: LocalHarness,
    provider: Arc<crate::test_support::ScriptedModelProvider>,
}

impl Fixture {
    async fn open() -> Result<Self> {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::write(checkout.path().join("source.txt"), b"root source").map_err(storage)?;
        let provider = Arc::new(
            crate::test_support::ScriptedModelProvider::new([]).with_context_capacity(
                crate::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                },
            ),
        );
        let harness = LocalHarness::open(
            crate::test_support::local_harness_config(
                checkout.path(),
                (
                    Model::new(
                        "scripted".into(),
                        "test".into(),
                        "1".into(),
                        serde_json::Value::Null,
                    )?,
                    provider.clone(),
                ),
            )?
            .compaction(crate::context::CompactionPolicy::Disabled)
            .without_default_tools()
            .state_directory(state.path().join("runtime")),
        )
        .await?;
        Ok(Self {
            checkout,
            state,
            harness,
            provider,
        })
    }

    async fn task(&self, parent: Option<TaskId>) -> Result<TaskContext> {
        let operation = OperationId::new();
        let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) = self
            .harness
            .submit_to(operation, parent, "native host task")
            .await?
        else {
            return Err(Error::Conflict("fixture task was not admitted".into()));
        };
        let WorkPull::Claimed(lease) = self
            .harness
            .runtime
            .task_host()
            .pull_work(
                &Worker {
                    id: "native-host-fixture".into(),
                    available: Default::default(),
                    labels: Default::default(),
                },
                Some(operation),
                &mut None,
            )
            .await?
        else {
            return Err(Error::Conflict("fixture task was not claimed".into()));
        };
        self.harness
            .runtime
            .task_host()
            .start_task(&lease, &mut None)
            .await?;
        self.harness
            .runtime
            .task_context(task_id, LeaseFence::from(&lease.reservation))
            .await
    }

    async fn attach(&self, context: &TaskContext, mounts: &Path) -> Result<HostWorkspace> {
        let workspace = self.harness.workspace().unwrap();
        HostWorkspace::attach(
            &workspace.filesystem,
            workspace.store.clone(),
            context,
            HostWorkspaceAttachment {
                root: acyclic_fs::hold_root(&workspace.path).map_err(storage)?,
                admitted_mount_root: acyclic_fs::hold_root(mounts).map_err(storage)?,
                state_directory: self.state.path().join("host"),
                project: self.harness.project().clone(),
                scratch: self.harness.scratch().clone(),
            },
        )
        .await
    }
}

#[tokio::test]
async fn native_writes_fork_and_reopen_without_execution() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let root = acyclic_fs::hold_root(fixture.checkout.path()).map_err(storage)?;
    let mounts = root
        .create_directories(Path::new(HOST_VIEW_ROOT))
        .map_err(storage)?;
    let workspace = fixture.attach(&parent, mounts.path()).await?;
    std::fs::write(workspace.path().join("parent.txt"), b"host authored").map_err(storage)?;
    std::fs::write(mounts.path().join("reserved.txt"), b"not project history").map_err(storage)?;
    workspace.capture(&parent, OperationId::new()).await?;
    let child = fixture.task(Some(workspace.task())).await?;
    assert!(matches!(
        workspace.reopen(&parent, &child).await,
        Err(Error::NotFound(_))
    ));
    let fork = workspace.fork(&parent, &child).await?;
    assert!(fork.path().starts_with(mounts.path()));
    assert_eq!(
        std::fs::read(fork.path().join("parent.txt")).map_err(storage)?,
        b"host authored"
    );
    assert!(
        !fork
            .path()
            .join(HOST_VIEW_ROOT)
            .join("reserved.txt")
            .exists()
    );
    std::fs::write(fork.path().join("child.txt"), b"child only").map_err(storage)?;
    fork.capture(&child, OperationId::new()).await?;
    assert!(!fixture.checkout.path().join("child.txt").exists());
    let reopened = workspace.reopen(&parent, &child).await?;
    assert_eq!(
        std::fs::read(reopened.path().join("child.txt")).map_err(storage)?,
        b"child only"
    );
    let grandchild = fixture.task(Some(fork.task())).await?;
    let nested = fork.fork(&child, &grandchild).await?;
    assert!(nested.path().starts_with(mounts.path()));
    assert_eq!(
        std::fs::read(nested.path().join("child.txt")).map_err(storage)?,
        b"child only"
    );
    assert!(fixture.provider.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn task_grants_parentage_and_cancellation_fence_every_boundary() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let foreign = fixture.task(None).await?;
    let mounts = tempfile::tempdir().map_err(storage)?;
    let workspace = fixture.attach(&parent, mounts.path()).await?;
    let unrelated = fixture
        .task(Some(foreign.owned_task_binding(None)?.0))
        .await?;
    assert!(workspace.fork(&parent, &unrelated).await.is_err());
    assert!(workspace.reopen(&parent, &unrelated).await.is_err());
    assert!(
        workspace
            .capture(&foreign, OperationId::new())
            .await
            .is_err()
    );
    let child = fixture.task(Some(workspace.task())).await?;
    let narrowed_parent = parent.scoped(
        Capabilities::new(std::iter::empty::<&str>()),
        parent.scope().limits(),
    )?;
    assert!(
        workspace
            .capture(&narrowed_parent, OperationId::new())
            .await
            .is_err()
    );
    assert!(workspace.fork(&narrowed_parent, &child).await.is_err());
    assert!(workspace.reopen(&narrowed_parent, &child).await.is_err());
    assert!(
        fixture
            .attach(&narrowed_parent, mounts.path())
            .await
            .is_err()
    );
    let narrowed = child.scoped(
        Capabilities::new(std::iter::empty::<&str>()),
        child.scope().limits(),
    )?;
    assert!(workspace.fork(&parent, &narrowed).await.is_err());
    assert!(fixture.attach(&narrowed, mounts.path()).await.is_err());
    let fork = workspace.fork(&parent, &child).await?;
    fixture
        .harness
        .runtime
        .task_host()
        .cancel(fork.task())
        .await?;
    assert!(fork.capture(&child, OperationId::new()).await.is_err());
    assert!(workspace.fork(&parent, &child).await.is_err());
    assert!(workspace.reopen(&parent, &child).await.is_err());
    fixture
        .harness
        .runtime
        .task_host()
        .cancel(workspace.task())
        .await?;
    assert!(
        workspace
            .capture(&parent, OperationId::new())
            .await
            .is_err()
    );
    assert!(fixture.attach(&parent, mounts.path()).await.is_err());
    assert!(fixture.provider.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn attachment_rejects_an_arbitrary_in_checkout_mount_root() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let mounts = fixture.checkout.path().join("authored-directory");
    std::fs::create_dir(&mounts).map_err(storage)?;
    assert!(matches!(
        fixture.attach(&parent, &mounts).await,
        Err(Error::Invalid(_))
    ));
    assert!(!fixture.state.path().join("host").exists());
    assert!(fixture.provider.requests().is_empty());
    Ok(())
}
