use super::*;
use crate::{WorkspaceGraph, WorkspaceLineageError, WorkspaceLineageStore};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_fork_retains_its_original_content_through_collection_and_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let options = LocalOptions::new(directory.path().join("filesystem"));
    let state = directory.path().join("core");
    let key = crate::IdempotencyKey::from_bytes([83; 16]);
    let original_content = "original fork content ".repeat(4096);
    let cut;
    {
        let fs = Fs::local(options.clone()).await?;
        let parent = fs
            .create_workspace("parent", WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        parent.write_text("/content", &original_content).await?;
        cut = parent
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id();
        let graph = WorkspaceGraph::new(crate::LocalCoreStateStore::new(&state));
        graph
            .register_root(
                &parent,
                &mut crate::kernel::KernelAccounting::standalone(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                ),
            )
            .await?;
        // Stop after intent publication, before even creating a child authority.
        // Recovery must not depend on a live Generation or a physical fork pin.
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        let interrupted: Result<
            crate::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
            WorkspaceLineageError<crate::LocalCoreStateStoreError>,
        > = graph
            .fork(
                &parent,
                "child",
                key,
                Some(cut),
                &mut accounting,
                StopBeforeCreation,
                |child| child,
            )
            .await;
        assert!(matches!(
            interrupted,
            Err(WorkspaceLineageError::UnauthorizedJoin)
        ));
        let intent = graph
            .store()
            .load(fs.workspace_id("child")?)
            .await?
            .ok_or("missing intent")?;
        assert!(!intent.ready);
        assert_eq!(intent.fork_generation, cut);
        parent
            .write_text("/content", "replacement after interruption")
            .await?;
        let current = parent
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        assert_ne!(current.id(), cut);

        // Causal control: keeping only the head and ancestor root metadata loses
        // content unique to the captured cut. Marking live authority history is
        // the existing shared retention mechanism; no graph-specific pin exists.
        let mut old = crate::kernel::Marker::new(&fs.inner.objects, &cancellation);
        old.generation(cut).await?;
        let mut head_only = crate::kernel::Marker::new(&fs.inner.objects, &cancellation);
        head_only.generation(current.id()).await?;
        head_only.lineage_roots().await?;
        assert!(
            !old.marked().is_subset(head_only.marked()),
            "control did not lose old content"
        );
        let mut retained = crate::kernel::Marker::new(&fs.inner.objects, &cancellation);
        fs.mark_local_authorities(&mut retained, &cancellation)
            .await?;
        assert!(old.marked().is_subset(retained.marked()));
        drop((old, head_only, retained, current, intent, graph, parent));
        let released = fs
            .local_root_release_barrier()
            .ok_or("missing release barrier")?;
        drop(fs);
        released.wait_async().await;
    }

    let fs = Fs::local(options).await?;
    let graph = WorkspaceGraph::new(crate::LocalCoreStateStore::new(&state));
    fs.collect_local_garbage(Some(graph.store()), &cancellation)
        .await?;
    let parent = fs
        .open_workspace("parent", WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let child = graph
        .fork::<_, _, _, WorkspaceLineageError<crate::LocalCoreStateStoreError>, _>(
            &parent,
            "child",
            key,
            None,
            &mut accounting,
            crate::lineage::GenerationFork {
                parent: &parent,

                permit: crate::PublicationPermit::Unrestricted,
            },
            |child| child,
        )
        .await?;
    assert_eq!(
        child
            .read("/content", 128 * 1024, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .as_ref(),
        original_content.as_bytes()
    );
    assert_eq!(
        graph
            .authorize_join(child.id(), parent.id())
            .await?
            .fork_generation,
        cut
    );
    Ok(())
}

struct StopBeforeCreation;
impl
    crate::lineage::WorkspaceForkCreator<
        LocalAuthorityBackend,
        LocalObjectBackend,
        crate::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        WorkspaceLineageError<crate::LocalCoreStateStoreError>,
    > for StopBeforeCreation
{
    async fn create(
        self,
        _creation: crate::lineage::WorkspaceForkCreation<
            '_,
            LocalAuthorityBackend,
            LocalObjectBackend,
        >,
        _accounting: &mut KernelAccounting<'_>,
    ) -> Result<
        crate::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        WorkspaceLineageError<crate::LocalCoreStateStoreError>,
    > {
        Err(WorkspaceLineageError::UnauthorizedJoin)
    }
}
