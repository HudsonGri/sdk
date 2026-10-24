use super::*;
use crate::{Digest, Fs, WorkspaceName};

fn workspace(name: &str) -> WorkspaceId {
    WorkspaceId::derive(
        [9; 16],
        &WorkspaceName::new(
            name,
            &crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .expect("valid name")
        .value,
        &crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .expect("unbounded workspace identity")
    .value
}

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

#[tokio::test]
async fn a_child_without_its_creation_identity_cannot_authorize_publication() {
    let child = workspace("identity-child");
    let record = WorkspaceLineageRecord {
        version: LINEAGE_VERSION,
        revision: 1,
        ready: true,
        workspace_id: child,
        workspace_name: "identity-child".into(),
        parent_workspace_id: Some(workspace("identity-parent")),
        parent_workspace_name: Some("identity-parent".into()),
        creation_operation: Some(crate::OperationId::from_bytes([72; 16])),
        fork_generation: generation(1),
        initial_generation: generation(2),
    };
    assert!(lineage_shape_is_valid(&record));
    // The old schema accepted a child based only on names and generations.
    // Loading it directly models a durable record, not caller validation.
    for invalid in [
        WorkspaceLineageRecord {
            creation_operation: None,
            ..record.clone()
        },
        WorkspaceLineageRecord {
            version: LINEAGE_VERSION - 1,
            ..record.clone()
        },
    ] {
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        assert!(
            graph
                .store()
                .compare_and_swap(child, 0, invalid)
                .await
                .unwrap()
        );
        assert!(matches!(
            graph
                .authorize_join(child, workspace("identity-parent"))
                .await,
            Err(WorkspaceLineageError::IncompatibleState)
        ));
    }
}

#[tokio::test]
async fn malformed_lineage_shape_never_enters_the_graph() {
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    let child = workspace("child");
    let malformed = WorkspaceLineageRecord {
        version: LINEAGE_VERSION,
        revision: 1,
        ready: true,
        workspace_id: child,
        workspace_name: "child".to_owned(),
        parent_workspace_id: Some(child),
        parent_workspace_name: None,
        creation_operation: Some(crate::OperationId::from_bytes([1; 16])),
        fork_generation: generation(1),
        initial_generation: generation(2),
    };

    assert!(matches!(
        graph.register(malformed).await,
        Err(WorkspaceLineageError::IncompatibleState)
    ));
}

#[tokio::test]
async fn direct_parent_authorization_and_recursive_walk_are_durable() {
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    let root = workspace("root");
    let child = workspace("child");
    let grandchild = workspace("grandchild");
    graph
        .register(WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: true,
            workspace_id: root,
            workspace_name: "root".to_owned(),
            parent_workspace_id: None,
            parent_workspace_name: None,
            creation_operation: None,
            fork_generation: generation(1),
            initial_generation: generation(1),
        })
        .await
        .expect("root registration");
    for (workspace_id, parent_workspace_id, byte) in [(child, root, 2), (grandchild, child, 3)] {
        graph
            .register(WorkspaceLineageRecord {
                version: LINEAGE_VERSION,
                revision: 1,
                ready: true,
                workspace_id,
                workspace_name: if workspace_id == child {
                    "child".to_owned()
                } else {
                    "grandchild".to_owned()
                },
                parent_workspace_id: Some(parent_workspace_id),
                parent_workspace_name: Some(if parent_workspace_id == root {
                    "root".to_owned()
                } else {
                    "child".to_owned()
                }),
                creation_operation: Some(crate::OperationId::from_bytes([byte; 16])),
                fork_generation: generation(byte),
                initial_generation: generation(byte),
            })
            .await
            .expect("child registration");
    }
    graph
        .authorize_join(grandchild, child)
        .await
        .expect("direct parent");
    assert!(matches!(
        graph.authorize_join(grandchild, root).await,
        Err(WorkspaceLineageError::UnauthorizedJoin)
    ));
    let ancestors = graph
        .ancestors(grandchild, 3)
        .await
        .expect("bounded ancestry");
    assert_eq!(
        ancestors
            .iter()
            .map(|record| record.workspace_id)
            .collect::<Vec<_>>(),
        vec![child, root]
    );
}

#[tokio::test]
async fn a_durable_planned_child_is_completed_by_retry_after_a_crash() {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "root",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("root");
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    graph.register_root(&parent).await.expect("register root");
    let parent_head = parent
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .expect("parent head");
    let child_id = parent
        .volume
        .fs
        .workspace_id("child")
        .expect("derived child identity");
    let planned = WorkspaceLineageRecord {
        version: LINEAGE_VERSION,
        revision: 1,
        ready: false,
        workspace_id: child_id,
        workspace_name: "child".to_owned(),
        parent_workspace_id: Some(parent.id()),
        parent_workspace_name: Some(parent.name().as_str().to_owned()),
        creation_operation: Some(crate::OperationId::from_bytes([31; 16])),
        fork_generation: parent_head.id(),
        initial_generation: parent_head.id(),
    };
    graph.register(planned).await.expect("persist fork intent");
    assert!(matches!(
        graph.resolve(child_id).await,
        Err(WorkspaceLineageError::IncompatibleState)
    ));

    // The crash cut is durable before physical creation. Advancing the parent
    // must not select a new cut when the same creation resumes.
    parent
        .write_text("/later-parent", "parent advanced after intent")
        .await
        .unwrap();
    let child = fork_workspace(
        &graph,
        &parent,
        "child",
        IdempotencyKey::from_bytes([31; 16]),
    )
    .await
    .expect("retry completes child");
    assert_eq!(child.id(), child_id);
    let recovered = graph.resolve(child_id).await.expect("ready lineage");
    assert!(recovered.ready);
    assert_eq!(recovered.parent_workspace_id, Some(parent.id()));
    assert_eq!(
        recovered.initial_generation,
        child
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .expect("head")
            .id()
    );
}

#[tokio::test]
async fn recovered_fork_keeps_creation_identity_after_both_heads_advance() {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "replay-root",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .value;
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    let key = IdempotencyKey::from_bytes([51; 16]);
    let child = fork_workspace(&graph, &parent, "replay-child", key)
        .await
        .unwrap();
    let original = graph.resolve(child.id()).await.unwrap();
    parent
        .write_text("/parent-later", "new parent state")
        .await
        .unwrap();
    child
        .write_text("/child-later", "new child state")
        .await
        .unwrap();
    // Negative control for rereading either head: both differ from the durable
    // registration, yet resuming the original operation must leave it intact.
    assert_ne!(
        parent
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .unwrap()
            .value
            .id(),
        original.fork_generation
    );
    assert_ne!(
        child
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .unwrap()
            .value
            .id(),
        original.initial_generation
    );
    let recovered = fork_workspace(&graph, &parent, "replay-child", key)
        .await
        .unwrap();
    assert_eq!(recovered.id(), child.id());
    assert_eq!(graph.resolve(child.id()).await.unwrap(), original);
    assert!(matches!(
        fork_workspace(
            &graph,
            &parent,
            "replay-child",
            IdempotencyKey::from_bytes([52; 16])
        )
        .await,
        Err(WorkspaceLineageError::ConflictingRegistration)
    ));
}

#[tokio::test]
async fn dropped_external_creator_cannot_publish_lineage_or_change_the_recovery_cut() {
    let fs = Fs::memory();
    let parent = fs
        .create_workspace(
            "external-parent",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .value;
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    graph.register_root(&parent).await.unwrap();
    let key = IdempotencyKey::from_bytes([61; 16]);
    let child_id = fs.workspace_id("external-child").unwrap();
    let cut = parent
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value
        .id();
    let parent_ref = &parent;
    let (created, observed) = tokio::sync::oneshot::channel();
    let mut attempt = Box::pin(graph.fork(
        &parent,
        "external-child",
        key,
        Some(cut),
        |generation| async move {
            let child = parent_ref
                .fork::<_, _, WorkspaceError>(
                    "external-child",
                    crate::ForkOptions::from_generation(generation, key),
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                    (),
                )
                .await
                .map_err(|failure| {
                    WorkspaceLineageError::<MemoryWorkspaceLineageStoreError>::from(failure.error)
                })?
                .value;
            created.send(()).unwrap();
            std::future::pending::<()>().await;
            Ok::<_, WorkspaceLineageError<MemoryWorkspaceLineageStoreError>>(child)
        },
        |child| child,
    ));
    tokio::select! {
        _ = &mut attempt => panic!("creator unexpectedly returned before its interruption point"),
        result = observed => result.unwrap(),
    }
    drop(attempt);
    let planned = graph.store().load(child_id).await.unwrap().unwrap();
    assert!(!planned.ready);
    assert_eq!(planned.fork_generation, cut);
    assert!(graph.resolve(child_id).await.is_err());
    parent
        .write_text("/later", "after creator interruption")
        .await
        .unwrap();
    let child = graph
        .fork(
            &parent,
            "external-child",
            key,
            None,
            |generation| async move {
                assert_eq!(generation.id(), cut);
                parent_ref
                    .fork::<_, _, WorkspaceError>(
                        "external-child",
                        crate::ForkOptions::from_generation(generation, key),
                        crate::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                        (),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| {
                        WorkspaceLineageError::<MemoryWorkspaceLineageStoreError>::from(
                            failure.error,
                        )
                    })
            },
            |child| child,
        )
        .await
        .unwrap();
    assert_eq!(child.id(), child_id);
    assert!(graph.resolve(child_id).await.unwrap().ready);
    assert_eq!(graph.resolve(child_id).await.unwrap().fork_generation, cut);
}

async fn fork_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    graph: &WorkspaceGraph<MemoryWorkspaceLineageStore>,
    parent: &Workspace<A, O>,
    destination: &str,
    key: IdempotencyKey,
) -> Result<Workspace<A, O>, WorkspaceLineageError<MemoryWorkspaceLineageStoreError>> {
    graph
        .fork(
            parent,
            destination,
            key,
            None,
            |generation| async move {
                parent
                    .fork::<_, _, WorkspaceError>(
                        destination,
                        crate::ForkOptions::from_generation(generation, key),
                        crate::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                        (),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| WorkspaceLineageError::from(failure.error))
            },
            |child| child,
        )
        .await
}
