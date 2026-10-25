use super::*;
use crate::facade::FsWork as _;
use crate::{Digest, Fs, WorkspaceName};

struct IssuedForkGuard<'a> {
    denied: bool,
    called: &'a std::sync::atomic::AtomicUsize,
    #[cfg(not(target_arch = "wasm32"))]
    original: Option<&'a crate::NativeMaterializationWork>,
}

impl crate::AuthorityGuard for IssuedForkGuard<'_> {
    type Error = WorkspaceError;

    async fn seal(
        &mut self,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::AuthorityError<Self::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(
            work.original.map(std::ptr::from_ref),
            self.original.map(std::ptr::from_ref)
        );
        work.cancellation
            .check()
            .map_err(|_| crate::AuthorityError::Owner(WorkspaceError::NotFound))?;
        self.called
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.denied {
            Err(crate::AuthorityError::Owner(WorkspaceError::NotFound))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn guarded_graph_fork_retains_issued_guard_paths_and_original_cut() {
    let cancellation = CancellationToken::new();
    for native in [false, true] {
        let fs = Fs::memory();
        let parent = fs
            .create_workspace("guard-parent", WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap()
            .value;
        parent.write_text("/selected", "original").await.unwrap();
        parent.write_text("/excluded", "private").await.unwrap();
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        graph
            .register_root(
                &parent,
                &mut crate::kernel::KernelAccounting::standalone(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                ),
            )
            .await
            .unwrap();
        #[cfg(not(target_arch = "wasm32"))]
        let original = native.then(|| {
            crate::NativeMaterializationWork::new(
                crate::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .unwrap()
        });
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = &original {
            accounting = KernelAccounting::native(original, &cancellation);
        }
        let key = IdempotencyKey::from_bytes([73; 16]);
        let called = std::sync::atomic::AtomicUsize::new(0);
        for denied in [true, false] {
            let result: Result<_, WorkspaceLineageError<MemoryWorkspaceLineageStoreError>> = graph
                .fork(
                    &parent,
                    "guard-child",
                    key,
                    None,
                    &mut accounting,
                    GenerationFork {
                        parent: &parent,

                        permit: crate::PublicationPermit::Unrestricted,
                    }
                    .guarded(
                        IssuedForkGuard {
                            denied,
                            called: &called,
                            #[cfg(not(target_arch = "wasm32"))]
                            original: original.as_ref(),
                        },
                        Some(vec!["/selected".into()]),
                    ),
                )
                .await;
            assert!(called.load(std::sync::atomic::Ordering::SeqCst) > 0);
            let child_id = fs.workspace_id("guard-child").unwrap();
            let planned = graph.store().load(child_id).await.unwrap().unwrap();
            assert_eq!(planned.ready, !denied);
            if denied {
                assert!(matches!(
                    result,
                    Err(WorkspaceLineageError::Workspace(WorkspaceError::NotFound))
                ));
                parent.write_text("/selected", "new head").await.unwrap();
            } else {
                let child = result.unwrap();
                let head = child
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .unwrap()
                    .value;
                assert_eq!(
                    head.read("/selected", 32, WorkBudget::UNBOUNDED, &cancellation)
                        .await
                        .unwrap()
                        .value
                        .as_ref(),
                    b"original"
                );
                assert!(
                    matches!(head.stat("/excluded", WorkBudget::UNBOUNDED, &cancellation).await,
                    Err(failure) if matches!(failure.error, WorkspaceError::NotFound))
                );
            }
        }
        // Native and ordinary entry points execute the same creator and guard.
        assert!(native || called.load(std::sync::atomic::Ordering::SeqCst) > 1);
    }
}

#[tokio::test]
async fn graph_fork_requires_the_exact_ready_parent_before_admitting_child_intent() {
    for native in [false, true] {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let parent = fs
            .create_workspace("parent-admission", WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap()
            .value;
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        for state in 0..4 {
            if state != 0 {
                let head = parent
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .unwrap()
                    .value;
                let root = WorkspaceLineageRecord {
                    version: LINEAGE_VERSION,
                    revision: state,
                    ready: state != 1,
                    workspace_id: parent.id(),
                    workspace_name: if state == 2 {
                        "another-parent".into()
                    } else {
                        parent.name().as_str().into()
                    },
                    parent_workspace_id: None,
                    parent_workspace_name: None,
                    creation_operation: None,
                    fork_generation: head.id(),
                    initial_generation: head.id(),
                };
                assert!(
                    graph
                        .store()
                        .compare_and_swap(parent.id(), state - 1, root)
                        .await
                        .unwrap()
                );
            }
            #[cfg(not(target_arch = "wasm32"))]
            let original = native.then(|| {
                crate::NativeMaterializationWork::new(
                    crate::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .unwrap()
            });
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = &original {
                accounting = KernelAccounting::native(original, &cancellation);
            }
            let called = std::sync::atomic::AtomicBool::new(false);
            let key = IdempotencyKey::from_bytes([91; 16]);
            let result: Result<_, WorkspaceLineageError<MemoryWorkspaceLineageStoreError>> = graph
                .fork(
                    &parent,
                    "parent-admission-child",
                    key,
                    None,
                    &mut accounting,
                    TestFork {
                        parent: &parent,
                        destination: "parent-admission-child",
                        key,
                        cut: None,
                        interrupted: None,
                        called: Some(&called),
                        refuse: false,
                    },
                )
                .await;
            if state == 3 {
                result.unwrap();
                assert!(called.load(std::sync::atomic::Ordering::SeqCst));
            } else {
                assert!(matches!(
                    result,
                    Err(WorkspaceLineageError::IncompatibleState)
                ));
                assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
                let child_id = fs.workspace_id("parent-admission-child").unwrap();
                assert!(graph.store().load(child_id).await.unwrap().is_none());
                assert!(
                    fs.open_workspace(
                        "parent-admission-child",
                        WorkBudget::UNBOUNDED,
                        &cancellation
                    )
                    .await
                    .is_err()
                );
            }
        }
    }
}

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

#[tokio::test]
async fn root_registration_cannot_reset_its_original_admission_budget() {
    for native in [false, true] {
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let root = fs
            .create_workspace("budget-root", WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap()
            .value;
        let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
        let mut budget = crate::performance::BOUNDARY_BUDGET;
        budget.items_examined = 0;
        let mut accounting = KernelAccounting::standalone(budget, &cancellation);
        #[cfg(not(target_arch = "wasm32"))]
        let original =
            native.then(|| crate::NativeMaterializationWork::new(budget, &cancellation).unwrap());
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = &original {
            accounting = KernelAccounting::native(original, &cancellation);
        }
        assert!(
            graph.register_root(&root, &mut accounting).await.is_err(),
            "native={native}"
        );
        assert!(graph.store().load(root.id()).await.unwrap().is_none());
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = &original {
            assert_eq!(original.receipt().backend_write_operations, 0);
            assert_eq!(
                original.live_allocation_bytes(),
                crate::NativeMaterializationWork::accounting_state_bytes()
            );
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn root_registration_retains_exact_native_metadata_custody_through_reopen() {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let root = fs
        .create_workspace("owned-root", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .unwrap();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let baseline = original.live_allocation_bytes();
    let registered = graph.register_root(&root, &mut accounting).await.unwrap();
    let initial = registered.value.initial_generation;
    let bytes = root.name().as_str().len() as u64;
    let retained = MemoryWorkspaceLineageStore::node_bytes() + bytes;
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + retained + bytes
    );
    registered.retire(&mut accounting).unwrap();
    assert_eq!(original.live_allocation_bytes(), baseline + retained);
    root.write_text("/later", "newer head").await.unwrap();
    let loaded = graph.register_root(&root, &mut accounting).await.unwrap();
    assert_eq!(loaded.value.initial_generation, initial);
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + retained + bytes
    );
    loaded.retire(&mut accounting).unwrap();
    drop(graph);
    assert_eq!(original.live_allocation_bytes(), baseline);
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
    let cancellation = CancellationToken::new();
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    assert!(lineage_shape_accounted(&record, &mut accounting).unwrap());
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
        graph
            .register(
                &malformed,
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())
            )
            .await,
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
        .register(
            &WorkspaceLineageRecord {
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
            },
            &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await
        .expect("root registration");
    for (workspace_id, parent_workspace_id, byte) in [(child, root, 2), (grandchild, child, 3)] {
        graph
            .register(
                &WorkspaceLineageRecord {
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
                },
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new()),
            )
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
    graph
        .register_root(
            &parent,
            &mut crate::kernel::KernelAccounting::standalone(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            ),
        )
        .await
        .expect("register root");
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
    graph
        .register(
            &planned,
            &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await
        .expect("persist fork intent");
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
    graph
        .register_root(
            &parent,
            &mut crate::kernel::KernelAccounting::standalone(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            ),
        )
        .await
        .unwrap();
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
    graph
        .register_root(
            &parent,
            &mut crate::kernel::KernelAccounting::standalone(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            ),
        )
        .await
        .unwrap();
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
    let cancellation = CancellationToken::new();
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let mut attempt = Box::pin(graph.fork(
        &parent,
        "external-child",
        key,
        Some(cut),
        &mut accounting,
        TestFork {
            parent: parent_ref,
            destination: "external-child",
            key,
            cut: Some(cut),
            interrupted: Some(created),
            called: None,
            refuse: false,
        },
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
            &mut accounting,
            TestFork {
                parent: parent_ref,
                destination: "external-child",
                key,
                cut: Some(cut),
                interrupted: None,
                called: None,
                refuse: false,
            },
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
    let cancellation = CancellationToken::new();
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    graph
        .fork(
            parent,
            destination,
            key,
            None,
            &mut accounting,
            GenerationFork {
                parent,
                permit: crate::PublicationPermit::Unrestricted,
            },
        )
        .await
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn fork_refuses_the_original_work_budget_before_any_creator_or_durable_intent() {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let parent = fs
        .create_workspace("budget-parent", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    graph
        .register_root(
            &parent,
            &mut crate::kernel::KernelAccounting::standalone(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            ),
        )
        .await
        .unwrap();
    let mut budget = crate::performance::work_budget(4096, 8 * 1024 * 1024);
    budget.items_examined = 0;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let original_work = &original;
    let mut accounting = original_work.accounting(&cancellation);
    let called = std::sync::atomic::AtomicBool::new(false);
    let attempt = graph
        .fork(
            &parent,
            "budget-child",
            IdempotencyKey::from_bytes([95; 16]),
            None,
            &mut accounting,
            TestFork {
                parent: &parent,
                destination: "budget-child",
                key: IdempotencyKey::from_bytes([95; 16]),
                cut: None,
                interrupted: None,
                called: Some(&called),
                refuse: false,
            },
        )
        .await;
    assert!(attempt.is_err());
    assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        graph
            .store()
            .load(fs.workspace_id("budget-child").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    // The old graph's replacement UNBOUNDED allowance reaches the creator on
    // exactly the same input. Retain this causal control through production code.
    let mut reset = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let attempt = graph
        .fork(
            &parent,
            "budget-child",
            IdempotencyKey::from_bytes([95; 16]),
            None,
            &mut reset,
            TestFork {
                parent: &parent,
                destination: "budget-child",
                key: IdempotencyKey::from_bytes([95; 16]),
                cut: None,
                interrupted: None,
                called: Some(&called),
                refuse: true,
            },
        )
        .await;
    assert!(matches!(
        attempt,
        Err(WorkspaceLineageError::UnauthorizedJoin)
    ));
    assert!(called.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn memory_lineage_keeps_payload_on_its_original_ledger_until_replacement_and_drop() {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(256, 64 * 1024),
        &cancellation,
    )
    .unwrap();
    let original_work = &original;
    let mut accounting = original_work.accounting(&cancellation);
    let store = MemoryWorkspaceLineageStore::new();
    let baseline = original.live_allocation_bytes();
    assert_eq!(
        baseline,
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    let record = WorkspaceLineageRecord {
        version: LINEAGE_VERSION,
        revision: 1,
        ready: true,
        workspace_id: workspace("owned-child"),
        workspace_name: "owned-child".into(),
        parent_workspace_id: Some(workspace("owned-parent")),
        parent_workspace_name: Some("owned-parent".into()),
        creation_operation: Some(crate::OperationId::from_bytes([96; 16])),
        fork_generation: generation(1),
        initial_generation: generation(2),
    };
    let bytes =
        (record.workspace_name.len() + record.parent_workspace_name.as_ref().unwrap().len()) as u64;
    let retained = bytes + MemoryWorkspaceLineageStore::node_bytes();
    assert!(
        store
            .compare_accounted(record.workspace_id, 0, &record, &mut accounting)
            .await
            .unwrap()
    );
    assert_eq!(original.live_allocation_bytes(), baseline + retained);
    let loaded = store
        .load_accounted(record.workspace_id, &mut accounting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.live_allocation_bytes(),
        baseline + retained + bytes
    );
    loaded.retire(&mut accounting).unwrap();
    assert_eq!(original.live_allocation_bytes(), baseline + retained);
    // A zero-ownership original cannot make the old adapter's uncharged clone.
    let mut budget = crate::performance::work_budget(256, 64 * 1024);
    budget.peak_allocation_bytes = 0;
    let mut limited = budget.accounting(&cancellation);
    assert!(
        store
            .load_accounted(record.workspace_id, &mut limited)
            .await
            .is_err()
    );
    assert_eq!(
        store.load(record.workspace_id).await.unwrap(),
        Some(record.clone())
    );
    let mut replacement = record.clone();
    replacement.revision += 1;
    assert!(
        store
            .compare_accounted(record.workspace_id, 1, &replacement, &mut accounting)
            .await
            .unwrap()
    );
    assert_eq!(original.live_allocation_bytes(), baseline + retained);
    drop(store);
    assert_eq!(original.live_allocation_bytes(), baseline);
}

struct TestFork<'a, A, O> {
    parent: &'a Workspace<A, O>,
    destination: &'a str,
    key: IdempotencyKey,
    cut: Option<GenerationId>,
    interrupted: Option<tokio::sync::oneshot::Sender<()>>,
    called: Option<&'a std::sync::atomic::AtomicBool>,
    refuse: bool,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore>
    WorkspaceForkCreator<
        A,
        O,
        Workspace<A, O>,
        WorkspaceLineageError<MemoryWorkspaceLineageStoreError>,
    > for TestFork<'_, A, O>
{
    fn workspace(output: &Workspace<A, O>) -> &Workspace<A, O> {
        output
    }

    async fn create(
        self,
        creation: WorkspaceForkCreation<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Workspace<A, O>, WorkspaceLineageError<MemoryWorkspaceLineageStoreError>> {
        if let Some(called) = self.called {
            called.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if self.refuse {
            return Err(WorkspaceLineageError::UnauthorizedJoin);
        }
        if let Some(cut) = self.cut {
            assert_eq!(creation.generation().id(), cut);
        }
        assert_eq!(creation.destination(), self.destination);
        assert_eq!(creation.key(), self.key);
        let child: Result<
            Workspace<A, O>,
            WorkspaceLineageError<MemoryWorkspaceLineageStoreError>,
        > = GenerationFork {
            parent: self.parent,

            permit: crate::PublicationPermit::Unrestricted,
        }
        .create(creation, accounting)
        .await;
        let child = child?;
        if let Some(interrupted) = self.interrupted {
            interrupted.send(()).unwrap();
            std::future::pending::<()>().await;
        }
        Ok(child)
    }
}

/// A storage adapter that schedules one competing publication at the CAS cut.
struct PromotionRace {
    inner: MemoryWorkspaceLineageStore,
    legal_winner: bool,
    attempts: std::sync::atomic::AtomicUsize,
}

impl WorkspaceLineageStore for PromotionRace {
    type Error = MemoryWorkspaceLineageStoreError;

    async fn load(&self, id: WorkspaceId) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.inner.load(id).await
    }

    async fn compare_and_swap(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.inner.compare_and_swap(id, revision, replacement).await
    }

    async fn load_accounted(
        &self,
        id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Owned<WorkspaceLineageRecord>>, WorkspaceLineageError<Self::Error>> {
        self.inner.load_accounted(id, accounting).await
    }

    async fn compare_accounted(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: &WorkspaceLineageRecord,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, WorkspaceLineageError<Self::Error>> {
        let attempt = self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if attempt == 0 {
            let mut winner = work::copy(replacement, accounting)?;
            if !self.legal_winner {
                // A new pending revision is not the sole legal intent -> ready
                // transition. Retrying would silently accept this corruption.
                winner.value.ready = false;
                winner.value.initial_generation = winner.value.fork_generation;
            }
            let published = self
                .inner
                .compare_accounted(id, revision, &winner.value, accounting)
                .await;
            winner.retire(accounting)?;
            assert!(published?);
            return Ok(false);
        }
        self.inner
            .compare_accounted(id, revision, replacement, accounting)
            .await
    }
}

#[tokio::test]
async fn promotion_cas_loser_accepts_only_the_single_legal_terminal_transition() {
    for legal_winner in [false, true] {
        let store = PromotionRace {
            inner: MemoryWorkspaceLineageStore::new(),
            legal_winner,
            attempts: std::sync::atomic::AtomicUsize::new(0),
        };
        let planned = WorkspaceLineageRecord {
            version: LINEAGE_VERSION,
            revision: 1,
            ready: false,
            workspace_id: workspace("promotion-child"),
            workspace_name: "promotion-child".into(),
            parent_workspace_id: Some(workspace("promotion-parent")),
            parent_workspace_name: Some("promotion-parent".into()),
            creation_operation: Some(crate::OperationId::from_bytes([81; 16])),
            fork_generation: generation(1),
            initial_generation: generation(1),
        };
        assert!(
            store
                .inner
                .compare_and_swap(planned.workspace_id, 0, planned.clone())
                .await
                .unwrap()
        );
        let graph = WorkspaceGraph::new(store);
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        let result = work::promote(&graph, &planned, generation(2), &mut accounting).await;
        if legal_winner {
            result.unwrap();
            assert_eq!(
                graph
                    .resolve(planned.workspace_id)
                    .await
                    .unwrap()
                    .initial_generation,
                generation(2)
            );
        } else {
            // Negative control: the old CAS loop makes a second publication and
            // succeeds on precisely this still-pending replacement.
            assert!(matches!(
                result,
                Err(WorkspaceLineageError::ConflictingRegistration)
            ));
            assert!(
                !graph
                    .store()
                    .inner
                    .load(planned.workspace_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .ready
            );
        }
        assert_eq!(
            graph
                .store()
                .attempts
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn lineage_cursor_matches_finite_chains_and_rejects_every_cycle(
        links in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..32)
    ) {
        futures::executor::block_on(async {
            let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
            let id = |index: usize| WorkspaceId::from_bytes((index as u128 + 1).to_le_bytes());
            let mut parents = Vec::new();
            for (index, link) in links.iter().enumerate() {
                let parent = usize::from(*link) % (links.len() + 1);
                let parent = (parent < links.len() && parent != index).then_some(parent);
                parents.push(parent);
                let record = WorkspaceLineageRecord {
                    version: LINEAGE_VERSION, revision: 1, ready: true,
                    workspace_id: id(index), workspace_name: format!("walk-{index}"),
                    parent_workspace_id: parent.map(id),
                    parent_workspace_name: parent.map(|p| format!("walk-{p}")),
                    creation_operation: parent.map(|_| crate::OperationId::from_bytes([1; 16])),
                    fork_generation: generation(1), initial_generation: generation(1),
                };
                assert!(graph.store().compare_and_swap(id(index), 0, record).await.unwrap());
            }
            let mut seen = std::collections::BTreeSet::new();
            let mut next = Some(0);
            let mut cycle = false;
            let mut expected = Vec::new();
            while let Some(index) = next {
                if !seen.insert(index) { cycle = true; break; }
                expected.push(id(index));
                next = parents[index];
            }
            let cancellation = CancellationToken::new();
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
            let depth = u32::try_from(links.len())
                .map_err(|_| proptest::test_runner::TestCaseError::fail("chain depth overflows"))?;
            let mut cursor = graph.walk(id(0), depth + 1);
            let mut observed = Vec::new();
            let outcome = loop {
                match cursor.next_record(&mut accounting).await {
                    Ok(Some(record)) => {
                        observed.push(record.value.workspace_id);
                        record.retire(&mut accounting).unwrap();
                    }
                    Ok(None) => break Ok(()),
                    Err(error) => break Err(error),
                }
                assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
            };
            if cycle {
                proptest::prop_assert!(matches!(outcome, Err(WorkspaceLineageError::TraversalLimit)));
            } else {
                proptest::prop_assert!(outcome.is_ok());
                proptest::prop_assert_eq!(observed, expected);
            }
            proptest::prop_assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
            Ok(())
        })?;
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn lineage_cursor_cannot_copy_a_record_outside_its_original_allocation_budget() {
    let graph = WorkspaceGraph::new(MemoryWorkspaceLineageStore::new());
    let record = WorkspaceLineageRecord {
        version: LINEAGE_VERSION,
        revision: 1,
        ready: true,
        workspace_id: workspace("walk-funded"),
        workspace_name: "walk-funded".into(),
        parent_workspace_id: None,
        parent_workspace_name: None,
        creation_operation: None,
        fork_generation: generation(1),
        initial_generation: generation(1),
    };
    assert!(
        graph
            .store()
            .compare_and_swap(record.workspace_id, 0, record.clone())
            .await
            .unwrap()
    );
    let cancellation = CancellationToken::new();
    let setup =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let operations = setup.receipt().allocation_operations;
    drop(setup);
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: operations,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .unwrap();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = graph
        .walk(record.workspace_id, 1)
        .next_record(&mut accounting)
        .await;
    assert!(
        result.is_err(),
        "the ordinary load path bypassed the original allocation budget"
    );
    assert_eq!(original.receipt().allocation_operations, operations);
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
}
