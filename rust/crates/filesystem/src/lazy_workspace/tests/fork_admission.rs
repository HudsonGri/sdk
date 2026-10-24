use super::*;

#[derive(Clone, Default)]
struct CompetingForkStore {
    inner: MemoryLazyWorkspaceStore,
    phase: Arc<AtomicUsize>,
    outcome: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
}

impl LazyWorkspaceStore for CompetingForkStore {
    async fn load_lazy_workspace(
        &self,
        id: WorkspaceId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazyWorkspaceState>, LazyWorkspaceError> {
        self.inner.load_lazy_workspace(id, ctx).await
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
        mut state: LazyWorkspaceState,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let phase = state.fork_origin.map_or(0, |origin| match origin.phase {
            LazyForkPhase::Prepared => 1,
            LazyForkPhase::Committed => 2,
        });
        if phase != 0 && self.phase.load(Ordering::SeqCst) == phase {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                match self.outcome.load(Ordering::SeqCst) {
                    0 => {}
                    // No create winner, or a commit CAS with no transition.
                    1 => return Ok(false),
                    // An advanced pending revision is not a committed winner.
                    2 => state.fork_origin.as_mut().unwrap().phase = LazyForkPhase::Prepared,
                    // A different operation must never be adopted.
                    3 => {
                        state.fork_origin.as_mut().unwrap().operation_id =
                            crate::OperationId::from_bytes([0xff; 16]);
                    }
                    _ => unreachable!(),
                }
                assert!(
                    self.inner
                        .compare_and_swap_lazy_workspace(id, revision, state, ctx)
                        .await?
                );
                return Ok(false);
            }
        }
        self.inner
            .compare_and_swap_lazy_workspace(id, revision, state, ctx)
            .await
    }

    async fn load_lazy_overlay(
        &self,
        id: LazyOverlayId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyOverlay, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.inner.load_lazy_overlay(id, ctx).await
    }

    async fn load_lazy_shadow(
        &self,
        id: LazyShadowId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<(LazyShadow, KernelOwnedAllocation)>, LazyWorkspaceError> {
        self.inner.load_lazy_shadow(id, ctx).await
    }

    async fn put_lazy_overlay(
        &self,
        id: LazyOverlayId,
        value: (LazyOverlay, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.inner.put_lazy_overlay(id, value, ctx).await
    }

    async fn put_lazy_shadow(
        &self,
        id: LazyShadowId,
        value: (LazyShadow, KernelOwnedAllocation),
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        self.inner.put_lazy_shadow(id, value, ctx).await
    }
}

#[tokio::test]
async fn sparse_fork_cas_loser_authenticates_one_winner_without_republishing() {
    for native in [false, true] {
        for graph_host in [false, true] {
            for phase in [1, 2] {
                for outcome in [0, 1, 2, 3] {
                    let fs = Fs::memory();
                    let store = CompetingForkStore::default();
                    let root = attach(
                        &fs,
                        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
                        store.clone(),
                    )
                    .await
                    .unwrap();
                    let key = IdempotencyKey::from_bytes([0x42; 16]);
                    let cancellation = CancellationToken::new();
                    let mut ctx =
                        KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
                    #[cfg(not(target_arch = "wasm32"))]
                    let original = native.then(|| {
                        crate::NativeMaterializationWork::new(
                            crate::performance::BOUNDARY_BUDGET,
                            &cancellation,
                        )
                        .unwrap()
                    });
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(original) = &original {
                        ctx = KernelAccounting::native(original, &cancellation);
                    }
                    let cut = root.snapshot(&mut ctx).await.unwrap();
                    let graph =
                        crate::WorkspaceGraph::new(crate::MemoryWorkspaceLineageStore::new());
                    graph
                        .register_root(
                            root.workspace(),
                            &mut crate::kernel::KernelAccounting::standalone(
                                crate::WorkBudget::UNBOUNDED,
                                &crate::CancellationToken::new(),
                            ),
                        )
                        .await
                        .unwrap();
                    if phase == 2 {
                        root.prepare_fork_snapshot("child", cut, key.operation_id(), &mut ctx)
                            .await
                            .unwrap();
                    }
                    store.outcome.store(outcome, Ordering::SeqCst);
                    store.phase.store(phase, Ordering::SeqCst);
                    let result: Result<_, Box<dyn std::error::Error + Send + Sync>> = if graph_host
                    {
                        graph
                            .fork(
                                root.workspace(),
                                "child",
                                key,
                                Some(cut.authored_generation),
                                &mut ctx,
                                crate::lineage::LazyFork {
                                    parent: &root,
                                    cut,
                                    permit: crate::PublicationPermit::Unrestricted,
                                },
                                |result| result.0.workspace(),
                            )
                            .await
                    } else {
                        root.fork_snapshot(
                            "child",
                            cut,
                            key,
                            crate::PublicationPermit::Unrestricted,
                            &mut ctx,
                        )
                        .await
                        .map_err(Into::into)
                    };
                    // The create winner may already be prepared. A commit winner
                    // must have made the sole legal Prepared -> Committed change.
                    let accepted = outcome == 0 || (phase == 1 && outcome == 2);
                    assert_eq!(
                        result.is_ok(),
                        accepted,
                        "native={native}, phase={phase}, outcome={outcome}"
                    );
                    assert_eq!(
                        store.attempts.load(Ordering::SeqCst),
                        1,
                        "a CAS loser must authenticate its winner without another write"
                    );
                    if !accepted {
                        assert!(matches!(
                            result.err().unwrap().downcast_ref::<LazyWorkspaceError>(),
                            Some(LazyWorkspaceError::Concurrent)
                        ));
                    }
                }
            }
        }
    }
}
