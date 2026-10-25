//! Immutable project refs remain readable across selected branch views.
use super::*;
use acyclic_fs::{WorkspaceGraph, WorkspaceId, WorkspaceLineageStore};

async fn authorize<S: WorkspaceLineageStore>(
    graph: &WorkspaceGraph<S>,
    root: WorkspaceId,
    owner: WorkspaceId,
    accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
) -> std::result::Result<(), acyclic_fs::WorkspaceLineageError<S::Error>> {
    if owner == root {
        return Ok(());
    }
    let mut cursor = graph.walk(owner, u32::MAX);
    while let Some(record) = cursor.next_record(accounting).await? {
        let found = record.value.workspace_id == root;
        record.retire(accounting)?;
        if found {
            return Ok(());
        }
    }
    Err(acyclic_fs::WorkspaceLineageError::UnauthorizedJoin)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(super) async fn project_generation(
        &self,
        workspace: &Workspace<A, O>,
        generation: GenerationId,
        root: WorkspaceId,
        io: &PreparationIo,
    ) -> Result<Generation<A, O>> {
        let owner = io.sdk(
            self.filesystem
                .generation_workspace_id(
                    generation,
                    workspace.volume_config(),
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
        let related;
        let workspace = if owner == root {
            workspace
        } else {
            let mut accounting = io.accounting(io.cancellation());
            let resolved = async {
                authorize(&self.lineage, root, owner, &mut accounting).await?;
                let record =
                    self.lineage
                        .walk(owner, 1)
                        .next_record(&mut accounting)
                        .await?
                        .ok_or(
                            acyclic_fs::WorkspaceLineageError::<
                                acyclic_fs::WorkspaceLineageStorageError,
                            >::IncompatibleState,
                        )?;
                let opened = io.sdk(
                    self.filesystem
                        .open_workspace(&record.value.workspace_name, io, io.cancellation())
                        .await,
                );
                record.retire(&mut accounting)?;
                opened
            }
            .await;
            related = io.sdk(acyclic_fs::facade::FsWork::finish(
                &io,
                resolved,
                &accounting,
            ))?;
            &related
        };
        io.sdk(
            workspace
                .generation(generation, io, io.cancellation())
                .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::{MemoryWorkspaceLineageStore, WorkspaceLineageRecord};
    use proptest::prelude::*;

    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    #[tokio::test]
    async fn native_project_family_reads_use_the_original_ledger_at_every_allocation_cut()
    -> Result<()> {
        use acyclic_fs::{
            PublicationPermit, WorkBudget, facade::FsWork as _, lineage::GenerationFork,
        };
        let filesystem = acyclic_fs::Fs::memory();
        let cancellation = acyclic_fs::CancellationToken::new();
        let parent = filesystem
            .create_workspace("family-root", WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let store = acyclic_fs::WorkspaceLineageStorage::Memory(Arc::new(
            MemoryWorkspaceLineageStore::new(),
        ));
        let graph = WorkspaceGraph::new(store.clone());
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        graph
            .register_root(&parent, &mut accounting)
            .await?
            .retire(&mut accounting)?;
        let child = graph
            .fork::<_, _, _, Error, _>(
                &parent,
                "family-child",
                acyclic_fs::IdempotencyKey::new(),
                None,
                &mut accounting,
                GenerationFork {
                    parent: &parent,
                    permit: PublicationPermit::Unrestricted,
                },
            )
            .await?;
        let selected = child
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
            .id();
        let provider =
            ProviderRef::new("family".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = FilesystemHost::new(filesystem, &provider, None, store)?
            .with_project_family(parent.id())?;
        let make_work = |budget| {
            acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
        };
        let original = make_work(WorkBudget::UNBOUNDED)?;
        let io = PreparationIo::from_native_work(&original)?;
        let setup = original.receipt().allocation_operations;
        let generation = host.select_generation(&parent, selected, &io).await?;
        assert_eq!(generation.id(), selected);
        let required = original.receipt().allocation_operations - setup;
        assert!(required > 0);
        drop((generation, io));
        assert_eq!(
            original.live_allocation_bytes(),
            acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
        );
        for cut in 0..required {
            let original = make_work(WorkBudget {
                allocation_operations: setup + cut,
                ..WorkBudget::UNBOUNDED
            })?;
            let io = PreparationIo::from_native_work(&original)?;
            let result = host.select_generation(&parent, selected, &io).await;
            assert!(result.is_err(), "cut={cut}/{required}");
            drop((result, io));
            assert_eq!(
                original.live_allocation_bytes(),
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
                "cut={cut}/{required}"
            );
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // A pinned generation is authorized iff its owner is the project root
        // or a proven descendant. Sibling projects never inherit that grant.
        #[test]
        fn project_read_authority_follows_only_durable_ancestry(depth in 0u8..8, foreign in any::<bool>(), missing in any::<bool>(), seed in any::<[u8; 16]>()) {
            futures::executor::block_on(async {
                let store = MemoryWorkspaceLineageStore::new();
                let id = |index: u8| { let mut bytes = seed; bytes[0] = index; WorkspaceId::from_bytes(bytes) };
                let generation = GenerationId::new(Digest::from_bytes([1; 32]));
                for index in 0..=depth + 1 {
                    let parent = (index > 1).then(|| id(index - 1));
                    let record = WorkspaceLineageRecord {
                        version: 2, revision: 1, ready: true,
                        workspace_id: id(index), workspace_name: format!("workspace-{index}"),
                        parent_workspace_id: parent,
                        parent_workspace_name: parent.map(|_| format!("workspace-{}", index - 1)),
                        creation_operation: parent.map(|_| acyclic_fs::OperationId::from_bytes(seed)),
                        fork_generation: generation, initial_generation: generation,
                    };
                    assert!(store.compare_and_swap(id(index), 0, record).await.unwrap());
                }
                let graph = WorkspaceGraph::new(store);
                let owner = if missing { id(9) } else if foreign { id(0) } else { id(depth + 1) };
                let cancellation = acyclic_fs::CancellationToken::new();
                let mut accounting = acyclic_fs::facade::FsWork::accounting(&acyclic_fs::WorkBudget::UNBOUNDED, &cancellation);
                prop_assert_eq!(authorize(&graph, id(1), owner, &mut accounting).await.is_ok(), !foreign && !missing);
                Ok(())
            })?;
        }
    }
}
