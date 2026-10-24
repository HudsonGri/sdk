use super::*;
use crate::kernel::KernelOwnedAllocation;
use crate::ordered_tree;
use crate::sync::Mutex;

type Tree = ordered_tree::Tree<Node>;

// Rotations move pointers without merging different invocations' custody.
// Field order frees the physical box before releasing its allocation credit.
struct Pointer {
    value: Box<Node>,
    _allocation: KernelOwnedAllocation,
}

impl std::ops::Deref for Pointer {
    type Target = Node;
    fn deref(&self) -> &Node {
        &self.value
    }
}
impl std::ops::DerefMut for Pointer {
    fn deref_mut(&mut self) -> &mut Node {
        &mut self.value
    }
}

struct Node {
    record: Owned<WorkspaceLineageRecord>,
    left: Tree,
    right: Tree,
    height: usize,
}

impl ordered_tree::Node for Node {
    type Key = WorkspaceId;
    type Pointer = Pointer;
    fn key(&self) -> &WorkspaceId {
        &self.record.value.workspace_id
    }
    fn left(&self) -> &Tree {
        &self.left
    }
    fn left_mut(&mut self) -> &mut Tree {
        &mut self.left
    }
    fn right(&self) -> &Tree {
        &self.right
    }
    fn right_mut(&mut self) -> &mut Tree {
        &mut self.right
    }
    fn height(&self) -> usize {
        self.height
    }
    fn refresh(&mut self) {
        self.height = 1 + ordered_tree::height::<Self>(&self.left)
            .max(ordered_tree::height::<Self>(&self.right));
    }
}

fn pointer(record: Owned<WorkspaceLineageRecord>, allocation: KernelOwnedAllocation) -> Pointer {
    Pointer {
        value: Box::new(Node {
            record,
            left: None,
            right: None,
            height: 1,
        }),
        _allocation: allocation,
    }
}

/// Process-local lineage adapter sharing the policy registry's balanced index.
/// Every node has an explicit box layout and one original allocation owner.
#[derive(Default)]
pub struct MemoryWorkspaceLineageStore {
    records: Mutex<Tree>,
}

impl MemoryWorkspaceLineageStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(super) const fn node_bytes() -> u64 {
        std::mem::size_of::<Node>() as u64
    }
}

/// Process-local lineage synchronization failure.
#[derive(Debug, Error)]
#[error("workspace-lineage memory store is unavailable")]
pub struct MemoryWorkspaceLineageStoreError;

impl WorkspaceLineageStore for MemoryWorkspaceLineageStore {
    type Error = MemoryWorkspaceLineageStoreError;

    async fn load(&self, id: WorkspaceId) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        let records = self
            .records
            .lock()
            .map_err(|_| MemoryWorkspaceLineageStoreError)?;
        Ok(
            ordered_tree::find_with::<Node, Self::Error>(&records, &id, || Ok(()))?
                .map(|node| node.record.value.clone()),
        )
    }

    async fn compare_and_swap(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MemoryWorkspaceLineageStoreError)?;
        let previous = ordered_tree::find_with::<Node, Self::Error>(&records, &id, || Ok(()))?;
        if replacement.workspace_id != id
            || previous.map_or(0, |node| node.record.value.revision) != revision
        {
            return Ok(false);
        }
        let bytes = replacement.workspace_name.capacity() as u64
            + replacement
                .parent_workspace_name
                .as_ref()
                .map_or(0, |name| name.capacity() as u64);
        let node = pointer(
            Owned::new(replacement, KernelOwnedAllocation::Standalone, bytes),
            KernelOwnedAllocation::Standalone,
        );
        let (root, previous) = ordered_tree::insert::<Node>(records.take(), node);
        *records = Some(root);
        drop(previous);
        Ok(true)
    }

    async fn load_accounted(
        &self,
        id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Owned<WorkspaceLineageRecord>>, WorkspaceLineageError<Self::Error>> {
        accounting.check()?;
        let records = self
            .records
            .lock()
            .map_err(|_| WorkspaceLineageError::Store(MemoryWorkspaceLineageStoreError))?;
        ordered_tree::find_with::<Node, crate::FsError>(&records, &id, || {
            accounting.charge_items(1)
        })?
        .map(|node| work::copy(&node.record.value, accounting))
        .transpose()
    }

    async fn compare_accounted(
        &self,
        id: WorkspaceId,
        revision: u64,
        replacement: &WorkspaceLineageRecord,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, WorkspaceLineageError<Self::Error>> {
        accounting.check()?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| WorkspaceLineageError::Store(MemoryWorkspaceLineageStoreError))?;
        let mut probes = 0;
        let previous = ordered_tree::find_with::<Node, crate::FsError>(&records, &id, || {
            accounting.charge_items(1)?;
            probes += 1;
            Ok(())
        })?;
        if replacement.workspace_id != id
            || previous.map_or(0, |node| node.record.value.revision) != revision
        {
            return Ok(false);
        }
        let record = work::copy(replacement, accounting)?;
        let allocation = match accounting.claim_owned(std::mem::size_of::<Node>() as u64, 1) {
            Ok(allocation) => allocation,
            Err(error) => {
                record.retire(accounting)?;
                return Err(error.into());
            }
        };
        let mut pending = Some(pointer(record, allocation));
        // The same locked key follows exactly the path just observed. Admit
        // those comparisons before changing links, then record actual work.
        let inserted = accounting.perform(
            crate::WorkCounters {
                items_examined: probes,
                ..crate::WorkCounters::UNCHARGED
            },
            || {
                let node = pending
                    .take()
                    .ok_or(WorkspaceLineageError::IncompatibleState)?;
                let (root, previous) = ordered_tree::insert::<Node>(records.take(), node);
                *records = Some(root);
                drop(previous);
                Ok::<_, WorkspaceLineageError<Self::Error>>(())
            },
        );
        if let Some(Pointer {
            value,
            _allocation: mut allocation,
        }) = pending
        {
            let Node { record, .. } = *value;
            record.retire(accounting)?;
            accounting.release_owned(&mut allocation, std::mem::size_of::<Node>() as u64)?;
        }
        inserted??;
        Ok(true)
    }
}

#[cfg(all(test, not(target_arch = "wasm32"), not(acyclic_fs_loom)))]
#[path = "memory_tests.rs"]
mod tests;
