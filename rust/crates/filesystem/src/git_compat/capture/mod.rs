use super::*;

/// Captures an ignore-aware tree without changing the live workspace.
pub async fn capture_git_compatible_generation<A, O>(
    workspace: &Workspace<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let cancellation = CancellationToken::new();
    // Unmetered: git capture runs on unbounded budgets and holds no work ledger.
    let live = crate::heap_future::erase(crate::heap_future::in_heap_unmetered(|| {
        workspace.head(WorkBudget::UNBOUNDED, &cancellation)
    }))
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    // Unmetered: git capture runs on unbounded budgets and holds no work ledger.
    crate::heap_future::erase(crate::heap_future::in_heap_unmetered(|| {
        capture_git_compatible_generation_from(live, policy, already_tracked, operation_id)
    }))
    .await
}

/// Captures an ignore-aware tree from one exact immutable generation.
pub async fn capture_git_compatible_generation_at<A, O>(
    workspace: &Workspace<A, O>,
    generation: GenerationId,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let cancellation = CancellationToken::new();
    // Unmetered: git capture runs on unbounded budgets and holds no work ledger.
    let live = crate::heap_future::erase(crate::heap_future::in_heap_unmetered(|| {
        workspace.generation(generation, WorkBudget::UNBOUNDED, &cancellation)
    }))
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    // Unmetered: git capture runs on unbounded budgets and holds no work ledger.
    crate::heap_future::erase(crate::heap_future::in_heap_unmetered(|| {
        capture_git_compatible_generation_from(live, policy, already_tracked, operation_id)
    }))
    .await
}

mod proof;
pub use proof::*;
mod traversal;
pub(crate) use traversal::*;
mod full;
use full::*;
mod incremental;
pub use incremental::*;
mod apply;
use apply::*;
