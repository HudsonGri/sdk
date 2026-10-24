use super::*;
use acyclic_stream::ProviderTask;

/// The physical creation phase inside a durable graph fork.
///
/// Implementations borrow the original accounting for the complete future.
/// The explicit future contract also permits native hosts to move a fork task
/// between executor threads without constraining an async closure's lifetimes.
pub trait WorkspaceForkCreator<A: AsyncAuthorityStore, O: AsyncObjectStore, T, E> {
    /// Creates or reopens the child from the graph's immutable parent cut.
    fn create(
        self,
        generation: crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<T, E>> + ProviderTask;
}

/// Canonical creator for a workspace backed by an authored generation.
pub struct GenerationFork<'a, A, O> {
    /// Parent authenticated by the graph.
    pub parent: &'a Workspace<A, O>,
    /// Canonical child name.
    pub destination: &'a str,
    /// Stable creation identity.
    pub key: IdempotencyKey,
    /// Authority used for physical publication.
    pub permit: crate::PublicationPermit,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, E: From<WorkspaceError>>
    WorkspaceForkCreator<A, O, Workspace<A, O>, E> for GenerationFork<'_, A, O>
{
    async fn create(
        self,
        generation: crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Workspace<A, O>, E> {
        crate::accounted_fs!(accounting, WorkspaceError, |work, cancellation| self
            .parent
            .fork::<_, _, WorkspaceError>(
                self.destination,
                crate::ForkOptions::from_generation(generation, self.key),
                self.permit,
                work,
                cancellation,
                (),
            ))
        .map_err(E::from)
    }
}

/// Canonical creator retaining the source and overlay of a sparse workspace.
pub struct LazyFork<'a, A, O, D, S> {
    /// Parent sparse view authenticated by the graph.
    pub parent: &'a crate::LazyWorkspace<A, O, D, S>,
    /// Canonical child name.
    pub destination: &'a str,
    /// Persisted parent cut, including its source and overlay.
    pub cut: crate::LazySnapshotRef,
    /// Stable creation identity.
    pub key: IdempotencyKey,
    /// Authority used for physical publication.
    pub permit: crate::PublicationPermit,
}

impl<A, O, D, S, E>
    WorkspaceForkCreator<A, O, (crate::LazyWorkspace<A, O, D, S>, crate::LazySnapshotRef), E>
    for LazyFork<'_, A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: crate::demand::DemandSource + 'static,
    S: crate::LazyWorkspaceStore,
    E: From<crate::LazyWorkspaceError>,
{
    async fn create(
        self,
        generation: crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(crate::LazyWorkspace<A, O, D, S>, crate::LazySnapshotRef), E> {
        if generation.workspace_id() != self.cut.workspace_id
            || generation.id() != self.cut.authored_generation
        {
            return Err(crate::LazyWorkspaceError::OriginalSnapshotUnavailable.into());
        }
        self.parent
            .fork_snapshot(
                self.destination,
                self.cut,
                self.key,
                self.permit,
                accounting,
            )
            .await
            .map_err(E::from)
    }
}
