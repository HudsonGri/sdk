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

impl<'a, A, O> GenerationFork<'a, A, O> {
    /// Retains the issued publication guard and optional inherited paths inside
    /// the graph's creation phase. Recovery still receives the graph's original
    /// generation; the caller cannot substitute a newer parent head.
    pub fn guarded<G>(
        self,
        guard: G,
        paths: Option<Vec<String>>,
    ) -> GuardedGenerationFork<'a, A, O, G> {
        GuardedGenerationFork {
            fork: self,
            guard,
            paths,
        }
    }
}

/// A graph creator holding the original authority guard through publication.
pub struct GuardedGenerationFork<'a, A, O, G> {
    fork: GenerationFork<'a, A, O>,
    guard: G,
    paths: Option<Vec<String>>,
}

async fn create_generation<A, O, G, E>(
    fork: GenerationFork<'_, A, O>,
    generation: crate::Generation<A, O>,
    paths: Option<Vec<String>>,
    guard: G,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Workspace<A, O>, E>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    G: crate::AuthorityGuard,
    G::Error: Into<E>,
    E: From<crate::FsError> + From<WorkspaceError> + Send,
{
    let options = crate::ForkOptions::from_generation(generation, fork.key);
    let options = match paths {
        Some(paths) => options.inherit_paths(paths),
        None => options,
    };
    crate::accounted_fs!(accounting, E, |work, cancellation| fork
        .parent
        .fork::<_, _, E>(
            fork.destination,
            options,
            fork.permit,
            work,
            cancellation,
            guard,
        ))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, E: From<WorkspaceError>>
    WorkspaceForkCreator<A, O, Workspace<A, O>, E> for GenerationFork<'_, A, O>
{
    async fn create(
        self,
        generation: crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Workspace<A, O>, E> {
        create_generation::<_, _, _, WorkspaceError>(self, generation, None, (), accounting)
            .await
            .map_err(E::from)
    }
}

impl<A, O, G, E> WorkspaceForkCreator<A, O, Workspace<A, O>, E>
    for GuardedGenerationFork<'_, A, O, G>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    G: crate::AuthorityGuard,
    G::Error: Into<E>,
    E: From<crate::FsError> + From<WorkspaceError> + Send,
{
    async fn create(
        self,
        generation: crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Workspace<A, O>, E> {
        create_generation(self.fork, generation, self.paths, self.guard, accounting).await
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
