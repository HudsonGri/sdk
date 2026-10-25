use super::*;
use acyclic_stream::ProviderTask;

/// Immutable creation inputs issued only after the graph records its intent.
/// Canonical creators consume the graph's destination and operation identity
/// instead of accepting a second, independently supplied set of bindings.
///
/// ```compile_fail
/// use acyclic_fs::{Generation, IdempotencyKey};
/// use acyclic_fs::lineage::WorkspaceForkCreation;
/// fn forge<A, O>(generation: Generation<A, O>) {
///     let _ = WorkspaceForkCreation {
///         generation,
///         destination: "unregistered-child",
///         key: IdempotencyKey::new(),
///     };
/// }
/// ```
pub struct WorkspaceForkCreation<'a, A, O> {
    pub(super) generation: crate::Generation<A, O>,
    pub(super) destination: &'a str,
    pub(super) key: IdempotencyKey,
}

impl<'a, A, O> WorkspaceForkCreation<'a, A, O> {
    /// Borrows the graph's original authenticated parent generation.
    pub const fn generation(&self) -> &crate::Generation<A, O> {
        &self.generation
    }

    /// Borrows the exact destination recorded in the durable intent.
    pub const fn destination(&self) -> &'a str {
        self.destination
    }

    /// Returns the exact operation identity recorded in the durable intent.
    pub const fn key(&self) -> IdempotencyKey {
        self.key
    }

    /// Moves the generation and its immutable creation bindings together.
    pub fn into_parts(self) -> (crate::Generation<A, O>, &'a str, IdempotencyKey) {
        (self.generation, self.destination, self.key)
    }
}

/// The physical creation phase inside a durable graph fork.
///
/// Implementations borrow the original accounting for the complete future.
/// The explicit future contract also permits native hosts to move a fork task
/// between executor threads without constraining an async closure's lifetimes.
/// The creator itself must identify its output workspace; a caller cannot
/// supply an independent output selector after creation.
///
/// ```compile_fail
/// use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Workspace, WorkspaceError};
/// use acyclic_fs::kernel::KernelAccounting;
/// use acyclic_fs::lineage::{WorkspaceForkCreation, WorkspaceForkCreator};
/// struct Creator;
/// impl<A: AsyncAuthorityStore, O: AsyncObjectStore>
///     WorkspaceForkCreator<A, O, Workspace<A, O>, WorkspaceError> for Creator
/// {
///     async fn create(
///         self,
///         _creation: WorkspaceForkCreation<'_, A, O>,
///         _accounting: &mut KernelAccounting<'_>,
///     ) -> Result<Workspace<A, O>, WorkspaceError> {
///         Err(WorkspaceError::IncompatibleWorkspace)
///     }
/// }
/// ```
pub trait WorkspaceForkCreator<A: AsyncAuthorityStore, O: AsyncObjectStore, T, E> {
    /// Borrows the created workspace that must match the graph's durable intent.
    fn workspace(output: &T) -> &Workspace<A, O>;

    /// Creates or reopens the child from the graph's immutable parent cut.
    fn create(
        self,
        creation: WorkspaceForkCreation<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<T, E>> + ProviderTask;
}

/// Canonical creator for a workspace backed by an authored generation.
pub struct GenerationFork<'a, A, O> {
    /// Parent authenticated by the graph.
    pub parent: &'a Workspace<A, O>,
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
    creation: WorkspaceForkCreation<'_, A, O>,
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
    let (generation, destination, key) = creation.into_parts();
    let options = crate::ForkOptions::from_generation(generation, key);
    let options = match paths {
        Some(paths) => options.inherit_paths(paths),
        None => options,
    };
    crate::accounted_fs!(accounting, E, |work, cancellation| fork
        .parent
        .fork::<_, _, E>(
            destination,
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
    fn workspace(output: &Workspace<A, O>) -> &Workspace<A, O> {
        output
    }

    async fn create(
        self,
        creation: WorkspaceForkCreation<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Workspace<A, O>, E> {
        create_generation::<_, _, _, WorkspaceError>(self, creation, None, (), accounting)
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
    fn workspace(output: &Workspace<A, O>) -> &Workspace<A, O> {
        output
    }

    async fn create(
        self,
        creation: WorkspaceForkCreation<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Workspace<A, O>, E> {
        create_generation(self.fork, creation, self.paths, self.guard, accounting).await
    }
}

/// Canonical creator retaining the source and overlay of a sparse workspace.
pub struct LazyFork<'a, A, O, D, S> {
    /// Parent sparse view authenticated by the graph.
    pub parent: &'a crate::LazyWorkspace<A, O, D, S>,
    /// Persisted parent cut, including its source and overlay.
    pub cut: crate::LazySnapshotRef,
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
    fn workspace(
        output: &(crate::LazyWorkspace<A, O, D, S>, crate::LazySnapshotRef),
    ) -> &Workspace<A, O> {
        output.0.workspace()
    }

    async fn create(
        self,
        creation: WorkspaceForkCreation<'_, A, O>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(crate::LazyWorkspace<A, O, D, S>, crate::LazySnapshotRef), E> {
        let (generation, destination, key) = creation.into_parts();
        if generation.workspace_id() != self.cut.workspace_id
            || generation.id() != self.cut.authored_generation
        {
            return Err(crate::LazyWorkspaceError::OriginalSnapshotUnavailable.into());
        }
        self.parent
            .fork_snapshot(destination, self.cut, key, self.permit, accounting)
            .await
            .map_err(E::from)
    }
}
