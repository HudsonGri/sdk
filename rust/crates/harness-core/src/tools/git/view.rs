//! Git operations over either a source-backed checkout or an SDK volume.
use super::*;

/// The task-selected filesystem view, borrowed for one facade operation.
#[derive(Clone, Copy)]
pub enum View<'a> {
    /// A normal checkout whose unchanged files remain demand-backed.
    Checkout(&'a LocalLazyWorkspace),
    /// An SDK volume, including a child's copy-on-write fork.
    Volume(&'a LocalWorkspace),
}

/// A branch selected by the facade and retained by its task binding.
pub enum OwnedView {
    /// A branch retaining its original native source capability.
    Checkout(LocalLazyWorkspace),
    /// A branch of an SDK volume.
    Volume(LocalWorkspace),
}

impl OwnedView {
    /// Retains a source-backed branch for an ordinary checkout binding.
    pub fn into_checkout(self) -> crate::Result<LocalLazyWorkspace> {
        match self {
            Self::Checkout(view) => Ok(view),
            Self::Volume(_) => Err(Error::Unauthorized(
                "expected a source-backed checkout branch".into(),
            )),
        }
    }

    /// Borrows the selected view without copying its source or contents.
    pub const fn view(&self) -> View<'_> {
        match self {
            Self::Checkout(view) => View::Checkout(view),
            Self::Volume(view) => View::Volume(view),
        }
    }

    /// The authored workspace supplying this view's generations.
    pub const fn workspace(&self) -> &LocalWorkspace {
        self.view().workspace()
    }
}

impl<'a> View<'a> {
    /// The authored workspace supplying this view's generations.
    pub const fn workspace(self) -> &'a LocalWorkspace {
        match self {
            Self::Checkout(view) => view.workspace(),
            Self::Volume(view) => view,
        }
    }

    pub(super) async fn read(
        self,
        path: &str,
        maximum: u64,
    ) -> crate::Result<acyclic_stream::OwnedBytes> {
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        match self {
            Self::Checkout(view) => view
                .read(path, maximum, &mut accounting)
                .await
                .map(|chunk| chunk.bytes)
                .map_err(Error::from),
            Self::Volume(view) => {
                let generation = view
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                Ok(generation
                    .read(path, maximum, WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value)
            }
        }
    }

    pub(super) async fn tree(
        self,
        exact: bool,
        permit: PublicationPermit,
    ) -> crate::Result<GitTreeRef> {
        let cancellation = CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        match self {
            Self::Checkout(view) if exact => {
                let generation = view.exactify("/", permit, &mut accounting).await?;
                Ok(GitTreeRef::exact(view.workspace().id(), generation.id()))
            }
            Self::Checkout(view) => view
                .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&cancellation))
                .await
                .map(GitTreeRef::Lazy)
                .map_err(Error::from),
            Self::Volume(view) => {
                let generation = view
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                Ok(GitTreeRef::exact(view.id(), generation.id()))
            }
        }
    }

    pub(super) async fn fork(
        self,
        destination: &str,
        operation: OperationId,
        permit: PublicationPermit,
        store: &LocalCoreStateStore,
    ) -> crate::Result<OwnedView> {
        let cancellation = CancellationToken::new();
        let key = IdempotencyKey::from_bytes(operation.into_bytes());
        let graph = WorkspaceGraph::new(store.clone());
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        self.fork_accounted(destination, key, permit, &graph, &mut accounting, None)
            .await
    }

    pub(super) async fn fork_accounted<'original>(
        self,
        destination: &str,
        key: IdempotencyKey,
        permit: PublicationPermit,
        graph: &WorkspaceGraph<LocalCoreStateStore>,
        accounting: &mut acyclic_fs::kernel::KernelAccounting<'original>,
        selected: Option<acyclic_fs::LazySnapshotRef>,
    ) -> crate::Result<OwnedView> {
        match self {
            Self::Checkout(view) => {
                let cut = match selected {
                    Some(cut) => cut,
                    None => view.prepare_fork(destination, key, accounting).await?,
                };
                let (child, _) = graph
                    .fork::<_, _, _, Error, _>(
                        view.workspace(),
                        destination,
                        key,
                        Some(cut.authored_generation),
                        accounting,
                        acyclic_fs::lineage::LazyFork {
                            parent: view,
                            destination,
                            cut,
                            key,
                            permit,
                        },
                        |result| result.0.workspace(),
                    )
                    .await?;
                Ok(OwnedView::Checkout(child))
            }
            Self::Volume(view) => graph
                .fork::<_, _, _, Error, _>(
                    view,
                    destination,
                    key,
                    None,
                    accounting,
                    acyclic_fs::lineage::GenerationFork {
                        parent: view,
                        destination,
                        key,
                        permit,
                    },
                    |child| child,
                )
                .await
                .map(OwnedView::Volume),
        }
    }

    pub(super) async fn related(self, workspace: LocalWorkspace) -> crate::Result<OwnedView> {
        match self {
            Self::Checkout(view) => view
                .open_related(
                    workspace,
                    &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                )
                .await
                .map(OwnedView::Checkout)
                .map_err(Error::from),
            Self::Volume(_) => Ok(OwnedView::Volume(workspace)),
        }
    }

    pub(crate) async fn optional(
        self,
        path: &str,
        maximum: u64,
    ) -> crate::Result<Option<acyclic_stream::OwnedBytes>> {
        match self.read(path, maximum).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(Error::NativeSource { source, .. })
                if matches!(*source, acyclic_fs::LazyWorkspaceError::NotFound) =>
            {
                Ok(None)
            }
            Err(Error::NativeWorkspace { source, .. })
                if matches!(*source, WorkspaceError::NotFound) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

impl<'a> From<&'a LocalLazyWorkspace> for View<'a> {
    fn from(value: &'a LocalLazyWorkspace) -> Self {
        Self::Checkout(value)
    }
}

impl<'a> From<&'a LocalWorkspace> for View<'a> {
    fn from(value: &'a LocalWorkspace) -> Self {
        Self::Volume(value)
    }
}

#[cfg(test)]
mod tests;
