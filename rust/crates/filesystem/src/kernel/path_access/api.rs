use super::*;

/// Resolves one exact path without following terminal or intermediate links.
pub async fn lookup_path<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathLookup, PathLookupFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    lookup_path_async_with_accounting(
        store,
        generation,
        path,
        config,
        &mut accounting,
        cancellation,
        None,
    )
    .await
    .map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
}

/// Resolves one path and captures the exact semantic regions needed for a safe rebase.
pub async fn observe_path_async<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ObservedPathLookup, PathLookupFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    observe_path_async_with_accounting(
        store,
        generation,
        path,
        config,
        &mut accounting,
        cancellation,
        None,
        true,
    )
    .await
    .map(|result| result.0)
    .map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
}

/// Resolves a non-empty exact path batch through shared authenticated prefixes.
pub async fn lookup_paths<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathBatchLookup, PathLookupFailure> {
    lookup_path_queries_async(
        store,
        generation,
        PathQueries::Owned(paths),
        PathObservation::None,
        config,
        budget,
        cancellation,
        None,
        PathBatchMode::NoFollow,
    )
    .await
    .map(|observed| observed.lookup)
}

/// Resolves a non-empty no-follow path batch with shared work while capturing, in the same walk, every region [`observe_path_async`] would capture for each path.
pub async fn observe_paths_async<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    paths: &[NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ObservedPathBatch, PathLookupFailure> {
    lookup_path_queries_async(
        store,
        generation,
        PathQueries::Owned(paths),
        PathObservation::RECORDS,
        config,
        budget,
        cancellation,
        None,
        PathBatchMode::NoFollow,
    )
    .await
}

/// Resolves borrowed paths without first cloning their names into an owned batch.
pub async fn lookup_path_refs<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    paths: &[&NamespacePath],
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PathBatchLookup, PathLookupFailure> {
    lookup_path_queries_async(
        store,
        generation,
        PathQueries::Borrowed(paths),
        PathObservation::None,
        config,
        budget,
        cancellation,
        None,
        PathBatchMode::NoFollow,
    )
    .await
    .map(|observed| observed.lookup)
}

/// What a path batch records for a later rebase.
#[derive(Clone, Copy)]
pub(crate) enum PathObservation {
    /// Nothing: a plain read.
    None,
    /// Every name binding and kind. Queries at or after `records_from` also
    /// record their terminal file record, like a single lookup that captures
    /// its terminal. Earlier queries are directory prefixes, so sibling
    /// additions under them stay rebase-safe.
    Paths { records_from: usize },
}

impl PathObservation {
    /// Every query also records its terminal file record.
    pub(crate) const RECORDS: Self = Self::Paths { records_from: 0 };

    pub(super) const fn bindings(self) -> bool {
        matches!(self, Self::Paths { .. })
    }

    pub(super) const fn record(self, index: usize) -> bool {
        matches!(self, Self::Paths { records_from } if index >= records_from)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PathBatchMode {
    NoFollow,
    SelectedRestore,
}

pub(crate) async fn lookup_path_async_with_accounting<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
) -> Result<PathLookup, crate::FsError> {
    let result = resolve_path_with_accounting(
        store,
        generation,
        path,
        config,
        accounting,
        cancellation,
        data_custody,
        false,
        false,
    )
    .await?;
    Ok(result.0.lookup)
}

#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub(crate) async fn observe_path_async_with_accounting<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    path: &NamespacePath,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    capture_terminal: bool,
) -> Result<(ObservedPathLookup, KernelOwnedAllocation), crate::FsError> {
    resolve_path_with_accounting(
        store,
        generation,
        path,
        config,
        accounting,
        cancellation,
        data_custody,
        true,
        capture_terminal,
    )
    .await
}
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub(crate) async fn lookup_paths_with_accounting<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    paths: PathQueries<'_>,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    custody: Option<&crate::kernel::ProofDataCustody>,
    mode: PathBatchMode,
) -> Result<(PathBatchLookup, KernelOwnedAllocation), crate::FsError> {
    let (value, allocation) = path_queries_with_accounting(
        store,
        generation,
        paths,
        PathObservation::None,
        config,
        accounting,
        cancellation,
        custody,
        mode,
    )
    .await?;
    Ok((value.lookup, allocation))
}

#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub(super) async fn lookup_path_queries_async<S: AsyncObjectStore>(
    store: &S,
    generation: &crate::kernel::GenerationRoot,
    paths: PathQueries<'_>,
    observation: PathObservation,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    data_custody: Option<&crate::kernel::ProofDataCustody>,
    mode: PathBatchMode,
) -> Result<ObservedPathBatch, PathLookupFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    path_queries_with_accounting(
        store,
        generation,
        paths,
        observation,
        config,
        &mut accounting,
        cancellation,
        data_custody,
        mode,
    )
    .await
    .map(|result| result.0)
    .map_err(|error| OperationFailure::new(path_error(error), accounting.spent()))
}
