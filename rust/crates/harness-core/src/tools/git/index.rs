//! One producer per opened namespace; clients retain only a page and their top-k.
//! Notifications wake readers, but index files are the authoritative snapshot.
use super::*;
use acyclic_fs::{NameIndex, NameIndexManifest, NameIndexRefresh};
use acyclic_stream::StreamProvider as _;
use futures::StreamExt as _;
use std::sync::{OnceLock, Weak};
use tokio::sync::watch;

type Index = NameIndex<LocalAuthorityBackend, LocalObjectBackend>;
// Workspace IDs are scoped to their authority store. Two independent local
// stores can both contain a volume named `main` with the same opaque ID.
type Registry = BTreeMap<(usize, acyclic_fs::WorkspaceId), Arc<AsyncMutex<Weak<Shared>>>>;
static INDEXES: OnceLock<Mutex<Registry>> = OnceLock::new();

struct Shared {
    latest: AsyncMutex<Option<Index>>,
    changed: watch::Sender<u64>,
    failure: Mutex<Option<String>>,
    refresh: tokio::sync::Notify,
}

/// A bounded cursor following the shared, file-backed name index.
pub struct IndexedNames {
    shared: Arc<Shared>,
    changed: watch::Receiver<u64>,
    next: u64,
    cut: Option<acyclic_fs::NameCut>,
}

/// One bounded page, or an explicit replacement cut after namespace changes.
pub struct NamePage {
    /// Clears the query's previous candidate fold before accepting more pages.
    pub reset: bool,
    /// Newly available metadata names at this cut.
    pub names: Vec<SearchName>,
}

impl IndexedNames {
    /// Follows existing index pages immediately while one producer fills the
    /// unindexed remainder. Closing one query cannot cancel the shared producer.
    pub async fn open(view: View<'_>) -> crate::Result<Self> {
        let workspace = view.workspace();
        let (provider, _) = workspace.authority_records().map_err(storage_index)?;
        let identity = (Arc::as_ptr(&provider) as usize, workspace.id());
        let entry = {
            let mut registry = INDEXES
                .get_or_init(|| Mutex::new(BTreeMap::new()))
                .lock()
                .map_err(|_| Error::Storage("pathname index registry poisoned".into()))?;
            Arc::clone(
                registry
                    .entry(identity)
                    .or_insert_with(|| Arc::new(AsyncMutex::new(Weak::new()))),
            )
        };
        let mut entry = entry.lock().await;
        if let Some(shared) = entry.upgrade() {
            let mut latest = shared.latest.lock().await;
            let valid = if let Some(index) = latest.as_ref() {
                reusable(view, index).await?
            } else {
                true
            };
            if !valid {
                *latest = None;
                shared.refresh.notify_one();
            }
            drop(latest);
            return Ok(Self {
                changed: shared.changed.subscribe(),
                shared,
                next: 0,
                cut: None,
            });
        }
        let owned = match view {
            View::Checkout(lazy) => OwnedView::Checkout(
                lazy.open_related(
                    workspace.clone(),
                    &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                )
                .await?,
            ),
            View::Volume(workspace) => OwnedView::Volume(workspace.clone()),
        };
        let existing = Index::open(workspace)
            .await
            .map_err(super::search::search_error)?;
        let existing = if let Some(index) = existing {
            if reusable(view, &index).await? {
                Some(index)
            } else {
                None
            }
        } else {
            None
        };
        let (changed, receiver) = watch::channel(0);
        let shared = Arc::new(Shared {
            latest: AsyncMutex::new(existing),
            changed,
            failure: Mutex::new(None),
            refresh: tokio::sync::Notify::new(),
        });
        *entry = Arc::downgrade(&shared);
        let producer = Arc::clone(&shared);
        tokio::spawn(async move {
            if let Err(error) = maintain(&owned, &producer).await {
                if let Ok(mut failure) = producer.failure.lock() {
                    *failure = Some(error.to_string());
                }
                producer.changed.send_modify(|revision| *revision += 1);
            }
        });
        Ok(Self {
            shared,
            changed: receiver,
            next: 0,
            cut: None,
        })
    }

    /// Reads at most one committed page; awaits publication if indexing is
    /// incomplete. `None` is returned only for a completed manifest.
    pub async fn next_page(&mut self) -> crate::Result<Option<NamePage>> {
        loop {
            self.changed.borrow_and_update();
            if let Some(message) = self
                .shared
                .failure
                .lock()
                .map_err(|_| Error::Storage("index failure lock poisoned".into()))?
                .clone()
            {
                return Err(Error::Storage(message));
            }
            let latest = self.shared.latest.lock().await;
            if let Some(index) = latest.as_ref() {
                if self.cut.as_ref() != Some(&index.manifest.cut) {
                    self.cut = Some(index.manifest.cut.clone());
                    self.next = 0;
                    return Ok(Some(NamePage {
                        reset: true,
                        names: Vec::new(),
                    }));
                }
                if self.next < index.manifest.pages {
                    let page = index
                        .page(self.next)
                        .await
                        .map_err(super::search::search_error)?;
                    self.next += 1;
                    return Ok(Some(NamePage {
                        reset: false,
                        names: page,
                    }));
                }
                if index.manifest.complete {
                    return Ok(None);
                }
            }
            drop(latest);
            self.changed.changed().await.map_err(storage_index)?;
        }
    }

    /// Unfinished directory count in the latest followed manifest.
    pub async fn pending_directories(&self) -> usize {
        self.shared
            .latest
            .lock()
            .await
            .as_ref()
            .map_or(1, |index| index.manifest.pending_directories)
    }

    /// Waits for an ordinary index-file revision after a terminal page. The
    /// cursor remains reusable; the next call to `next_page` emits a reset if
    /// the indexed namespace changed. No source-directory polling is involved.
    pub async fn changed(&mut self) -> crate::Result<()> {
        self.changed.changed().await.map_err(storage_index)
    }

    /// Whether the current names describe the caller's pinned content tree.
    /// Index-only transactions may advance authored generations, but source,
    /// overlay, shadow, and public pathname changes cannot be substituted.
    pub async fn matches_tree(&self, tree: GitTreeRef) -> crate::Result<bool> {
        let latest = self.shared.latest.lock().await;
        let Some(index) = latest.as_ref() else {
            return Ok(false);
        };
        let (requested, indexed) = match (tree, &index.manifest.cut) {
            (GitTreeRef::Lazy(requested), acyclic_fs::NameCut::Lazy(indexed))
                if requested.workspace_id == indexed.workspace_id
                    && requested.source == indexed.source
                    && requested.overlay == indexed.overlay
                    && requested.shadows == indexed.shadows =>
            {
                (requested.authored_generation, indexed.authored_generation)
            }
            (
                GitTreeRef::Exact(requested),
                acyclic_fs::NameCut::Exact {
                    workspace,
                    generation,
                },
            ) if requested.workspace_id == *workspace => (requested.generation, *generation),
            _ => return Ok(false),
        };
        if requested == indexed {
            return Ok(true);
        }
        let workspace = index.generation().workspace();
        let requested = workspace
            .generation(requested, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let observed = workspace
            .generation(
                index.manifest.observed_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        only_index_changes(&observed, &requested).await
    }
}

fn storage_index(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

async fn maintain(view: &OwnedView, shared: &Shared) -> crate::Result<()> {
    let mut native = match view {
        OwnedView::Checkout(lazy) => {
            let cancellation = CancellationToken::new();
            let work =
                acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            lazy.follow_native_names(&mut acyclic_fs::kernel::KernelAccounting::native(
                &work,
                &cancellation,
            ))?
        }
        OwnedView::Volume(_) => None,
    };
    let mut native_delta = None;
    let (provider, path) = view
        .workspace()
        .authority_records()
        .map_err(storage_index)?;
    loop {
        // Establish the follow before reading the head. Commits between this
        // watermark and the snapshot are harmless duplicates; commits after
        // the snapshot cannot fall into a subscribe-after-read gap.
        let watermark = provider
            .watermarks(path.clone())
            .await
            .map_err(storage_index)?;
        let mut records = provider
            .follow(path.clone(), watermark.accepted)
            .await
            .map_err(storage_index)?;
        let published = Index::open(view.workspace())
            .await
            .map_err(super::search::search_error)?;
        let mut policy = None;
        let published = match published {
            Some(index) if reusable(view.view(), &index).await? => Some(index),
            Some(index) => {
                // The shared index has one volume-owned policy. Query callers
                // cannot replace it, and policy edits invalidate the old fold.
                let ignore = super::search::search_ignore(view.view()).await?;
                let cut = match view.view() {
                    View::Checkout(lazy) => acyclic_fs::NameCut::Lazy(
                        lazy.snapshot(
                            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                        )
                        .await?,
                    ),
                    View::Volume(workspace) => acyclic_fs::NameCut::Exact {
                        workspace: workspace.id(),
                        generation: workspace
                            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                            .await
                            .map_err(|failure| failure.error)?
                            .value
                            .id(),
                    },
                };
                let outcome = match (native_delta.take(), view.view(), cut) {
                    (Some(delta), View::Checkout(lazy), acyclic_fs::NameCut::Lazy(snapshot)) => {
                        index.refresh_native(lazy, snapshot, delta, &ignore).await
                    }
                    (_, _, cut) => index.refresh_authored(view.workspace(), cut, &ignore).await,
                }
                .map_err(super::search::search_error)?;
                policy = Some(ignore);
                match outcome {
                    NameIndexRefresh::Published(index) => Some(index),
                    NameIndexRefresh::Conflict => continue,
                    NameIndexRefresh::Ineligible => None,
                }
            }
            None => None,
        };
        let needs_fill = published
            .as_ref()
            .is_none_or(|index| !index.manifest.complete);
        {
            let mut latest = shared.latest.lock().await;
            let changed = latest.as_ref().map(|index| index.generation().id())
                != published.as_ref().map(|index| index.generation().id());
            *latest = published;
            if changed {
                shared.changed.send_modify(|revision| *revision += 1);
            }
        }
        if needs_fill {
            let ignore = match policy {
                Some(ignore) => ignore,
                None => super::search::search_ignore(view.view()).await?,
            };
            tokio::select! {
                biased;
                _ = shared.changed.closed() => return Ok(()),
                changed = source_changed(&mut native) => { native_delta = Some(changed?); },
                filled = fill(view, ignore, shared) => { filled?; },
            }
            // Coalesce our own page publications. Reading a million-name
            // index must not make its producer replay thousands of its own
            // old notifications one by one.
            continue;
        }
        tokio::select! {
            _ = shared.changed.closed() => return Ok(()),
            _ = shared.refresh.notified() => {},
            changed = source_changed(&mut native) => { native_delta = Some(changed?); },
            record = records.next() => {
                record.ok_or_else(|| Error::Storage("pathname index file follow ended".into()))?
                    .map_err(storage_index)?;
            }
        }
    }
}

async fn source_changed(
    native: &mut Option<acyclic_fs::lazy_workspace::NativeNameChanges>,
) -> crate::Result<acyclic_fs::lazy_workspace::NativeNameDelta> {
    match native {
        Some(native) => native.changed().await.map_err(Error::from),
        None => std::future::pending().await,
    }
}

async fn reusable(view: View<'_>, index: &Index) -> crate::Result<bool> {
    let indexed_workspace = match &index.manifest.cut {
        acyclic_fs::NameCut::Lazy(cut) => cut.workspace_id,
        acyclic_fs::NameCut::Exact { workspace, .. } => *workspace,
    };
    if indexed_workspace != view.workspace().id() {
        return Ok(false);
    }
    if let (View::Checkout(lazy), acyclic_fs::NameCut::Lazy(old)) = (view, &index.manifest.cut) {
        let new = lazy
            .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()))
            .await?;
        if old.source != new.source || old.overlay != new.overlay || old.shadows != new.shadows {
            return Ok(false);
        }
    } else if !matches!(
        (view, &index.manifest.cut),
        (View::Volume(_), acyclic_fs::NameCut::Exact { .. })
    ) {
        return Ok(false);
    }
    // The manifest's observed head is protected by compare-and-publish.
    // Compare that anchor with today's head. `Index::open` pins today's head,
    // which can contain later authored edits, so its generation alone is not
    // proof that the manifest is still current.
    let current = view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| failure.error)?
        .value;
    let observed = view
        .workspace()
        .generation(
            index.manifest.observed_generation,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    only_index_changes(&observed, &current).await
}

async fn only_index_changes(
    observed: &LocalGeneration,
    current: &LocalGeneration,
) -> crate::Result<bool> {
    let (diff, _) = observed
        .diff_to(
            current,
            512,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    if diff.changes().truncated {
        return Ok(false);
    }
    let paths = match diff
        .changed_paths(512, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
    {
        Ok(receipt) => receipt.value.0,
        Err(failure) if matches!(failure.error, WorkspaceError::ChangedPathLimit) => {
            // A bounded diff cannot certify a larger revision as index-only.
            // Rebuild/stream it; a large edit must not kill the producer.
            return Ok(false);
        }
        Err(failure) => return Err(failure.error.into()),
    };
    for path in paths {
        let mut text = String::new();
        for component in path.path.components() {
            text.push('/');
            text.push_str(
                &component
                    .unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .map_err(|failure| WorkspaceError::from(failure.error))?
                    .value,
            );
        }
        let path = if text.is_empty() { "/" } else { &text };
        if path != "/"
            && path != "/.acyclic-sdk"
            && path != "/.acyclic-sdk/name-index"
            && !path.starts_with("/.acyclic-sdk/name-index/")
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn fill(view: &OwnedView, ignore: GitIgnorePolicy, shared: &Shared) -> crate::Result<()> {
    loop {
        if fill_pass(view, ignore.clone(), shared).await? {
            return Ok(());
        }
        // A concurrent workspace writer or index producer won publication.
        // Follow a compatible winning checkpoint; otherwise start a new cut.
        let current = Index::open(view.workspace())
            .await
            .map_err(super::search::search_error)?;
        let current = match current {
            Some(index) if reusable(view.view(), &index).await? => Some(index),
            _ => None,
        };
        *shared.latest.lock().await = current;
        shared.changed.send_modify(|revision| *revision += 1);
        tokio::task::yield_now().await;
    }
}

async fn fill_pass(
    view: &OwnedView,
    ignore: GitIgnorePolicy,
    shared: &Shared,
) -> crate::Result<bool> {
    let workspace = view.workspace();
    let prior = shared.latest.lock().await.take();
    let (mut walk, mut pages, mut expected) = if let Some(index) = prior {
        if index.manifest.complete {
            *shared.latest.lock().await = Some(index);
            return Ok(true);
        }
        let walk = match view.view() {
            View::Checkout(lazy) => {
                NameWalk::resume(lazy, index.manifest.checkpoint.clone(), ignore).await
            }
            View::Volume(volume) => {
                NameWalk::resume_exact(volume, index.manifest.checkpoint.clone(), ignore).await
            }
        }
        .map_err(super::search::search_error)?;
        let pages = index.manifest.pages;
        let expected = index.generation();
        *shared.latest.lock().await = Some(index);
        (walk, pages, expected)
    } else {
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let walk = walk_names(view.view(), None, ignore).await?;
        let authored = match walk.cut() {
            acyclic_fs::NameCut::Lazy(cut) => cut.authored_generation,
            acyclic_fs::NameCut::Exact { generation, .. } => generation,
        };
        if authored != expected.id() {
            return Ok(false);
        }
        (walk, 0, expected)
    };
    loop {
        let page = walk
            .next_page()
            .await
            .map_err(super::search::search_error)?;
        let complete = page.is_none();
        pages += u64::from(page.is_some());
        let manifest = NameIndexManifest {
            schema: 1,
            cut: walk.cut(),
            observed_generation: expected.id(),
            pages,
            pending_directories: walk.pending_directories(),
            complete,
            checkpoint: walk.checkpoint().map_err(super::search::search_error)?,
        };
        let index = Index::publish(workspace, &expected, manifest, page.as_deref())
            .await
            .map_err(super::search::search_error)?;
        let Some(index) = index else {
            return Ok(false);
        };
        expected = index.generation();
        *shared.latest.lock().await = Some(index);
        shared.changed.send_modify(|revision| *revision += 1);
        if complete {
            return Ok(true);
        }
        tokio::task::yield_now().await;
    }
}
