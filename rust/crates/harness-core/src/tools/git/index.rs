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
    latest: AsyncMutex<Option<Arc<Index>>>,
    changed: watch::Sender<u64>,
    failure: Mutex<Option<String>>,
    directory_query: Mutex<Option<String>>,
    refresh: tokio::sync::Notify,
}

impl Shared {
    async fn snapshot(&self) -> Option<Arc<Index>> {
        self.latest.lock().await.clone()
    }
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
            let latest = shared.snapshot().await;
            let valid = if let Some(index) = latest.as_ref() {
                reusable(view, index).await?
            } else {
                true
            };
            if !valid {
                let mut current = shared.latest.lock().await;
                // Reuse checks perform I/O without blocking page readers or
                // the producer. Do not invalidate a newer publication that
                // arrived while this snapshot was being checked.
                if current
                    .as_ref()
                    .zip(latest.as_ref())
                    .is_some_and(|(a, b)| Arc::ptr_eq(a, b))
                {
                    *current = None;
                }
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
        let native = native_follow(&owned)?;
        let existing = Index::open(workspace)
            .await
            .map_err(super::search::search_error)?;
        let mut existing = if let Some(index) = existing {
            if reusable(view, &index).await? {
                Some(index)
            } else {
                None
            }
        } else {
            None
        };
        let restart_delta = restore_watches(&owned, existing.as_ref()).await?;
        if restart_delta.all || !restart_delta.paths.is_empty() {
            if let OwnedView::Checkout(lazy) = &owned {
                lazy.invalidate_indexed_native_metadata();
            }
            existing = None;
        }
        let (changed, receiver) = watch::channel(0);
        let shared = Arc::new(Shared {
            latest: AsyncMutex::new(existing.map(Arc::new)),
            changed,
            failure: Mutex::new(None),
            directory_query: Mutex::new(None),
            refresh: tokio::sync::Notify::new(),
        });
        *entry = Arc::downgrade(&shared);
        let producer = Arc::clone(&shared);
        tokio::spawn(async move {
            if let Err(error) = maintain(&owned, &producer, native, restart_delta).await {
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

    /// Supplies a disposable scheduling hint derived from the followed query
    /// file. The producer and durable index remain shared by all consumers;
    /// query text is never published into the volume's public index files.
    pub fn prefer_directory_names(&self, query: &str) -> crate::Result<()> {
        let mut current = self
            .shared
            .directory_query
            .lock()
            .map_err(|_| Error::Storage("index directory query lock poisoned".into()))?;
        let query = (!query.is_empty()).then_some(query);
        if current.as_deref() != query {
            *current = query.map(str::to_owned);
        }
        Ok(())
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
            let latest = self.shared.snapshot().await;
            if let Some(index) = latest.as_ref() {
                if self.cut.as_ref() != Some(&index.manifest.names_cut) {
                    self.cut = Some(index.manifest.names_cut.clone());
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

    /// Unreadable or unrepresentable entries recorded in the durable cut.
    pub async fn unavailable_entries(&self) -> usize {
        self.shared
            .latest
            .lock()
            .await
            .as_ref()
            .map_or(0, |index| index.manifest.checkpoint.unavailable_entries())
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
        let latest = self.shared.snapshot().await;
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

fn native_follow(
    view: &OwnedView,
) -> crate::Result<Option<acyclic_fs::lazy_workspace::NativeNameChanges>> {
    Ok(match view {
        OwnedView::Checkout(lazy) => {
            let cancellation = CancellationToken::new();
            let work =
                acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            lazy.follow_native_names(&mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &cancellation,
            })?
        }
        OwnedView::Volume(_) => None,
    })
}

async fn restore_watches(
    view: &OwnedView,
    index: Option<&Index>,
) -> crate::Result<acyclic_fs::lazy_workspace::NativeNameDelta> {
    // A newly opened inotify subscription initially watches only the root.
    // Restore registrations and compare persisted per-directory tokens before
    // declaring a completed index live. Lookup registers each directory and
    // its ancestors; it never lists it or reads candidate content. Offline
    // changes cannot be hidden by reinstalling watches on a stale snapshot.
    let mut delta = acyclic_fs::lazy_workspace::NativeNameDelta::default();
    if let OwnedView::Checkout(lazy) = view
        && let Some(index) = index
        && let acyclic_fs::NameCut::Lazy(snapshot) = &index.manifest.cut
    {
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        for (path, expected) in index.manifest.checkpoint.source_versions() {
            let observed = lazy
                .observe_indexed_native_metadata(*snapshot, &format!("/{path}"), &mut accounting)
                .await?;
            if observed != expected {
                // Policy content can change without touching the directory's
                // token. Rediscover that policy's subtree, not its siblings.
                let root = if matches!(path.rsplit('/').next(), Some(".gitignore" | ".ignore")) {
                    path.rsplit_once('/').map_or("", |(parent, _)| parent)
                } else {
                    path
                };
                if root.is_empty() || !index.manifest.complete || delta.paths.len() == 512 {
                    delta.all = true;
                    delta.paths.clear();
                } else if !delta.all {
                    delta.paths.insert(root.to_owned());
                }
            }
        }
    }
    Ok(delta)
}

async fn maintain(
    view: &OwnedView,
    shared: &Shared,
    mut native: Option<acyclic_fs::lazy_workspace::NativeNameChanges>,
    restart_delta: acyclic_fs::lazy_workspace::NativeNameDelta,
) -> crate::Result<()> {
    let mut native_delta =
        (restart_delta.all || !restart_delta.paths.is_empty()).then_some(restart_delta);
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
            *latest = published.map(Arc::new);
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
    let (diff, _) = match observed
        .diff_to(
            current,
            512,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
    {
        Ok(receipt) => receipt.value,
        Err(failure) if matches!(failure.error, WorkspaceError::JoinLimit) => return Ok(false),
        Err(failure) => return Err(failure.error.into()),
    };
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
        if !index_path(path.path.components()) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn index_path(parts: &[acyclic_fs::kernel::LogicalName]) -> bool {
    fn equals(name: &acyclic_fs::kernel::LogicalName, text: &str) -> bool {
        use acyclic_fs::kernel::NameEncoding;
        match name.encoding() {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => name.as_bytes() == text.as_bytes(),
            NameEncoding::WindowsUtf16Le => {
                let (units, rest) = name.as_bytes().as_chunks::<2>();
                rest.is_empty()
                    && units
                        .iter()
                        .map(|unit| u16::from_le_bytes(*unit))
                        .eq(text.encode_utf16())
            }
        }
    }
    parts.is_empty()
        || (equals(&parts[0], ".acyclic-sdk")
            && (parts.len() == 1 || equals(&parts[1], "name-index")))
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
        *shared.latest.lock().await = current.map(Arc::new);
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
    let mut prior = shared.snapshot().await;
    let (mut walk, mut pages, mut expected) = if let Some(index) = &prior {
        if index.manifest.complete {
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
        {
            let query = shared
                .directory_query
                .lock()
                .map_err(|_| Error::Storage("index directory query lock poisoned".into()))?;
            walk.prefer_directory_names(query.as_deref());
        }
        let page = walk
            .next_page()
            .await
            .map_err(super::search::search_error)?;
        let complete = page.is_none();
        pages += u64::from(page.is_some());
        let manifest = NameIndexManifest {
            schema: 5,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages,
            pending_directories: walk.pending_directories(),
            complete,
            checkpoint: walk.checkpoint().map_err(super::search::search_error)?,
        };
        let index = Index::publish(
            workspace,
            &expected,
            manifest,
            page.as_deref(),
            prior.as_deref(),
        )
        .await
        .map_err(super::search::search_error)?;
        let Some(index) = index else {
            return Ok(false);
        };
        expected = index.generation();
        prior = Some(Arc::new(index));
        *shared.latest.lock().await = prior.clone();
        shared.changed.send_modify(|revision| *revision += 1);
        if complete {
            return Ok(true);
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_search_index_only_paths_classify_native_names_without_projection() {
        use acyclic_fs::kernel::{LogicalName, NameEncoding};
        for encoding in [
            NameEncoding::Utf8,
            NameEncoding::PosixBytes,
            NameEncoding::WindowsUtf16Le,
        ] {
            let name = |text: &str| {
                LogicalName::new(
                    encoding,
                    match encoding {
                        NameEncoding::WindowsUtf16Le => {
                            text.encode_utf16().flat_map(u16::to_le_bytes).collect()
                        }
                        _ => text.as_bytes().to_vec(),
                    },
                    255,
                )
                .unwrap()
            };
            assert!(index_path(&[]));
            assert!(index_path(&[name(".acyclic-sdk")]));
            assert!(index_path(&[
                name(".acyclic-sdk"),
                name("name-index"),
                name("page.json")
            ]));
            assert!(!index_path(&[name(".acyclic-sdk"), name("public.txt")]));
            assert!(!index_path(&[name(".acyclic-sdk-other")]));
        }
        let raw = LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255).unwrap();
        // Negative control: the old projection fails before classifying this
        // ordinary authored edit as a reason to refresh the index.
        assert!(
            raw.unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .is_err()
        );
        assert!(!index_path(&[raw]));
    }

    #[tokio::test]
    async fn volume_search_pinned_readers_allow_publication_and_retain_their_page()
    -> crate::Result<()> {
        let temp = tempfile::tempdir().map_err(storage_index)?;
        let fs = LocalFs::local(acyclic_fs::LocalOptions::new(temp.path().join("fs")))
            .await
            .map_err(storage_index)?;
        let stop = CancellationToken::new();
        let workspace = fs
            .create_workspace("snapshot-reader", WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?
            .value;
        workspace.write_text("/retained.txt", "content").await?;
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let mut walk = NameWalk::exact(expected.clone(), None, GitIgnorePolicy::default())
            .await
            .map_err(super::super::search::search_error)?;
        let page = walk
            .next_page()
            .await
            .map_err(super::super::search::search_error)?
            .unwrap();
        assert!(
            walk.next_page()
                .await
                .map_err(super::super::search::search_error)?
                .is_none()
        );
        let manifest = NameIndexManifest {
            schema: 5,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages: 1,
            pending_directories: 0,
            complete: true,
            checkpoint: walk
                .checkpoint()
                .map_err(super::super::search::search_error)?,
        };
        let index = Index::publish(&workspace, &expected, manifest, Some(&page), None)
            .await
            .map_err(super::super::search::search_error)?
            .unwrap();
        let (changed, _receiver) = watch::channel(0);
        let shared = Arc::new(Shared {
            latest: AsyncMutex::new(Some(Arc::new(index))),
            changed,
            failure: Mutex::new(None),
            directory_query: Mutex::new(None),
            refresh: tokio::sync::Notify::new(),
        });
        let reader = shared.snapshot().await.unwrap();
        // A suspended consumer keeps its generation pin, but publication must
        // finish before that consumer resumes and reads its old immutable page.
        let publisher = Arc::clone(&shared);
        tokio::time::timeout(std::time::Duration::from_secs(1), async move {
            *publisher.latest.lock().await = None;
        })
        .await
        .map_err(storage_index)?;
        assert_eq!(
            reader
                .page(0)
                .await
                .map_err(super::super::search::search_error)?[0]
                .path,
            "retained.txt"
        );
        // Negative control: holding the old reader guard prevents publication.
        let held = shared.latest.lock().await;
        assert!(shared.latest.try_lock().is_err());
        drop(held);
        Ok(())
    }
}
