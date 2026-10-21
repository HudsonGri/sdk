//! One producer per opened namespace; clients retain only a page and their top-k.
//! Notifications wake readers, but index files are the authoritative snapshot.
use super::*;
use acyclic_fs::{NameIndex, NameIndexManifest};
use std::sync::{OnceLock, Weak};
use tokio::sync::watch;

type Index = NameIndex<LocalAuthorityBackend, LocalObjectBackend>;
type Registry = BTreeMap<acyclic_fs::WorkspaceId, Arc<AsyncMutex<Weak<Shared>>>>;
static INDEXES: OnceLock<Mutex<Registry>> = OnceLock::new();

struct Shared {
    latest: AsyncMutex<Option<Index>>,
    changed: watch::Sender<u64>,
    failure: Mutex<Option<String>>,
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
    pub async fn open(view: View<'_>, ignore: GitIgnorePolicy) -> crate::Result<Self> {
        let workspace = view.workspace();
        let entry = {
            let mut registry = INDEXES
                .get_or_init(|| Mutex::new(BTreeMap::new()))
                .lock()
                .map_err(|_| Error::Storage("pathname index registry poisoned".into()))?;
            Arc::clone(
                registry
                    .entry(workspace.id())
                    .or_insert_with(|| Arc::new(AsyncMutex::new(Weak::new()))),
            )
        };
        let mut entry = entry.lock().await;
        let current = workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        if let Some(shared) = entry.upgrade() {
            let latest = shared.latest.lock().await;
            let reusable = if let Some(index) = latest.as_ref() {
                index.generation().id() == current.id() && reusable(view, index).await?
            } else {
                true
            };
            if reusable {
                drop(latest);
                return Ok(Self {
                    changed: shared.changed.subscribe(),
                    shared,
                    next: 0,
                    cut: None,
                });
            }
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
        });
        *entry = Arc::downgrade(&shared);
        let producer = Arc::clone(&shared);
        tokio::spawn(async move {
            if producer
                .latest
                .lock()
                .await
                .as_ref()
                .is_some_and(|index| index.manifest.complete)
            {
                return;
            }
            if let Err(error) = fill(&owned, ignore, &producer).await {
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
}

fn storage_index(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
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
    let old = view
        .workspace()
        .generation(
            index.manifest.observed_generation,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let (diff, _) = old
        .diff_to(
            &index.generation(),
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
    let paths = diff
        .changed_paths(512, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| failure.error)?
        .value
        .0;
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
