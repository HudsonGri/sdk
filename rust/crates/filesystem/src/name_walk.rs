//! Paged metadata-only traversal shared by pathname indexing and content grep.
use crate::demand::{DemandSource, SourceNodeKind};
use crate::facade::FsWork as _;
use crate::kernel::FileKind;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Generation, GitIgnorePolicy,
    LazyDirectoryCursor, LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore, WorkBudget,
};
use std::collections::BTreeMap;
const PAGE_ENTRIES: u32 = 128;
/// Failure while traversing a pinned logical namespace.
#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    /// Invalid durable index bytes.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Namespace encoding or bounded kernel operation failed.
    #[error(transparent)]
    Filesystem(#[from] crate::FsError),
    /// Authored filesystem failure.
    #[error(transparent)]
    Workspace(#[from] crate::WorkspaceError),
    /// Demand-backed filesystem failure.
    #[error(transparent)]
    Lazy(#[from] crate::LazyWorkspaceError),
    /// Invalid search root or continuation.
    #[error("{0}")]
    Invalid(String),
    /// Missing explicit root.
    #[error("search root missing: {0}")]
    NotFound(String),
}
enum Cut<'a, A, O, D, S> {
    Lazy(&'a LazyWorkspace<A, O, D, S>, LazySnapshotRef),
    Exact(Generation<A, O>),
}

/// Immutable identity of the names represented by one index version.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NameCut {
    /// Authored generation plus pinned source, overlay and shadow bindings.
    Lazy(LazySnapshotRef),
    /// Exact SDK generation.
    Exact {
        /// Owning branch in the volume.
        workspace: crate::WorkspaceId,
        /// Immutable namespace generation.
        generation: crate::GenerationId,
    },
}

/// Bounded unfinished traversal, stored alongside index pages.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct NameCheckpoint {
    cut: NameCut,
    pending: Vec<(String, SavedCursor)>,
    single: Option<SearchName>,
    ignore_policies: BTreeMap<String, GitIgnorePolicy>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NameCheckpoint {
    pub(crate) fn completed_at(&self, cut: NameCut) -> Option<Self> {
        if !self.pending.is_empty() || self.single.is_some() {
            return None;
        }
        let mut checkpoint = self.clone();
        checkpoint.cut = cut;
        Some(checkpoint)
    }

    pub(crate) fn eligible(&self, root: &GitIgnorePolicy, path: &str) -> bool {
        root.eligible_with_nested(&self.ignore_policies, path, false)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
enum SavedCursor {
    Lazy(Option<Vec<u8>>),
    Exact(Option<(u8, Vec<u8>)>),
}

/// A name and its no-follow classification. File contents are never consulted.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SearchName {
    /// Root-relative slash-separated spelling.
    pub path: String,
    /// Only regular files can be read by content search.
    pub regular: bool,
    /// Only directories are traversed; links are leaves.
    pub directory: bool,
}

enum Continuation {
    Lazy(Option<LazyDirectoryCursor>),
    Exact(Option<crate::kernel::LogicalName>),
}

/// Bounded pages over one immutable authored/source cut. Retains directory
/// continuations, never file contents or the complete set of regular names.
pub struct NameWalk<'a, A, O, D, S> {
    cut: Cut<'a, A, O, D, S>,
    pending: Vec<(String, Continuation)>,
    authored_pending: Vec<(String, Continuation)>,
    single: Option<SearchName>,
    ignore: GitIgnorePolicy,
    // Existing authored metadata is cheap and available before native listing.
    authored: Option<Generation<A, O>>,
    ignore_policies: BTreeMap<String, GitIgnorePolicy>,
}

impl<
    'a,
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> NameWalk<'a, A, O, D, S>
{
    /// Exact generation and source binding of this traversal.
    pub fn cut(&self) -> NameCut {
        match &self.cut {
            Cut::Lazy(_, cut) => NameCut::Lazy(*cut),
            Cut::Exact(generation) => NameCut::Exact {
                workspace: generation.workspace_id(),
                generation: generation.id(),
            },
        }
    }

    /// Captures the bounded frontier without retaining emitted candidates.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn checkpoint(&self) -> Result<NameCheckpoint, SearchError> {
        let pending = self
            .pending
            .iter()
            .chain(self.authored_pending.iter())
            .map(|(path, cursor)| {
                let cursor = match cursor {
                    Continuation::Lazy(cursor) => SavedCursor::Lazy(
                        cursor
                            .as_ref()
                            .map(LazyDirectoryCursor::checkpoint)
                            .transpose()?,
                    ),
                    Continuation::Exact(name) => SavedCursor::Exact(
                        name.as_ref()
                            .map(|name| (name.encoding().tag(), name.as_bytes().to_vec())),
                    ),
                };
                Ok((path.clone(), cursor))
            })
            .collect::<Result<_, SearchError>>()?;
        Ok(NameCheckpoint {
            cut: self.cut(),
            pending,
            single: self.single.clone(),
            ignore_policies: self.ignore_policies.clone(),
        })
    }

    /// Resumes at the original cut; the supplied view retains its source capability.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resume(
        view: &'a LazyWorkspace<A, O, D, S>,
        checkpoint: NameCheckpoint,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        Self::resume_with(view.workspace(), Some(view), checkpoint, ignore).await
    }

    /// Restores an exact volume's unfinished metadata traversal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resume_exact(
        workspace: &crate::Workspace<A, O>,
        checkpoint: NameCheckpoint,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        Self::resume_with(workspace, None, checkpoint, ignore).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn resume_with(
        workspace: &crate::Workspace<A, O>,
        view: Option<&'a LazyWorkspace<A, O, D, S>>,
        checkpoint: NameCheckpoint,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        let cut = match checkpoint.cut {
            NameCut::Lazy(cut) if cut.workspace_id == workspace.id() => Cut::Lazy(
                view.ok_or_else(|| {
                    SearchError::Invalid("lazy index requires its source capability".into())
                })?,
                cut,
            ),
            NameCut::Exact {
                workspace: id,
                generation,
            } if id == workspace.id() => Cut::Exact(
                workspace
                    .generation(generation, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ),
            _ => {
                return Err(SearchError::Invalid(
                    "checkpoint belongs to another workspace".into(),
                ));
            }
        };
        let pending: Vec<(String, Continuation)> = checkpoint
            .pending
            .into_iter()
            .map(|(path, cursor)| {
                let cursor = match cursor {
                    SavedCursor::Lazy(cursor) => Continuation::Lazy(
                        cursor
                            .map(|bytes| {
                                LazyDirectoryCursor::restore_checkpoint(
                                    &bytes,
                                    workspace.profile(),
                                    workspace.limits(),
                                )
                            })
                            .transpose()?,
                    ),
                    SavedCursor::Exact(name) => Continuation::Exact(
                        name.map(|(encoding, bytes)| {
                            crate::kernel::LogicalName::new(
                                crate::kernel::NameEncoding::from_tag(encoding)?,
                                bytes,
                                workspace.limits().maximum_component_bytes,
                            )
                        })
                        .transpose()
                        .map_err(|error| SearchError::Invalid(error.to_string()))?,
                    ),
                };
                Ok((path, cursor))
            })
            .collect::<Result<_, SearchError>>()?;
        let authored = match &cut {
            Cut::Lazy(lazy, snapshot) => Some(
                lazy.workspace()
                    .generation(
                        snapshot.authored_generation,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ),
            Cut::Exact(_) => None,
        };
        let (authored_pending, pending) = pending.into_iter().partition(|(_, cursor)| {
            authored.is_some() && matches!(cursor, Continuation::Exact(_))
        });
        Ok(Self {
            cut,
            pending,
            authored_pending,
            single: checkpoint.single,
            ignore,
            authored,
            ignore_policies: checkpoint.ignore_policies,
        })
    }

    /// Pins a lazy metadata cut without reading regular-file contents.
    pub async fn lazy(
        view: &'a LazyWorkspace<A, O, D, S>,
        root: Option<&str>,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        let snapshot = view
            .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()))
            .await?;
        Self::at(Cut::Lazy(view, snapshot), root, ignore).await
    }
    /// Lists one immutable exact generation.
    pub async fn exact(
        view: Generation<A, O>,
        root: Option<&str>,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        Self::at(Cut::Exact(view), root, ignore).await
    }
    /// Lists a previously pinned lazy cut.
    pub async fn at_snapshot(
        view: &'a LazyWorkspace<A, O, D, S>,
        snapshot: LazySnapshotRef,
        root: Option<&str>,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        Self::at(Cut::Lazy(view, snapshot), root, ignore).await
    }
    async fn at(
        cut: Cut<'a, A, O, D, S>,
        root: Option<&str>,
        ignore: GitIgnorePolicy,
    ) -> Result<Self, SearchError> {
        let path = format!("/{}", root.unwrap_or("").trim_matches('/'));
        // Validate before any source call; parent traversal is not a search root.
        let workspace = match &cut {
            Cut::Lazy(lazy, _) => lazy.workspace().clone(),
            Cut::Exact(generation) => generation.workspace(),
        };
        crate::kernel::NamespacePath::parse_in_profile(
            &path,
            workspace.profile(),
            workspace.limits(),
            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            &CancellationToken::new(),
        )?;
        let (directory, regular) = match &cut {
            Cut::Lazy(lazy, snapshot) => {
                let stop = CancellationToken::new();
                let mut ctx = WorkBudget::UNBOUNDED.accounting(&stop);
                let (entry, _, guard) = lazy.lookup_at_snapshot(*snapshot, &path, &mut ctx).await?;
                let entry = entry.ok_or_else(|| SearchError::NotFound(path.clone()))?;
                let kind = entry.stat().kind;
                lazy.retire_lookup((entry, guard), &mut ctx)?;
                (
                    kind == SourceNodeKind::Directory,
                    kind == SourceNodeKind::RegularFile,
                )
            }
            Cut::Exact(generation) => {
                let kind = generation
                    .stat(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value
                    .kind;
                (kind == FileKind::Directory, kind == FileKind::Regular)
            }
        };
        let authored = match &cut {
            Cut::Lazy(lazy, snapshot) => Some(
                lazy.workspace()
                    .generation(
                        snapshot.authored_generation,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ),
            Cut::Exact(_) => None,
        };
        let mut walk = Self {
            cut,
            pending: Vec::new(),
            authored_pending: Vec::new(),
            single: None,
            ignore,
            authored,
            ignore_policies: BTreeMap::new(),
        };
        if directory
            && let Cut::Lazy(lazy, snapshot) = &walk.cut
            && !lazy
                .same_volume_at_snapshot(
                    *snapshot,
                    &path,
                    &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                )
                .await?
        {
            return Ok(walk);
        }
        // An explicit subtree still inherits all ancestor ignore files.
        let relative = path.trim_matches('/');
        for (end, _) in relative.match_indices('/') {
            walk.load_directory_ignore(&relative[..end]).await?;
        }
        if directory && !relative.is_empty() {
            walk.load_directory_ignore(relative).await?;
        }
        if directory {
            let cursor = walk.start();
            walk.pending.push((path.clone(), cursor));
            if let Some(authored) = &walk.authored {
                match authored
                    .stat(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                {
                    Ok(stat) if stat.value.kind == FileKind::Directory => walk
                        .authored_pending
                        .push((path, Continuation::Exact(None))),
                    Ok(_) => {}
                    Err(failure) if matches!(failure.error, crate::WorkspaceError::NotFound) => {}
                    Err(failure) => return Err(failure.error.into()),
                }
            }
        } else {
            walk.single = Some(SearchName {
                path: path.trim_start_matches('/').into(),
                regular,
                directory: false,
            });
        }
        Ok(walk)
    }

    /// Unfinished directory count; zero is meaningful only after a terminal page.
    pub fn pending_directories(&self) -> usize {
        self.pending.len() + self.authored_pending.len()
    }

    fn start(&self) -> Continuation {
        match self.cut {
            Cut::Lazy(..) => Continuation::Lazy(None),
            Cut::Exact(_) => Continuation::Exact(None),
        }
    }

    /// Lists at most one source page. Empty pages can make progress; `None`
    /// alone signals completion. No regular-file read occurs on this path.
    pub async fn next_page(&mut self) -> Result<Option<Vec<SearchName>>, SearchError> {
        if let Some(single) = self.single.take() {
            return Ok(Some(
                if self
                    .ignore
                    .eligible_with_nested(&self.ignore_policies, &single.path, false)
                {
                    vec![single]
                } else {
                    Vec::new()
                },
            ));
        }
        let Some((directory, cursor)) = self.authored_pending.pop().or_else(|| self.pending.pop())
        else {
            return Ok(None);
        };
        let mut names = Vec::new();
        let exact = matches!(&cursor, Continuation::Exact(_));
        if matches!(
            &cursor,
            Continuation::Exact(None) | Continuation::Lazy(None)
        ) {
            // Check before reading this directory's ignore files, including
            // during the authored seed pass. Policy reads are content reads
            // too and cannot enter a foreign native mount.
            if let Cut::Lazy(lazy, snapshot) = &self.cut
                && !lazy
                    .same_volume_at_snapshot(
                        *snapshot,
                        &directory,
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await?
            {
                return Ok(Some(Vec::new()));
            }
            self.load_directory_ignore(directory.trim_matches('/'))
                .await?;
        }
        let (rows, next) = match (&self.cut, cursor) {
            (Cut::Lazy(lazy, snapshot), Continuation::Lazy(cursor)) => {
                let stop = CancellationToken::new();
                let mut ctx = WorkBudget::UNBOUNDED.accounting(&stop);
                let (page, _, guard) = lazy
                    .list_directory_at_snapshot(
                        *snapshot,
                        &directory,
                        cursor,
                        PAGE_ENTRIES,
                        &mut ctx,
                    )
                    .await?;
                let rows = page
                    .entries
                    .iter()
                    .filter(|entry| !entry.authored)
                    .map(|entry| {
                        Ok((
                            entry
                                .name
                                .unicode_text(WorkBudget::UNBOUNDED, &stop)
                                .map_err(|failure| failure.error)?
                                .value
                                .to_string(),
                            entry.kind == SourceNodeKind::RegularFile,
                            entry.kind == SourceNodeKind::Directory,
                        ))
                    })
                    .collect::<Result<Vec<_>, SearchError>>()?;
                let next = page.next.map(|cursor| Continuation::Lazy(Some(cursor)));
                drop(page.entries);
                drop(guard);
                (rows, next)
            }
            (cut, Continuation::Exact(after)) => {
                let generation = match cut {
                    Cut::Exact(generation) => generation,
                    Cut::Lazy(..) => self.authored.as_ref().ok_or_else(|| {
                        SearchError::Invalid("missing authored seed generation".into())
                    })?,
                };
                let page = generation
                    .list_directory(
                        &directory,
                        after.as_ref(),
                        PAGE_ENTRIES,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value
                    .0;
                let rows = page
                    .entries
                    .iter()
                    .map(|entry| {
                        Ok((
                            entry
                                .name
                                .unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                                .map_err(|failure| failure.error)?
                                .value
                                .to_string(),
                            entry.kind == FileKind::Regular,
                            entry.kind == FileKind::Directory,
                        ))
                    })
                    .collect::<Result<Vec<_>, SearchError>>()?;
                let next = if page.has_more {
                    page.entries
                        .last()
                        .map(|entry| Continuation::Exact(Some(entry.name.clone())))
                } else {
                    None
                };
                (rows, next)
            }
            _ => {
                return Err(SearchError::Invalid(
                    "directory continuation changed kind".into(),
                ));
            }
        };
        if let Some(cursor) = next {
            if exact && self.authored.is_some() {
                self.authored_pending.push((directory.clone(), cursor));
            } else {
                self.pending.push((directory.clone(), cursor));
            }
        }
        let mut children = Vec::new();
        for (name, regular, is_directory) in rows {
            let path = format!("{}/{}", directory.trim_matches('/'), name)
                .trim_start_matches('/')
                .to_owned();
            if path
                .split('/')
                .any(|part| matches!(part, ".git" | ".acyclic-sdk"))
                || !self
                    .ignore
                    .eligible_with_nested(&self.ignore_policies, &path, is_directory)
            {
                continue;
            }
            if is_directory {
                children.push((
                    format!("/{path}"),
                    if exact {
                        Continuation::Exact(None)
                    } else {
                        self.start()
                    },
                ));
            }
            names.push(SearchName {
                path,
                regular,
                directory: is_directory,
            });
        }
        // Descend before reading the parent's next page. The frontier is at
        // most PAGE_ENTRIES siblings per depth, independent of tree breadth.
        if exact && self.authored.is_some() {
            for (path, _) in &children {
                self.pending.push((path.clone(), Continuation::Lazy(None)));
            }
            self.authored_pending.extend(children.into_iter().rev());
        } else {
            self.pending.extend(children.into_iter().rev());
        }
        Ok(Some(names))
    }

    async fn load_directory_ignore(&mut self, directory: &str) -> Result<(), SearchError> {
        // Root rules were supplied by the owner. Only policy files are read;
        // listing and classifying candidate files never opens their contents.
        if directory.is_empty() || self.ignore_policies.contains_key(directory) {
            return Ok(());
        }
        let mut policy = None;
        for name in [".gitignore", ".ignore"] {
            let path = format!("{directory}/{name}");
            let bytes = match self.read(&path, 1024 * 1024).await {
                Ok(bytes) => bytes,
                Err(SearchError::Lazy(
                    crate::LazyWorkspaceError::NotFound | crate::LazyWorkspaceError::NotRegularFile,
                ))
                | Err(SearchError::Workspace(
                    crate::WorkspaceError::NotFound | crate::WorkspaceError::NotRegularFile,
                )) => continue,
                Err(error) => return Err(error),
            };
            policy
                .get_or_insert_with(GitIgnorePolicy::default)
                .extend(GitIgnorePolicy::parse(&String::from_utf8_lossy(&bytes)));
        }
        if let Some(policy) = policy {
            self.ignore_policies.insert(directory.to_owned(), policy);
        }
        Ok(())
    }

    /// Reads only an explicitly selected candidate at the original pinned cut.
    pub async fn read(
        &self,
        path: &str,
        maximum: u64,
    ) -> Result<acyclic_stream::OwnedBytes, SearchError> {
        let absolute = format!("/{path}");
        match &self.cut {
            Cut::Lazy(lazy, snapshot) => {
                let stop = CancellationToken::new();
                let mut ctx = WorkBudget::UNBOUNDED.accounting(&stop);
                let (entry, _, guard) = lazy
                    .lookup_at_snapshot(*snapshot, &absolute, &mut ctx)
                    .await?;
                let Some(entry) = entry else {
                    crate::kernel::retire_allocation(guard, 0, &mut ctx)?;
                    return Err(crate::LazyWorkspaceError::NotFound.into());
                };
                // An indexed regular name can be replaced by a link between
                // enumeration and reading. Search never reads link payloads,
                // including policy files, through the generic snapshot reader.
                let result = if entry.stat().kind == SourceNodeKind::RegularFile {
                    lazy.read_resolved(&entry, &absolute, None, maximum, &mut ctx)
                        .await
                        .map(|chunk| chunk.bytes)
                        .map_err(SearchError::from)
                } else {
                    Err(crate::LazyWorkspaceError::NotRegularFile.into())
                };
                lazy.retire_lookup((entry, guard), &mut ctx)?;
                result
            }
            Cut::Exact(generation) => generation
                .read(
                    &absolute,
                    maximum,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| SearchError::from(failure.error)),
        }
    }
}
