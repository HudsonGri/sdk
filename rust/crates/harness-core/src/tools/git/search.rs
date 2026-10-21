//! Paged grep of the selected logical view; no whole-tree exactification.
use super::*;
pub use acyclic_fs::SearchName;
use acyclic_fs::{GitGrepMatch, SearchError};

#[derive(serde::Serialize)]
pub(super) struct GrepResult {
    matches: Vec<GitGrepMatch>,
    truncated: bool,
    files_examined: u64,
    bytes_read: u64,
    pending_directories: usize,
    complete: bool,
}

impl GrepResult {
    fn finish(mut self) -> Self {
        self.matches
            .sort_by(|left, right| (&left.path, left.line).cmp(&(&right.path, right.line)));
        self
    }
}
/// Metadata cursor over the local selected view.
pub type NameWalk<'a> = acyclic_fs::NameWalk<
    'a,
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;
const FILE_BYTES: u64 = 1024 * 1024;
const TOTAL_BYTES: u64 = 16 * 1024 * 1024;
const MATCHES: usize = 1024;
/// Loads only the search ignore policy.
pub async fn search_ignore(view: View<'_>) -> crate::Result<GitIgnorePolicy> {
    crate::filesystem::workspace_policy::ignore_for(view).await
}
/// Starts metadata-only enumeration of a selected view.
pub async fn walk_names<'a>(
    view: View<'a>,
    root: Option<&str>,
    ignore: GitIgnorePolicy,
) -> crate::Result<NameWalk<'a>> {
    match view {
        View::Checkout(lazy) => NameWalk::lazy(lazy, root, ignore).await,
        View::Volume(volume) => {
            NameWalk::exact(
                volume
                    .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
                root,
                ignore,
            )
            .await
        }
    }
    .map_err(search_error)
}
pub(super) fn search_error(error: SearchError) -> Error {
    match error {
        SearchError::Json(error) => json_error(error),
        SearchError::Filesystem(error) => Error::from(WorkspaceError::from(error)),
        SearchError::Workspace(error) => error.into(),
        SearchError::Lazy(error) => error.into(),
        SearchError::Invalid(message) => Error::Invalid(message),
        SearchError::NotFound(path) => Error::NotFound(path),
    }
}
impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn grep(
        &self,
        tree: GitTreeRef,
        pattern: &str,
        path: Option<&str>,
    ) -> crate::Result<GrepResult> {
        let reader = match tree {
            GitTreeRef::Lazy(snapshot) => {
                let View::Checkout(lazy) = self.lazy_current else {
                    return Err(Error::Unauthorized(
                        "lazy search requires the selected source".into(),
                    ));
                };
                NameWalk::at_snapshot(lazy, snapshot, path, self.ignore.clone()).await
            }
            GitTreeRef::Exact(reference) => {
                NameWalk::exact(
                    self.generation(GitTreeRef::Exact(reference)).await?,
                    path,
                    self.ignore.clone(),
                )
                .await
            }
        }
        .map_err(search_error)?;
        let mut names = IndexedNames::open(self.lazy_current, self.ignore.clone()).await?;
        let root = path.unwrap_or("").trim_matches('/');
        let mut result = GrepResult {
            matches: Vec::new(),
            truncated: false,
            files_examined: 0,
            bytes_read: 0,
            pending_directories: names.pending_directories().await,
            complete: false,
        };
        let mut remaining = TOTAL_BYTES;
        while let Some(page) = names.next_page().await? {
            result.pending_directories = names.pending_directories().await;
            self.validate().await?;
            if page.reset && !names.matches_tree(tree).await? {
                // A concurrent public namespace edit cannot be folded into
                // this pinned read. Preserve already found matches and report
                // a partial result instead of mixing generations.
                result.truncated = true;
                return Ok(result.finish());
            }
            for name in page.names.into_iter().filter(|name| name.regular) {
                if !root.is_empty()
                    && name.path != root
                    && !name
                        .path
                        .strip_prefix(root)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                {
                    continue;
                }
                if !(self.read_allowed)(&name.path) {
                    continue;
                }
                if remaining == 0 {
                    result.truncated = true;
                    return Ok(result.finish());
                }
                let read = reader
                    .read(&name.path, FILE_BYTES.min(remaining))
                    .await
                    .map_err(search_error);
                result.files_examined += 1;
                let bytes = match read {
                    Ok(bytes) => bytes,
                    Err(Error::NativeSource { source, .. })
                        if matches!(
                            source.semantic_error(),
                            acyclic_fs::LazyWorkspaceError::TooLarge
                                | acyclic_fs::LazyWorkspaceError::NotRegularFile
                        ) =>
                    {
                        result.truncated = true;
                        continue;
                    }
                    Err(Error::NativeWorkspace { source, .. })
                        if matches!(
                            *source,
                            WorkspaceError::ReadLimitExceeded | WorkspaceError::NotRegularFile
                        ) =>
                    {
                        result.truncated = true;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                remaining = remaining.saturating_sub(bytes.len() as u64);
                result.bytes_read += bytes.len() as u64;
                if let Ok(text) = std::str::from_utf8(&bytes) {
                    for (line, text) in text
                        .lines()
                        .enumerate()
                        .filter(|(_, line)| line.contains(pattern))
                    {
                        if result.matches.len() == MATCHES {
                            result.truncated = true;
                            return Ok(result.finish());
                        }
                        result.matches.push(GitGrepMatch {
                            path: name.path.clone(),
                            line: line as u64 + 1,
                            text: text.into(),
                        });
                    }
                }
            }
        }
        result.pending_directories = 0;
        result.complete = !result.truncated;
        Ok(result.finish())
    }
}
