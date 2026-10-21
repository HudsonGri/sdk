use super::*;
use crate::GitIgnorePolicy;
use crate::kernel::FileKind;
use std::collections::BTreeMap;

/// Outcome of applying an authored leaf delta to an existing index.
pub enum NameIndexRefresh<A, O> {
    /// A directory/source/policy change requires namespace discovery.
    Ineligible,
    /// Another writer won the generation compare-and-publish.
    Conflict,
    /// The new index was published atomically without native enumeration.
    Published(NameIndex<A, O>),
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> NameIndex<A, O> {
    /// Applies bounded authored leaf changes without re-listing any directory.
    /// Pages stream through one bounded buffer; unchanged pages remain shared
    /// with the previous generation. Source and directory changes are explicit
    /// discovery work, never silently treated as a complete leaf delta.
    pub async fn refresh_authored(
        &self,
        workspace: &Workspace<A, O>,
        cut: NameCut,
        ignore: &GitIgnorePolicy,
    ) -> Result<NameIndexRefresh<A, O>, SearchError> {
        let generation = match (&self.manifest.cut, &cut) {
            (NameCut::Lazy(old), NameCut::Lazy(new))
                if old.workspace_id == new.workspace_id
                    && old.source == new.source
                    && old.overlay == new.overlay
                    && old.shadows == new.shadows =>
            {
                new.authored_generation
            }
            (
                NameCut::Exact { workspace: old, .. },
                NameCut::Exact {
                    workspace: new,
                    generation,
                },
            ) if old == new => *generation,
            _ => return Ok(NameIndexRefresh::Ineligible),
        };
        let Some(checkpoint) = self.manifest.checkpoint.completed_at(cut.clone()) else {
            return Ok(NameIndexRefresh::Ineligible);
        };
        if !self.manifest.complete || self.generation.workspace_id() != workspace.id() {
            return Ok(NameIndexRefresh::Ineligible);
        }
        let stop = CancellationToken::new();
        let expected = workspace
            .generation(generation, WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let observed = workspace
            .generation(
                self.manifest.observed_generation,
                WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let (diff, _) = observed
            .diff_to(&expected, 512, WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?
            .value;
        if diff.changes().truncated {
            return Ok(NameIndexRefresh::Ineligible);
        }
        let (paths, _allocation) = match diff.changed_paths(512, WorkBudget::UNBOUNDED, &stop).await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if matches!(failure.error, WorkspaceError::ChangedPathLimit) => {
                return Ok(NameIndexRefresh::Ineligible);
            }
            Err(failure) => return Err(failure.error.into()),
        };
        let mut leaves = BTreeMap::new();
        for change in paths {
            let mut path = String::new();
            for component in change.path.components() {
                if !path.is_empty() {
                    path.push('/');
                }
                path.push_str(
                    &component
                        .unicode_text(WorkBudget::UNBOUNDED, &stop)
                        .map_err(|failure| WorkspaceError::from(failure.error))?
                        .value,
                );
            }
            if path.is_empty()
                || path
                    .split('/')
                    .any(|part| matches!(part, ".git" | ".acyclic-sdk"))
            {
                continue;
            }
            if matches!(path.rsplit('/').next(), Some(".gitignore" | ".ignore")) {
                return Ok(NameIndexRefresh::Ineligible);
            }
            if matches!(cut, NameCut::Lazy(_)) && change.after.is_none() {
                // Removing an authored binding can reveal a native entry at
                // the same name. Only source-aware discovery can classify it.
                return Ok(NameIndexRefresh::Ineligible);
            }
            let before_dir = change
                .before
                .is_some_and(|record| record.kind == FileKind::Directory);
            let after_dir = change
                .after
                .is_some_and(|record| record.kind == FileKind::Directory);
            if before_dir || after_dir {
                if before_dir
                    && after_dir
                    && change.before.map(|record| record.file_id)
                        == change.after.map(|record| record.file_id)
                {
                    continue;
                }
                return Ok(NameIndexRefresh::Ineligible);
            }
            let name = change
                .after
                .filter(|_| checkpoint.eligible(ignore, &path))
                .map(|record| SearchName {
                    path: path.clone(),
                    regular: record.kind == FileKind::Regular,
                    directory: false,
                });
            leaves.insert(path, name);
        }
        self.publish_leaves(workspace, &expected, cut, checkpoint, leaves)
            .await
    }

    pub(super) async fn publish_leaves(
        &self,
        workspace: &Workspace<A, O>,
        expected: &Generation<A, O>,
        cut: NameCut,
        checkpoint: NameCheckpoint,
        mut leaves: BTreeMap<String, Option<SearchName>>,
    ) -> Result<NameIndexRefresh<A, O>, SearchError> {
        let stop = CancellationToken::new();
        let Some(mut transaction) = begin_at(workspace, expected).await? else {
            return Ok(NameIndexRefresh::Conflict);
        };
        let mut names_changed = false;
        let mut tail = None;
        let examined_pages = if leaves.is_empty() {
            0
        } else {
            self.manifest.pages
        };
        for ordinal in 0..examined_pages {
            let mut page = self.page(ordinal).await?;
            if page
                .iter()
                .any(|name| name.directory && leaves.contains_key(&name.path))
            {
                return Ok(NameIndexRefresh::Ineligible);
            }
            let mut changed = false;
            page.retain_mut(|name| {
                let Some(replacement) = leaves.remove(&name.path) else {
                    return true;
                };
                match replacement {
                    Some(replacement) => {
                        changed |= name.regular != replacement.regular;
                        *name = replacement;
                        true
                    }
                    None => {
                        changed = true;
                        false
                    }
                }
            });
            names_changed |= changed;
            if ordinal + 1 == self.manifest.pages {
                tail = Some((page, changed));
            } else if changed {
                write_page(&mut transaction, ordinal, &page, &stop).await?;
            }
        }
        // Reuse the final page across separate writes. Appending a new page
        // for each new filename would turn N single-file edits into N pages.
        let mut ordinal = self.manifest.pages.saturating_sub(1);
        let (mut page, mut dirty) = tail.unwrap_or_else(|| (Vec::with_capacity(128), false));
        let mut pages = self.manifest.pages;
        for name in leaves.into_values().flatten() {
            if page.len() == 128 {
                if dirty {
                    write_page(&mut transaction, ordinal, &page, &stop).await?;
                }
                ordinal += 1;
                page.clear();
            }
            page.push(name);
            names_changed = true;
            dirty = true;
            pages = pages.max(ordinal + 1);
        }
        if dirty {
            write_page(&mut transaction, ordinal, &page, &stop).await?;
        }
        let manifest = NameIndexManifest {
            schema: 1,
            names_cut: if names_changed {
                cut.clone()
            } else {
                self.manifest.names_cut.clone()
            },
            cut,
            observed_generation: expected.id(),
            pages,
            pending_directories: 0,
            complete: true,
            checkpoint,
        };
        let bytes = serde_json::to_vec(&manifest)?;
        if bytes.len() as u64 > MAXIMUM_INDEX_FILE_BYTES {
            return Err(SearchError::Invalid(
                "pathname index frontier exceeds its file budget".into(),
            ));
        }
        transaction
            .write(MANIFEST, bytes, WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?;
        let outcome = transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &stop,
                |_| async {
                    Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        match outcome {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => {
                Ok(NameIndexRefresh::Published(Self {
                    generation,
                    manifest,
                }))
            }
            TransactionCommit::Conflict { .. } => Ok(NameIndexRefresh::Conflict),
            TransactionCommit::Fenced | TransactionCommit::IdempotencyConflict => Err(
                SearchError::Invalid("pathname index publication was fenced".into()),
            ),
        }
    }
}

async fn write_page<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    ordinal: u64,
    page: &[SearchName],
    stop: &CancellationToken,
) -> Result<(), SearchError> {
    let bytes = serde_json::to_vec(page)?;
    if bytes.len() as u64 > MAXIMUM_INDEX_FILE_BYTES {
        return Err(SearchError::Invalid(
            "pathname index page exceeds its file budget".into(),
        ));
    }
    transaction
        .write(&page_path(ordinal), bytes, WorkBudget::UNBOUNDED, stop)
        .await
        .map_err(|failure| failure.error)?;
    Ok(())
}
