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
        let mut transaction = workspace
            .begin_pinned_transaction(
                &expected,
                IdempotencyKey::new(),
                WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        for ordinal in 0..self.manifest.pages {
            let mut page = self.page(ordinal).await?;
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
            if changed {
                write_page(&mut transaction, ordinal, &page, &stop).await?;
            }
        }
        let mut ordinal = self.manifest.pages;
        let mut page = Vec::with_capacity(128);
        for name in leaves.into_values().flatten() {
            page.push(name);
            if page.len() == 128 {
                write_page(&mut transaction, ordinal, &page, &stop).await?;
                ordinal += 1;
                page.clear();
            }
        }
        if !page.is_empty() {
            write_page(&mut transaction, ordinal, &page, &stop).await?;
            ordinal += 1;
        }
        let manifest = NameIndexManifest {
            schema: 1,
            cut,
            observed_generation: expected.id(),
            pages: ordinal,
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
