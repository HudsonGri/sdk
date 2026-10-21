use super::*;
use crate::demand::{DemandSource, SourceNodeKind};
use crate::facade::FsWork as _;
use crate::lazy_workspace::NativeNameDelta;
use crate::{GitIgnorePolicy, LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore};
use std::collections::BTreeMap;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> NameIndex<A, O> {
    /// Applies a complete native leaf delta through metadata lookups only.
    /// Unknown changes, directories and policy changes require discovery.
    pub async fn refresh_native<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        &self,
        lazy: &LazyWorkspace<A, O, D, S>,
        snapshot: LazySnapshotRef,
        delta: NativeNameDelta,
        ignore: &GitIgnorePolicy,
    ) -> Result<NameIndexRefresh<A, O>, SearchError> {
        let NameCut::Lazy(old) = &self.manifest.cut else {
            return Ok(NameIndexRefresh::Ineligible);
        };
        if delta.all
            || !self.manifest.complete
            || old.workspace_id != snapshot.workspace_id
            || old.source.identity != snapshot.source.identity
            || old.overlay != snapshot.overlay
            || old.shadows != snapshot.shadows
        {
            return Ok(NameIndexRefresh::Ineligible);
        }
        let cut = NameCut::Lazy(snapshot);
        let Some(checkpoint) = self.manifest.checkpoint.completed_at(cut.clone()) else {
            return Ok(NameIndexRefresh::Ineligible);
        };
        let stop = CancellationToken::new();
        let workspace = lazy.workspace();
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?
            .value;
        if expected.id() != snapshot.authored_generation {
            return Ok(NameIndexRefresh::Conflict);
        }
        let observed = workspace
            .generation(
                self.manifest.observed_generation,
                WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let (diff, _) = match observed
            .diff_to(&expected, 512, WorkBudget::UNBOUNDED, &stop)
            .await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if matches!(failure.error, WorkspaceError::JoinLimit) => {
                return Ok(NameIndexRefresh::Ineligible);
            }
            Err(failure) => return Err(failure.error.into()),
        };
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
        for path in paths {
            if let Some(first) = path.path.components().first()
                && first
                    .unicode_text(WorkBudget::UNBOUNDED, &stop)
                    .map_err(|failure| WorkspaceError::from(failure.error))?
                    .value
                    != ".acyclic-sdk"
            {
                // Do not mix an unrelated authored edit into the native delta.
                return Ok(NameIndexRefresh::Ineligible);
            }
        }
        let mut leaves = BTreeMap::new();
        for path in delta.paths {
            if path
                .split('/')
                .any(|part| matches!(part, ".git" | ".acyclic-sdk"))
            {
                continue;
            }
            if matches!(path.rsplit('/').next(), Some(".gitignore" | ".ignore")) {
                return Ok(NameIndexRefresh::Ineligible);
            }
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&stop);
            let parent = path.rsplit_once('/').map_or("/", |(parent, _)| parent);
            if !lazy
                .same_volume_at_snapshot(snapshot, parent, &mut accounting)
                .await?
            {
                leaves.insert(path, None);
                continue;
            }
            let (entry, _, guard) = lazy
                .lookup_at_snapshot(snapshot, &format!("/{path}"), &mut accounting)
                .await?;
            let kind = entry.as_ref().map(|entry| entry.stat().kind);
            if let Some(entry) = entry {
                lazy.retire_lookup((entry, guard), &mut accounting)?;
            } else {
                crate::kernel::retire_allocation(guard, 0, &mut accounting)?;
            }
            if kind == Some(SourceNodeKind::Directory) {
                return Ok(NameIndexRefresh::Ineligible);
            }
            let name = kind
                .filter(|_| checkpoint.eligible(ignore, &path))
                .map(|kind| SearchName {
                    path: path.clone(),
                    regular: kind == SourceNodeKind::RegularFile,
                    directory: false,
                });
            leaves.insert(path, name);
        }
        self.publish_leaves(workspace, &expected, cut, checkpoint, leaves)
            .await
    }
}
