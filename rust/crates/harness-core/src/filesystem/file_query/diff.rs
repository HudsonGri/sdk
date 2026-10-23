//! Exact immutable endpoints from the existing authority-log query.
use super::*;
use crate::context::{ContextRoot, DiffPath, DiffPublication, PinnedContextPath};

impl<P: StreamProvider + 'static, O: AsyncObjectStore + 'static> FilesystemFileQuery<P, O> {
    /// Snapshot changes from an immutable base, then follow exact affected paths.
    /// No native watcher, timer, execution event or caller-supplied file content.
    pub async fn observe_diff(
        &self,
        base: acyclic_fs::GenerationId,
        maximum_changes: u32,
    ) -> crate::Result<acyclic_stream::BoxProviderStream<'static, crate::Result<DiffPublication>>>
    {
        if maximum_changes == 0 {
            return Err(crate::Error::Invalid(
                "diff query requires a finite path bound".into(),
            ));
        }
        let io = PreparationIo::from_budget(self.budget, &acyclic_fs::CancellationToken::new())?;
        let base = self.reader.host.generation_ref_id(base, Some(&io))?;
        self.authorize(&Query::File(task_volume::FileKey::Goal), &io)
            .await?;
        let query = *blake3::hash(
            &serde_json::to_vec(&base).map_err(|e| crate::Error::Invalid(e.to_string()))?,
        )
        .as_bytes();
        let (workspace, current, cursor, _, _, records) = self.open_cut(query, None, &io).await?;
        let baseline = self
            .reader
            .host
            .generation(&workspace, &base, Some(&io))
            .await?;
        let snapshot = self
            .publication(&baseline, &baseline, &current, maximum_changes, &io)
            .await?;
        let source = self.clone();
        let changes = futures::stream::try_unfold(
            (source, baseline, current, cursor.position, records),
            move |(source, base, current, position, mut records)| async move {
                let record = records.next().await.ok_or_else(|| {
                    crate::Error::Conflict("diff follow ended; reopen snapshot".into())
                })??;
                if record.sequence != position {
                    return Err(crate::Error::Conflict(
                        "diff follow cursor gap; reopen snapshot".into(),
                    ));
                }
                let io = PreparationIo::from_budget(
                    source.budget,
                    &acyclic_fs::CancellationToken::new(),
                )?;
                source
                    .authorize(&Query::File(task_volume::FileKey::Goal), &io)
                    .await?;
                let next = generation(&current.workspace(), &record, &io).await?;
                let publication = source
                    .publication(&base, &current, &next, maximum_changes, &io)
                    .await?;
                let position = position
                    .checked_add(1)
                    .ok_or_else(|| crate::Error::Invalid("diff cursor overflow".into()))?;
                Ok(Some((publication, (source, base, next, position, records))))
            },
        );
        Ok(Box::pin(
            futures::stream::once(async { Ok(snapshot) }).chain(changes),
        ))
    }

    async fn publication(
        &self,
        base: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        previous: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        current: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        maximum: u32,
        io: &PreparationIo,
    ) -> crate::Result<DiffPublication> {
        let permissions = self.authorize_policy(&current.workspace(), io).await?;
        for generation in [base, previous, current] {
            self.require_snapshot(&permissions, generation, io).await?;
        }
        let (diff, _allocation) = io.sdk(
            previous
                .diff_to(current, maximum, io, io.cancellation())
                .await,
        )?;
        if diff.changes().truncated {
            return Err(crate::Error::Invalid(
                "diff query exceeds path bound".into(),
            ));
        }
        let (paths, _paths_allocation) =
            io.sdk(diff.changed_paths(maximum, io, io.cancellation()).await)?;
        let mut result = DiffPublication {
            previous: self
                .reader
                .host
                .generation_ref_id(previous.id(), Some(io))?,
            generation: self.reader.host.generation_ref_id(current.id(), Some(io))?,
            paths: Vec::new(),
        };
        for change in paths {
            let mut path = String::new();
            for component in change.path.components() {
                let name = component
                    .unicode_text(
                        io.remaining().map_err(crate::Error::from)?,
                        io.cancellation(),
                    )
                    .map_err(|e| crate::Error::Invalid(e.error.to_string()))?;
                io.charge(name.work).map_err(crate::Error::from)?;
                if !path.is_empty() {
                    path.push('/');
                }
                path.push_str(&name.value);
            }
            if path.is_empty() || crate::conversation::is_internal_path(&path) {
                continue;
            }
            let before = self.endpoint(base, &path, io).await?;
            let after = self.endpoint(current, &path, io).await?;
            let largest_file_bytes = before
                .as_ref()
                .map(|(_, bytes)| *bytes)
                .into_iter()
                .chain(after.as_ref().map(|(_, bytes)| *bytes))
                .max();
            let matches_base = match (&before, &after) {
                (Some((_, left)), Some((_, right))) if left == right => io.sdk(
                    current
                        .matches_content(base, &format!("/{path}"), io, io.cancellation())
                        .await,
                )?,
                _ => false,
            };
            result.paths.push(DiffPath {
                path,
                base: before.map(|(source, _)| source),
                current: after.map(|(source, _)| source),
                largest_file_bytes,
                matches_base,
            });
        }
        super::super::access::require_current(&current.workspace(), &permissions, io).await?;
        Ok(result)
    }

    async fn endpoint(
        &self,
        generation: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
        path: &str,
        io: &PreparationIo,
    ) -> crate::Result<Option<(PinnedContextPath, u64)>> {
        let (stat, recorded) = generation
            .stat(&format!("/{path}"), io, io.cancellation())
            .await;
        recorded.map_err(crate::Error::from)?;
        match stat {
            Ok(stat) if stat.kind == acyclic_fs::kernel::FileKind::Regular => {
                let bytes = stat.logical_bytes.ok_or_else(|| {
                    crate::Error::Storage("diff endpoint has no file length".into())
                })?;
                Ok(Some((
                    PinnedContextPath {
                        root: ContextRoot {
                            volume: self.volume.clone(),
                            directory: String::new(),
                        },
                        generation: self
                            .reader
                            .host
                            .generation_ref_id(generation.id(), Some(io))?,
                        path: path.to_owned(),
                    },
                    bytes,
                )))
            }
            Ok(_)
            | Err(
                acyclic_fs::WorkspaceError::NotFound | acyclic_fs::WorkspaceError::NotDirectory,
            ) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}
