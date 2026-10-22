//! Restart records are a stream of JSON lines in bounded volume files. A
//! record may span files; neither the manifest nor a write buffer grows with
//! the number of directories. Unchanged chunks retain their existing binding.
use super::*;

const CHUNK_BYTES: usize = 64 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredManifest {
    pub(super) schema: u32,
    cut: NameCut,
    names_cut: NameCut,
    observed_generation: GenerationId,
    pages: u64,
    pending_directories: usize,
    complete: bool,
    checkpoint_chunks: u64,
}

impl StoredManifest {
    pub(super) async fn restore<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        self,
        generation: &Generation<A, O>,
    ) -> Result<NameIndexManifest, SearchError> {
        let stop = CancellationToken::new();
        let mut checkpoint: Option<NameCheckpoint> = None;
        let mut record = Vec::new();
        for ordinal in 0..self.checkpoint_chunks {
            let bytes = generation
                .read(
                    &chunk_path(ordinal),
                    CHUNK_BYTES as u64,
                    WorkBudget::UNBOUNDED,
                    &stop,
                )
                .await
                .map_err(|failure| failure.error)?
                .value;
            for part in bytes.split_inclusive(|byte| *byte == b'\n') {
                record.extend_from_slice(part);
                if record.last() == Some(&b'\n') {
                    if let Some(checkpoint) = &mut checkpoint {
                        checkpoint.restore_record(&record)?;
                    } else {
                        checkpoint = Some(NameCheckpoint::from_header(&record)?);
                    }
                    record.clear();
                }
            }
        }
        if !record.is_empty() {
            return Err(SearchError::Invalid("truncated pathname checkpoint".into()));
        }
        let checkpoint =
            checkpoint.ok_or_else(|| SearchError::Invalid("empty pathname checkpoint".into()))?;
        if checkpoint.pending_directories() != self.pending_directories {
            return Err(SearchError::Invalid(
                "pathname checkpoint progress mismatch".into(),
            ));
        }
        Ok(NameIndexManifest {
            schema: self.schema,
            cut: self.cut,
            names_cut: self.names_cut,
            observed_generation: self.observed_generation,
            pages: self.pages,
            pending_directories: self.pending_directories,
            complete: self.complete,
            checkpoint,
        })
    }
}

pub(super) async fn write_manifest<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    expected: &Generation<A, O>,
    manifest: &NameIndexManifest,
) -> Result<(), SearchError> {
    let mut buffer = Vec::with_capacity(CHUNK_BYTES);
    let mut chunks = 0;
    for record in manifest.checkpoint.records() {
        let mut record = record?;
        record.push(b'\n');
        let mut remaining = record.as_slice();
        while !remaining.is_empty() {
            let count = remaining.len().min(CHUNK_BYTES - buffer.len());
            buffer.extend_from_slice(&remaining[..count]);
            remaining = &remaining[count..];
            if buffer.len() == CHUNK_BYTES {
                write_chunk(transaction, expected, chunks, &buffer).await?;
                chunks += 1;
                buffer.clear();
            }
        }
    }
    if !buffer.is_empty() {
        write_chunk(transaction, expected, chunks, &buffer).await?;
        chunks += 1;
    }
    let stored = StoredManifest {
        schema: 2,
        cut: manifest.cut.clone(),
        names_cut: manifest.names_cut.clone(),
        observed_generation: manifest.observed_generation,
        pages: manifest.pages,
        pending_directories: manifest.pending_directories,
        complete: manifest.complete,
        checkpoint_chunks: chunks,
    };
    transaction
        .write(
            MANIFEST,
            serde_json::to_vec(&stored)?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    Ok(())
}

async fn write_chunk<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    expected: &Generation<A, O>,
    ordinal: u64,
    bytes: &[u8],
) -> Result<(), SearchError> {
    let path = chunk_path(ordinal);
    let stop = CancellationToken::new();
    match expected
        .read(&path, CHUNK_BYTES as u64, WorkBudget::UNBOUNDED, &stop)
        .await
    {
        Ok(receipt) if receipt.value.as_ref() == bytes => return Ok(()),
        Ok(_) => {}
        Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {}
        Err(failure) => return Err(failure.error.into()),
    }
    transaction
        .write(&path, bytes.to_vec(), WorkBudget::UNBOUNDED, &stop)
        .await
        .map_err(|failure| failure.error)?;
    Ok(())
}

fn chunk_path(ordinal: u64) -> String {
    format!("{ROOT}/checkpoint-{ordinal:016x}.jsonl")
}

#[cfg(all(test, feature = "memory", feature = "native-mount"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn name_search_checkpoint_pages_large_frontier_and_reuses_unchanged_chunks() {
        let fs = crate::Fs::memory();
        let stop = CancellationToken::new();
        let workspace = fs
            .create_workspace("checkpoint-pages", WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let mut walk = crate::NameWalk::<
            _,
            _,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >::exact(expected.clone(), None, crate::GitIgnorePolicy::default())
        .await
        .unwrap();
        while walk.next_page().await.unwrap().is_some() {}
        let mut checkpoint = walk.checkpoint().unwrap();
        for ordinal in 0..5000 {
            checkpoint.discover_directory(
                &format!("{}/{}/{ordinal:05}", "d".repeat(250), "e".repeat(250)),
                true,
            );
        }
        let policy = crate::GitIgnorePolicy::parse(&"*.ignored\n".repeat(150));
        for ordinal in 0..32 {
            let record = serde_json::to_vec(
                &serde_json::json!({"Policy": [format!("policy-{ordinal}"), policy]}),
            )
            .unwrap();
            checkpoint.restore_record(&record).unwrap();
        }
        let manifest = NameIndexManifest {
            schema: 2,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages: 0,
            pending_directories: 5000,
            complete: false,
            checkpoint,
        };
        // Negative control: the former inline checkpoint cannot fit the
        // unchanged manifest file budget, despite containing no candidates.
        assert!(serde_json::to_vec(&manifest).unwrap().len() as u64 > MAXIMUM_INDEX_FILE_BYTES);
        let published = NameIndex::publish(&workspace, &expected, manifest, None)
            .await
            .unwrap()
            .unwrap();
        let opened = NameIndex::open(&workspace).await.unwrap().unwrap();
        assert_eq!(opened.manifest.pending_directories, 5000);
        assert_eq!(
            serde_json::to_vec(&published.manifest.checkpoint).unwrap(),
            serde_json::to_vec(&opened.manifest.checkpoint).unwrap()
        );
        let generation = opened.generation();
        let root = generation
            .read(MANIFEST, 4096, WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let stored: StoredManifest = serde_json::from_slice(&root).unwrap();
        assert!(stored.checkpoint_chunks > 32);
        for ordinal in 0..stored.checkpoint_chunks {
            let chunk = generation
                .read(
                    &chunk_path(ordinal),
                    CHUNK_BYTES as u64,
                    WorkBudget::UNBOUNDED,
                    &stop,
                )
                .await
                .unwrap()
                .value;
            assert!(chunk.len() <= CHUNK_BYTES);
        }
        let resumed = crate::NameWalk::<
            _,
            _,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >::resume_exact(
            &workspace,
            opened.manifest.checkpoint.clone(),
            crate::GitIgnorePolicy::default(),
        )
        .await
        .unwrap();
        assert_eq!(resumed.pending_directories(), 5000);
        let republished = NameIndex::publish(&workspace, &generation, opened.manifest, None)
            .await
            .unwrap()
            .unwrap();
        // Inspect each checkpoint binding directly. A whole-generation diff
        // spends its join budget traversing unrelated bindings as well.
        for ordinal in 0..stored.checkpoint_chunks {
            let mut ctx = crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &stop);
            let (path, _) = crate::kernel::NamespacePath::parse_in_profile(
                &chunk_path(ordinal),
                workspace.profile(),
                workspace.limits(),
                &mut ctx,
                &stop,
            )
            .unwrap();
            let paths = [path];
            let before = generation
                .lookup_paths(&paths, WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            let after = republished
                .generation()
                .lookup_paths(&paths, WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            assert!(before[0].is_some());
            assert_eq!(before, after);
        }
    }
}
