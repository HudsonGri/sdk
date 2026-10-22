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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    chunks: u64,
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
        let mut checkpoint =
            checkpoint.ok_or_else(|| SearchError::Invalid("empty pathname checkpoint".into()))?;
        let root = policy_root();
        let mut after = None;
        loop {
            let page = generation
                .list_directory(&root, after.as_ref(), 128, WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?
                .value
                .0;
            for entry in &page.entries {
                let Some(name) = entry.name.unicode_utf8_bytes() else {
                    return Err(SearchError::Invalid(
                        "invalid checkpoint policy filename".into(),
                    ));
                };
                let name = std::str::from_utf8(name).map_err(|_| {
                    SearchError::Invalid("invalid checkpoint policy filename".into())
                })?;
                let Some(hash) = name.strip_suffix(".json") else {
                    continue;
                };
                if hash.len() != 64 {
                    continue;
                }
                if !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(SearchError::Invalid(
                        "invalid checkpoint policy filename".into(),
                    ));
                }
                let path = format!("{root}/{hash}");
                let header = read_policy_header(generation, &path).await?;
                let mut record = Vec::new();
                for ordinal in 0..header.chunks {
                    let bytes = generation
                        .read(
                            &policy_chunk(&path, ordinal),
                            CHUNK_BYTES as u64,
                            WorkBudget::UNBOUNDED,
                            &stop,
                        )
                        .await
                        .map_err(|failure| failure.error)?
                        .value;
                    record.extend_from_slice(&bytes);
                }
                checkpoint.restore_policy_record(&record, hash)?;
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
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
    previous: Option<&NameIndex<A, O>>,
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
    // The prior snapshot must still name this policy namespace at the captured
    // head. A competing publication cannot make a stale diff authoritative.
    let previous = if let Some(previous) = previous {
        let bytes = expected
            .read(
                MANIFEST,
                MAXIMUM_INDEX_FILE_BYTES,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let prior = previous
            .generation
            .read(
                MANIFEST,
                MAXIMUM_INDEX_FILE_BYTES,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        (bytes == prior).then_some(previous)
    } else {
        None
    };
    let root = policy_root();
    if previous.is_none() {
        clear_policies(transaction, expected).await?;
    }
    transaction
        .write_batch(
            &[root.as_str()],
            &[],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    for record in manifest
        .checkpoint
        .policy_records_since(previous.map(|previous| &previous.manifest.checkpoint))
    {
        let (directory, record) = record?;
        let path = format!("{root}/{}", blake3::hash(directory.as_bytes()).to_hex());
        let old_chunks = if previous.is_some() {
            match read_policy_header(expected, &path).await {
                Ok(header) => header.chunks,
                Err(SearchError::Workspace(WorkspaceError::NotFound)) => 0,
                Err(error) => return Err(error),
            }
        } else {
            0
        };
        let chunks = if let Some(record) = record {
            let chunks = record.len().div_ceil(CHUNK_BYTES) as u64;
            for (ordinal, bytes) in record.chunks(CHUNK_BYTES).enumerate() {
                transaction
                    .write(
                        &policy_chunk(&path, ordinal as u64),
                        bytes.to_vec(),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?;
            }
            transaction
                .write(
                    &format!("{path}.json"),
                    serde_json::to_vec(&PolicyFile { chunks })?,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
            chunks
        } else {
            transaction
                .remove(
                    &format!("{path}.json"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
            0
        };
        for ordinal in chunks..old_chunks {
            transaction
                .remove(
                    &policy_chunk(&path, ordinal),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?;
        }
    }
    let stored = StoredManifest {
        schema: 3,
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

fn policy_root() -> String {
    format!("{ROOT}/policies")
}

async fn clear_policies<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut crate::Transaction<A, O>,
    expected: &Generation<A, O>,
) -> Result<(), SearchError> {
    let root = policy_root();
    let stop = CancellationToken::new();
    let mut after = None;
    loop {
        let page = match expected
            .list_directory(&root, after.as_ref(), 128, WorkBudget::UNBOUNDED, &stop)
            .await
        {
            Ok(receipt) => receipt.value.0,
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => return Ok(()),
            Err(failure) => return Err(failure.error.into()),
        };
        for entry in &page.entries {
            let name = entry
                .name
                .unicode_text(WorkBudget::UNBOUNDED, &stop)
                .map_err(|failure| failure.error)?
                .value;
            transaction
                .remove(&format!("{root}/{name}"), WorkBudget::UNBOUNDED, &stop)
                .await
                .map_err(|failure| failure.error)?;
        }
        if !page.has_more {
            return Ok(());
        }
        after = page.entries.last().map(|entry| entry.name.clone());
    }
}

fn policy_chunk(path: &str, ordinal: u64) -> String {
    format!("{path}-{ordinal:016x}.jsonl")
}

async fn read_policy_header<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
    path: &str,
) -> Result<PolicyFile, SearchError> {
    let bytes = generation
        .read(
            &format!("{path}.json"),
            128,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    Ok(serde_json::from_slice(&bytes)?)
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
    async fn name_search_checkpoint_persists_only_changed_policies_and_removes_retired_files() {
        let fs = crate::Fs::memory();
        let stop = CancellationToken::new();
        let workspace = fs
            .create_workspace("policy-deltas", WorkBudget::UNBOUNDED, &stop)
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
        for ordinal in 0..64 {
            let record = serde_json::to_vec(&serde_json::json!({"Policy": [format!("d{ordinal:02}"), crate::GitIgnorePolicy::parse("*.ignored")]})).unwrap();
            checkpoint.restore_record(&record).unwrap();
        }
        let manifest = NameIndexManifest {
            schema: 3,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages: 0,
            pending_directories: 0,
            complete: true,
            checkpoint,
        };
        let initial = NameIndex::publish(&workspace, &expected, manifest, None, None)
            .await
            .unwrap()
            .unwrap();
        let mut manifest = initial.manifest.clone();
        manifest.checkpoint.forget_subtree("d00");
        let record = serde_json::to_vec(
            &serde_json::json!({"Policy": ["d01", crate::GitIgnorePolicy::parse("*.changed")]}),
        )
        .unwrap();
        manifest.checkpoint.restore_record(&record).unwrap();
        assert_eq!(
            manifest
                .checkpoint
                .policy_records_since(Some(&initial.manifest.checkpoint))
                .count(),
            2
        );
        // Negative control: a complete rewrite serializes every remaining policy.
        assert_eq!(manifest.checkpoint.policy_records_since(None).count(), 63);
        let updated = NameIndex::publish(
            &workspace,
            &initial.generation(),
            manifest,
            None,
            Some(&initial),
        )
        .await
        .unwrap()
        .unwrap();
        let reopened = NameIndex::open(&workspace).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_vec(&updated.manifest.checkpoint).unwrap(),
            serde_json::to_vec(&reopened.manifest.checkpoint).unwrap()
        );
        let removed = format!("{}/{}", policy_root(), blake3::hash(b"d00").to_hex());
        assert!(matches!(
            read_policy_header(&updated.generation(), &removed).await,
            Err(SearchError::Workspace(WorkspaceError::NotFound))
        ));
        assert!(
            matches!(updated.generation().read(&policy_chunk(&removed, 0), CHUNK_BYTES as u64, WorkBudget::UNBOUNDED, &stop).await,
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound))
        );
        for directory in ["d02", "d31", "d63"] {
            let path = format!(
                "{}/{}",
                policy_root(),
                blake3::hash(directory.as_bytes()).to_hex()
            );
            for path in [format!("{path}.json"), policy_chunk(&path, 0)] {
                let mut ctx =
                    crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &stop);
                let (path, _) = crate::kernel::NamespacePath::parse_in_profile(
                    &path,
                    workspace.profile(),
                    workspace.limits(),
                    &mut ctx,
                    &stop,
                )
                .unwrap();
                let paths = [path];
                let before = initial
                    .generation()
                    .lookup_paths(&paths, WorkBudget::UNBOUNDED, &stop)
                    .await
                    .unwrap()
                    .value;
                let after = updated
                    .generation()
                    .lookup_paths(&paths, WorkBudget::UNBOUNDED, &stop)
                    .await
                    .unwrap()
                    .value;
                assert!(before[0].is_some());
                assert_eq!(before, after, "rewrote an unchanged policy file");
            }
        }
        // A fresh traversal replaces the current policy set. Older snapshots
        // retain their files through generation pins; the live tree retains no
        // obsolete policy namespace or historical delta chain.
        let mut empty = updated.manifest.clone();
        for ordinal in 0..64 {
            empty.checkpoint.forget_subtree(&format!("d{ordinal:02}"));
        }
        let reset = NameIndex::publish(&workspace, &updated.generation(), empty, None, None)
            .await
            .unwrap()
            .unwrap();
        let files = reset
            .generation()
            .list_directory(&policy_root(), None, 128, WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value
            .0;
        assert!(files.entries.is_empty());
        assert_eq!(
            NameIndex::open(&workspace)
                .await
                .unwrap()
                .unwrap()
                .manifest
                .checkpoint
                .policy_records_since(None)
                .count(),
            0
        );
        assert!(
            initial
                .manifest
                .checkpoint
                .eligible(&crate::GitIgnorePolicy::default(), "d00/file.ignored")
                == false
        );
    }

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
            schema: 3,
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
        let published = NameIndex::publish(&workspace, &expected, manifest, None, None)
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
        let republished = NameIndex::publish(
            &workspace,
            &generation,
            opened.manifest.clone(),
            None,
            Some(&opened),
        )
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
