use super::*;

async fn prepare_tables(
    store: &MemoryObjectStore,
    tables: [ObjectId; 3],
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<FileTablePreparation, FsError> {
    let [base, ours, theirs] = tables.map(|file_table| GenerationRoot {
        file_table,
        ..root(1, 9, 1)
    });
    prepare_file_tables_core(
        store,
        MergeFileTableRequest {
            base: &base,
            ours: &ours,
            theirs: &theirs,
            maximum_changes: 32,
            maximum_conflicts: 8,
            content: ContentMerge::Extents,
        },
        ResolutionView::Entries(&[]),
        DecodeLimits::default(),
        accounting,
        cancellation,
    )
    .await
}

async fn conflicting_tables(
    store: &MemoryObjectStore,
) -> Result<([ObjectId; 3], [FileRecord; 3]), Box<dyn std::error::Error>> {
    let base = FileRecord {
        metadata: put_metadata(store, timed(1, 0o644)).await?,
        ..record(1, 1, 1, FilePayload::Empty, FileKind::Fifo)
    };
    let ours = FileRecord {
        metadata: put_metadata(store, timed(1, 0o600)).await?,
        ..base
    };
    let theirs = FileRecord {
        metadata: put_metadata(store, timed(1, 0o640)).await?,
        ..base
    };
    let accepted_before = FileRecord {
        file_id: FileId::from_bytes([2; 16]),
        ..base
    };
    let accepted_after = FileRecord {
        metadata: ours.metadata,
        ..accepted_before
    };
    Ok((
        [
            put_table(store, vec![base, accepted_before]).await?,
            put_table(store, vec![ours, accepted_before]).await?,
            put_table(store, vec![theirs, accepted_after]).await?,
        ],
        [base, theirs, accepted_after],
    ))
}

async fn conflicting_replacement_tables(
    store: &MemoryObjectStore,
) -> Result<([ObjectId; 3], [FileRecord; 3]), Box<dyn std::error::Error>> {
    let metadata = put_metadata(store, timed(1, 0o644)).await?;
    let regular = |file, bytes: &[u8]| -> Result<FileRecord, Box<dyn std::error::Error>> {
        Ok(FileRecord {
            metadata,
            ..record(
                file,
                1,
                1,
                FilePayload::InlineRegular(crate::kernel::InlineFileData::new(bytes)?),
                FileKind::Regular,
            )
        })
    };
    let base = regular(1, b"base")?;
    let ours = regular(1, b"ours")?;
    let theirs = regular(2, b"theirs")?;
    let mut roots = Vec::new();
    for file in [base, ours, theirs] {
        let entries = put_tree(
            store,
            vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"saved".to_vec(), 8)?,
                file_id: file.file_id,
                kind: FileKind::Regular,
            }],
        )
        .await?;
        roots.push(
            put_table(
                store,
                vec![
                    file,
                    FileRecord {
                        metadata,
                        payload: FilePayload::Directory { entries },
                        ..directory(9, 1, 1, 1)
                    },
                ],
            )
            .await?,
        );
    }
    Ok((
        roots.try_into().map_err(|_| "missing replacement roots")?,
        [
            base,
            ours,
            FileRecord {
                file_id: base.file_id,
                ..theirs
            },
        ],
    ))
}

#[tokio::test]
async fn retained_replacement_frontier_preserves_its_name_rebind_for_every_selection()
-> Result<(), Box<dyn std::error::Error>> {
    for (selection, selected) in [
        (
            MergeConflictResolution::Select(MergeConflictSide::Base),
            Some(0),
        ),
        (
            MergeConflictResolution::Select(MergeConflictSide::Ours),
            Some(1),
        ),
        (
            MergeConflictResolution::Select(MergeConflictSide::Theirs),
            Some(2),
        ),
        (MergeConflictResolution::File(None), None),
    ] {
        let store = MemoryObjectStore::default();
        let cancellation = CancellationToken::new();
        let (tables, records) = conflicting_replacement_tables(&store).await?;
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut prepared = prepare_tables(&store, tables, &mut accounting, &cancellation).await?;
        let key = MergeConflict::File(records[0].file_id);
        assert_eq!(prepared.state.conflicts.value, vec![key.clone()]);
        let entries = [ResolutionEntry {
            key: &key,
            value: &selection,
        }];
        assert!(
            prepared
                .resolve(
                    ResolutionView::Entries(&entries),
                    &store,
                    DecodeLimits::default(),
                    &mut accounting,
                    &cancellation
                )
                .await?
        );
        assert_eq!(
            prepared
                .state
                .record(records[0].file_id, &mut accounting)?
                .1,
            selected.map(|index| records[index])
        );
        assert!(
            prepared
                .state
                .record(FileId::from_bytes([2; 16]), &mut accounting)?
                .1
                .is_none()
        );
        let expected_entries = if selected.is_some() {
            vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"saved".to_vec(), 8)?,
                file_id: records[0].file_id,
                kind: FileKind::Regular,
            }]
        } else {
            Vec::new()
        };
        let expected = put_tree(&store, expected_entries).await?;
        let (_, directory) = prepared
            .state
            .record(FileId::from_bytes([9; 16]), &mut accounting)?;
        assert_eq!(directory_entries(directory), Some(expected));
        let (outcome, _) = prepared
            .finish(
                &store,
                DecodeLimits::default(),
                &mut accounting,
                &cancellation,
            )
            .await?;
        assert!(matches!(outcome, MergeFileTableOutcome::Prepared(_)));
    }
    Ok(())
}

#[tokio::test]
async fn retained_frontier_resolves_files_without_rereading_inputs_or_losing_accepted_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (tables, [base, theirs, accepted_after]) = conflicting_tables(&store).await?;
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut prepared = prepare_tables(&store, tables, &mut accounting, &cancellation).await?;
    assert_eq!(
        prepared.state.conflicts.value,
        vec![MergeConflict::File(base.file_id)]
    );
    assert_eq!(
        prepared
            .state
            .record(accepted_after.file_id, &mut accounting)?
            .1,
        Some(accepted_after)
    );
    assert!(
        !prepared
            .resolve(
                ResolutionView::Entries(&[]),
                &store,
                DecodeLimits::default(),
                &mut accounting,
                &cancellation
            )
            .await?
    );
    let key = MergeConflict::File(base.file_id);
    let resolution = MergeConflictResolution::Select(MergeConflictSide::Theirs);
    let entries = [ResolutionEntry {
        key: &key,
        value: &resolution,
    }];
    let before = accounting.spent();
    assert!(
        prepared
            .resolve(
                ResolutionView::Entries(&entries),
                &store,
                DecodeLimits::default(),
                &mut accounting,
                &cancellation
            )
            .await?
    );
    assert_eq!(
        accounting.spent().object_bytes_read,
        before.object_bytes_read
    );
    let (outcome, _) = prepared
        .finish(
            &store,
            DecodeLimits::default(),
            &mut accounting,
            &cancellation,
        )
        .await?;
    let MergeFileTableOutcome::Prepared(table) = outcome else {
        return Err("resolved frontier stayed conflicted".into());
    };
    let (diff, allocation) = diff_file_records_with_accounting(
        &store,
        Some(tables[1]),
        Some(table),
        8,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
    )
    .await?;
    assert_eq!(diff.changes.len(), 2);
    assert_eq!(diff.changes[0].after, Some(theirs));
    assert_eq!(diff.changes[1].after, Some(accepted_after));
    let bytes = diff.retained_allocation_bytes;
    Owned::new(diff, allocation, bytes).retire(&mut accounting)?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_frontier(
    store: &MemoryObjectStore,
    tables: [ObjectId; 3],
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let mut accounting = KernelAccounting::native(original, cancellation);
    let mut prepared = prepare_tables(store, tables, &mut accounting, cancellation).await?;
    let pending = &prepared.state.pending_files;
    assert!(pending.bytes >= std::mem::size_of::<PendingFile>() as u64);
    assert!(
        matches!(&pending.allocation, KernelOwnedAllocation::NativeOriginal(allocation)
        if allocation.bytes() == pending.bytes && allocation.work().shares_ledger(original))
    );
    let pending = &prepared.state.pending_rebindings;
    if !pending.value.is_empty() {
        assert!(pending.bytes > std::mem::size_of::<FileFold>() as u64);
        assert!(
            matches!(&pending.allocation, KernelOwnedAllocation::NativeOriginal(allocation)
            if allocation.bytes() == pending.bytes && allocation.work().shares_ledger(original))
        );
    }
    let key = MergeConflict::File(FileId::from_bytes([1; 16]));
    let resolution = MergeConflictResolution::Select(MergeConflictSide::Theirs);
    let entries = [ResolutionEntry {
        key: &key,
        value: &resolution,
    }];
    let resolved = prepared
        .resolve(
            ResolutionView::Entries(&entries),
            store,
            DecodeLimits::default(),
            &mut accounting,
            cancellation,
        )
        .await;
    if let Err(error) = resolved {
        prepared.state.retire(&mut accounting)?;
        return Err(error);
    }
    assert_eq!(prepared.state.conflicts.bytes, 0);
    assert_eq!(prepared.state.pending_files.bytes, 0);
    assert_eq!(prepared.state.pending_bindings.bytes, 0);
    assert_eq!(prepared.state.pending_rebindings.bytes, 0);
    let (outcome, _) = prepared
        .finish(
            store,
            DecodeLimits::default(),
            &mut accounting,
            cancellation,
        )
        .await?;
    assert!(matches!(outcome, MergeFileTableOutcome::Prepared(_)));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn retained_frontier_preserves_original_native_custody_at_every_allocation_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    for replacement in [false, true] {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let setup = original.receipt().allocation_operations;
        let required;
        {
            let store = MemoryObjectStore::default();
            let (tables, _) = if replacement {
                conflicting_replacement_tables(&store).await?
            } else {
                conflicting_tables(&store).await?
            };
            native_frontier(&store, tables, &original, &cancellation).await?;
            required = original.receipt().allocation_operations - setup;
        }
        assert!(required > 0);
        assert_eq!(
            original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
        for cut in 0..required {
            let limit = setup + cut;
            let original = crate::NativeMaterializationWork::new(
                WorkBudget {
                    allocation_operations: limit,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            {
                let store = MemoryObjectStore::default();
                let (tables, _) = if replacement {
                    conflicting_replacement_tables(&store).await?
                } else {
                    conflicting_tables(&store).await?
                };
                assert!(
                    native_frontier(&store, tables, &original, &cancellation)
                        .await
                        .is_err(),
                    "cut {cut}"
                );
            }
            assert!(original.receipt().allocation_operations <= limit);
            assert_eq!(
                original.live_allocation_bytes(),
                crate::NativeMaterializationWork::accounting_state_bytes(),
                "cut {cut}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn retained_directory_frontier_keeps_accepted_bindings_across_scalar_and_name_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let base_metadata = put_metadata(&store, timed(1, 0o755)).await?;
    let ours_metadata = put_metadata(&store, timed(1, 0o700)).await?;
    let theirs_metadata = put_metadata(&store, timed(1, 0o770)).await?;
    let entry = |name: &[u8], file| -> Result<TreeEntry, Box<dyn std::error::Error>> {
        Ok(TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, name.to_vec(), 16)?,
            file_id: FileId::from_bytes([file; 16]),
            kind: FileKind::Fifo,
        })
    };
    let trees = [
        put_tree(&store, vec![entry(b"shared", 1)?]).await?,
        put_tree(&store, vec![entry(b"shared", 2)?]).await?,
        put_tree(&store, vec![entry(b"accepted", 4)?, entry(b"shared", 3)?]).await?,
    ];
    let directory = |entries, metadata| FileRecord {
        metadata,
        payload: FilePayload::Directory { entries },
        ..directory(9, 1, 1, 1)
    };
    let fifo = |id| FileRecord {
        metadata: base_metadata,
        ..record(id, 1, 1, FilePayload::Empty, FileKind::Fifo)
    };
    let tables = [
        put_table(&store, vec![fifo(1), directory(trees[0], base_metadata)]).await?,
        put_table(&store, vec![fifo(2), directory(trees[1], ours_metadata)]).await?,
        put_table(
            &store,
            vec![fifo(3), fifo(4), directory(trees[2], theirs_metadata)],
        )
        .await?,
    ];
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut prepared = prepare_tables(&store, tables, &mut accounting, &cancellation).await?;
    assert_eq!(prepared.state.conflicts.value.len(), 2);
    let resolutions: BTreeMap<_, _> = prepared
        .state
        .conflicts
        .value
        .iter()
        .cloned()
        .map(|key| {
            (
                key,
                MergeConflictResolution::Select(MergeConflictSide::Theirs),
            )
        })
        .collect();
    let indexed = index_resolutions(resolutions.iter(), &mut accounting)?;
    assert!(
        prepared
            .resolve(
                ResolutionView::Entries(&indexed.value),
                &store,
                DecodeLimits::default(),
                &mut accounting,
                &cancellation
            )
            .await?
    );
    indexed.retire(&mut accounting)?;
    let (_, resolved) = prepared
        .state
        .record(FileId::from_bytes([9; 16]), &mut accounting)?;
    let resolved = resolved.ok_or("missing retained directory")?;
    assert_eq!(resolved.metadata, theirs_metadata);
    let entries = directory_entries(Some(resolved)).ok_or("missing accepted tree")?;
    assert_eq!(entries, trees[2]);
    let (outcome, _) = prepared
        .finish(
            &store,
            DecodeLimits::default(),
            &mut accounting,
            &cancellation,
        )
        .await?;
    assert!(matches!(outcome, MergeFileTableOutcome::Prepared(_)));
    Ok(())
}

#[tokio::test]
async fn replacement_index_charges_its_final_ordering_before_completion()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let inline = crate::kernel::InlineFileData::new(b"body")?;
    let regular = |id| {
        record(
            id,
            1,
            1,
            FilePayload::InlineRegular(inline),
            FileKind::Regular,
        )
    };
    let mut trees = Vec::new();
    for ids in [[3, 1], [4, 2]] {
        let mut entries = Vec::new();
        for (name, id) in [b"a", b"b"].into_iter().zip(ids) {
            entries.push(TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, name.to_vec(), 1)?,
                file_id: regular(id).file_id,
                kind: FileKind::Regular,
            });
        }
        trees.push(put_tree(&store, entries).await?);
    }
    let changes = [
        crate::kernel::persistent_diff::ValueChange {
            key: regular(2).file_id,
            before: None,
            after: Some(regular(2)),
        },
        crate::kernel::persistent_diff::ValueChange {
            key: regular(4).file_id,
            before: None,
            after: Some(regular(4)),
        },
        crate::kernel::persistent_diff::ValueChange {
            key: regular(9).file_id,
            before: Some(FileRecord {
                payload: FilePayload::Directory { entries: trees[0] },
                ..directory(9, 1, 1, 1)
            }),
            after: Some(FileRecord {
                payload: FilePayload::Directory { entries: trees[1] },
                ..directory(9, 1, 1, 1)
            }),
        },
    ];
    let mut setup = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut state = MergeState::new(32, &mut setup)?;
    state.replacements = Owned::allocate(2, &mut setup)?;
    for id in [1, 3] {
        state.replacements.value.push((
            regular(id).file_id,
            [Some(regular(id)), Some(regular(id)), None],
        ));
    }
    let mut measured = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let index = replacement_index(
        &store,
        &changes,
        1,
        DecodeLimits::default(),
        &mut state,
        &mut measured,
        &cancellation,
    )
    .await?;
    assert_eq!(
        index
            .value
            .iter()
            .map(|entry| entry.replaced)
            .collect::<Vec<_>>(),
        vec![regular(1).file_id, regular(3).file_id]
    );
    // Independent receipt for this fixed two-name, reverse-identity history:
    // eighteen examinations build the index and two order it. Deriving the
    // denial budget from this invocation would let an unmetered sort lower
    // both the receipt and its own oracle.
    const ITEMS: u64 = 20;
    assert_eq!(measured.spent().items_examined, ITEMS);
    index.retire(&mut measured)?;
    let mut denied = KernelAccounting::standalone(
        WorkBudget {
            items_examined: ITEMS - 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let result = replacement_index(
        &store,
        &changes,
        1,
        DecodeLimits::default(),
        &mut state,
        &mut denied,
        &cancellation,
    )
    .await;
    if let Ok(index) = result {
        index.retire(&mut denied)?;
        panic!("ordering escaped its original item bound");
    }
    assert!(denied.spent().items_examined < ITEMS);
    assert_eq!(
        denied
            .standalone_parts()
            .ok_or("missing ledger")?
            .0
            .live_bytes(),
        0
    );
    state.retire(&mut setup)?;
    Ok(())
}

#[tokio::test]
async fn retained_merge_program_round_trips_without_replaying_either_input_diff()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (tables, [base, theirs, accepted]) = conflicting_tables(&store).await?;
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let tables_prepared = prepare_tables(&store, tables, &mut accounting, &cancellation).await?;
    let program = GenerationMergePreparation {
        tables: tables_prepared,
        checkpoint: MergeCheckpointContext {
            base_generation: object(ObjectKind::GenerationRoot, 10),
            ours_generation: Some(object(ObjectKind::GenerationRoot, 11)),
            ours_table: tables[1],
            theirs_generation: object(ObjectKind::GenerationRoot, 12),
            retain_theirs_parent: true,
            volume_id: crate::VolumeId::from_bytes([1; 16]),
            root_file_id: FileId::from_bytes([9; 16]),
            required_features: 0,
        },
    };
    let (bytes, allocation) = program.encode_program(DecodeLimits::default(), &mut accounting)?;
    let length = crate::foundation::usize_to_u64(bytes.capacity());
    let owned = Owned::new(bytes, allocation, length);
    assert_program_decode_custody(&owned.value)?;
    let mut resumed = GenerationMergePreparation::decode_program(
        &owned.value,
        DecodeLimits::default(),
        &mut accounting,
    )?;
    assert_eq!(resumed.conflicts(), program.conflicts());
    assert_eq!(
        resumed.records().collect::<Vec<_>>(),
        program.records().collect::<Vec<_>>()
    );
    let (canonical, allocation) =
        resumed.encode_program(DecodeLimits::default(), &mut accounting)?;
    assert_eq!(canonical, owned.value);
    let length = crate::foundation::usize_to_u64(canonical.capacity());
    Owned::new(canonical, allocation, length).retire(&mut accounting)?;
    program.retire(&mut accounting)?;
    let key = MergeConflict::File(base.file_id);
    let choices = [(
        key,
        MergeConflictResolution::Select(MergeConflictSide::Theirs),
    )];
    let reads = accounting.spent().backend_read_operations;
    assert!(
        resumed
            .resolve_entries(
                &store,
                choices.iter().map(|(k, v)| (k, v)),
                DecodeLimits::default(),
                &mut accounting,
                &cancellation
            )
            .await?
    );
    assert_eq!(accounting.spent().backend_read_operations, reads);
    let records = resumed.records().collect::<Vec<_>>();
    assert!(records.contains(&theirs));
    assert!(records.contains(&accepted));
    resumed.retire(&mut accounting)?;
    owned.retire(&mut accounting)?;
    Ok(())
}

fn assert_program_decode_custody(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    // Every truncated section must retire all sections decoded before it.
    for end in 0..bytes.len() {
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        assert!(
            GenerationMergePreparation::decode_program(
                &bytes[..end],
                DecodeLimits::default(),
                &mut accounting
            )
            .is_err()
        );
        assert_eq!(
            accounting
                .standalone_parts()
                .ok_or("missing ledger")?
                .0
                .live_bytes(),
            0,
            "prefix {end}"
        );
    }
    let mut required = None;
    let mut limit = u64::MAX;
    let mut setup = 0;
    loop {
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: limit,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        if required.is_none() {
            setup = original.receipt().allocation_operations;
        } else {
            assert_eq!(original.receipt().allocation_operations, setup);
        }
        {
            let mut accounting = KernelAccounting::native(&original, &cancellation);
            let result = GenerationMergePreparation::decode_program(
                bytes,
                DecodeLimits::default(),
                &mut accounting,
            );
            if required.is_none() {
                result?.retire(&mut accounting)?;
                let count = original.receipt().allocation_operations;
                assert!(
                    count > setup,
                    "decoded vectors must be admitted before allocation"
                );
                required = Some(count);
                limit = setup;
            } else {
                assert!(
                    result.is_err(),
                    "decode allocation cut {limit} escaped admission"
                );
                assert!(original.receipt().allocation_operations <= limit);
                limit += 1;
            }
        }
        assert_eq!(
            original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes(),
            "decode allocation cut {limit}"
        );
        if Some(limit) == required {
            break;
        }
    }
    Ok(())
}
