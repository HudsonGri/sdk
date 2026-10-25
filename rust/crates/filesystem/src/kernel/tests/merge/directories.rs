use super::*;

#[tokio::test]
async fn merge_request_guards_fail_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let valid = root(1, 2, 3);
    let cancellation = CancellationToken::new();

    let mut zero_changes = request(valid.clone(), valid.clone(), valid.clone());
    zero_changes.maximum_changes = 0;
    let failure = merge_error(zero_changes, &cancellation).await?;
    assert!(matches!(failure.error, MergeGenerationError::ChangeLimit));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut zero_conflicts = request(valid.clone(), valid.clone(), valid.clone());
    zero_conflicts.maximum_conflicts = 0;
    assert!(matches!(
        merge_error(zero_conflicts, &cancellation).await?.error,
        MergeGenerationError::ChangeLimit
    ));

    assert!(matches!(
        merge_error(
            request(valid.clone(), root(9, 2, 3), valid.clone()),
            &cancellation
        )
        .await?
        .error,
        MergeGenerationError::InvalidDiff
    ));
    assert!(matches!(
        merge_error(
            request(valid.clone(), valid.clone(), root(1, 9, 3)),
            &cancellation
        )
        .await?
        .error,
        MergeGenerationError::InvalidDiff
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = merge_error(request(valid.clone(), valid.clone(), valid), &cancelled).await?;
    assert!(matches!(failure.error, MergeGenerationError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn directory_scalar_conflicts_are_exact_and_bounded() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([7; 16]);
    let entries = put_tree(&store, Vec::new()).await?;
    let directory = |file, metadata, links, _| FileRecord {
        payload: FilePayload::Directory { entries },
        ..directory(file, metadata, links, 8)
    };

    // Authored metadata (the mode) changed differently on each side: a real conflict.
    let with_metadata = |metadata| FileRecord {
        metadata,
        ..directory(7, 1, 1, 8)
    };
    let metadata_conflict = (merge_directory_record_async(
        &store,
        directory_id,
        with_metadata(put_metadata(&store, timed(1, 0o755)).await?),
        with_metadata(put_metadata(&store, timed(1, 0o700)).await?),
        with_metadata(put_metadata(&store, timed(1, 0o770)).await?),
        1,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(metadata_conflict.value.record.is_none());
    assert_eq!(
        metadata_conflict.value.conflicts,
        vec![MergeConflict::File(directory_id)]
    );
    assert!(!metadata_conflict.value.truncated);
    assert_eq!(metadata_conflict.value.examined_changes, 0);

    let bounded_link_conflict = (merge_directory_record_async(
        &store,
        directory_id,
        directory(7, 1, 1, 8),
        directory(7, 1, 2, 8),
        directory(7, 1, 3, 8),
        1,
        0,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(bounded_link_conflict.value.record.is_none());
    assert!(bounded_link_conflict.value.conflicts.is_empty());
    assert!(bounded_link_conflict.value.truncated);

    let non_directory = record(7, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let result = (merge_directory_record_async(
        &store,
        directory_id,
        non_directory,
        non_directory,
        non_directory,
        1,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await;
    let failure = result
        .err()
        .ok_or_else(|| std::io::Error::other("directory merge accepted a non-directory record"))?;
    assert!(matches!(failure.error, MergeGenerationError::InvalidDiff));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn tree_mutation_preserves_exact_names_and_file_identity() -> Result<(), Box<dyn std::error::Error>>
{
    let name = LogicalName::new(NameEncoding::Utf8, b"entry".to_vec(), 16)?;
    let before = TreeEntry {
        name: name.clone(),
        file_id: FileId::from_bytes([1; 16]),
        kind: FileKind::Regular,
    };
    let after = TreeEntry {
        name: name.clone(),
        file_id: FileId::from_bytes([2; 16]),
        kind: FileKind::Directory,
    };
    assert!(matches!(
        tree_mutation(name.clone(), None, Some(after.clone())),
        Some(TreeMutation::Insert(value)) if value == after
    ));
    assert!(matches!(
        tree_mutation(name.clone(), Some(before.clone()), None),
        Some(TreeMutation::Remove { name: value, expected_file_id: Some(file_id) })
            if value == name && file_id == before.file_id
    ));
    assert!(matches!(
        tree_mutation(name.clone(), Some(before.clone()), Some(after.clone())),
        Some(TreeMutation::Replace { entry, expected_file_id })
            if entry == after && expected_file_id == before.file_id
    ));
    assert!(tree_mutation(name, None, None).is_none());
    Ok(())
}

#[tokio::test]
async fn directory_scalar_conflicts_do_not_hide_binding_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([7; 16]);
    let name = LogicalName::new(NameEncoding::Utf8, b"entry".to_vec(), 8)?;
    let mut records = Vec::new();
    for (identity, mode) in [(1, 0o755), (2, 0o700), (3, 0o770)] {
        let entries = put_tree(
            &store,
            vec![TreeEntry {
                name: name.clone(),
                file_id: FileId::from_bytes([identity; 16]),
                kind: FileKind::Regular,
            }],
        )
        .await?;
        records.push(FileRecord {
            file_id: directory_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata: put_metadata(&store, timed(1, mode)).await?,
            payload: FilePayload::Directory { entries },
        });
    }
    let [base, ours, theirs]: [FileRecord; 3] =
        records.try_into().map_err(|_| "three inputs required")?;
    for maximum_conflicts in [1, 2] {
        let result = merge_directory_record_async(
            &store,
            directory_id,
            base,
            ours,
            theirs,
            1,
            maximum_conflicts,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
        assert!(result.record.is_none());
        assert_eq!(result.examined_changes, 1);
        assert_eq!(result.truncated, maximum_conflicts == 1);
        let expected = [
            MergeConflict::File(directory_id),
            MergeConflict::Binding {
                directory_id,
                name: name.clone(),
            },
        ];
        assert_eq!(result.conflicts, expected[..maximum_conflicts as usize]);
    }
    Ok(())
}

#[tokio::test]
async fn link_count_adjustment_is_bounded_exact_and_rename_neutral()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let target_id = FileId::from_bytes([31; 16]);
    let directory_id = FileId::from_bytes([32; 16]);
    let target = record(31, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let a = LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 8)?;
    let b = LogicalName::new(NameEncoding::Utf8, b"b".to_vec(), 8)?;
    let entry = |name: LogicalName| TreeEntry {
        name,
        file_id: target_id,
        kind: FileKind::Fifo,
    };
    let base_tree = put_tree(&store, vec![entry(a.clone())]).await?;
    let empty_tree = put_tree(&store, Vec::new()).await?;
    let renamed_tree = put_tree(&store, vec![entry(b)]).await?;
    let base_directory = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Directory { entries: base_tree },
    };
    let removed_directory = FileRecord {
        payload: FilePayload::Directory {
            entries: empty_tree,
        },
        ..base_directory
    };
    let mut resolutions = BTreeMap::from([
        (
            directory_id,
            (Some(base_directory), Some(removed_directory)),
        ),
        (target_id, (Some(target), Some(target))),
    ]);
    let mut remaining_changes = 8;
    let mut work = WorkCounters::default();
    (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut remaining_changes,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut work,
    ))
    .await?;
    assert_eq!(
        resolutions
            .get(&target_id)
            .and_then(|(_, record)| *record)
            .ok_or("adjusted target record missing")?
            .link_count,
        1
    );

    // A file introduced on one side may lose its directory binding when the
    // other side wins a same-name replacement. It must not remain orphaned in
    // the merged file table merely because no binding changed relative to ours.
    let introduced = record(33, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let introduced_id = introduced.file_id;
    let mut resolutions = BTreeMap::from([(introduced_id, (None, Some(introduced)))]);
    (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut 8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .await?;
    assert_eq!(
        resolutions.get(&introduced_id).map(|(_, record)| *record),
        Some(None)
    );

    let renamed_directory = FileRecord {
        payload: FilePayload::Directory {
            entries: renamed_tree,
        },
        ..base_directory
    };
    let mut resolutions = BTreeMap::from([(
        directory_id,
        (Some(base_directory), Some(renamed_directory)),
    )]);
    let mut remaining_changes = 8;
    (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut remaining_changes,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .await?;
    assert!(!resolutions.contains_key(&target_id));

    let mut resolutions = BTreeMap::from([(
        directory_id,
        (Some(base_directory), Some(removed_directory)),
    )]);
    let limit = (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut 0,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
        &mut WorkCounters::default(),
    ))
    .await
    .err()
    .ok_or("zero link frontier unexpectedly succeeded")?;
    assert!(matches!(limit.error, MergeGenerationError::ChangeLimit));
    Ok(())
}

#[tokio::test]
async fn pruning_an_unbound_directory_recomputes_descendant_links()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let directory_id = FileId::from_bytes([90; 16]);
    let child = record(91, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let child_id = child.file_id;
    let entries = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 8)?,
            file_id: child_id,
            kind: FileKind::Fifo,
        }],
    )
    .await?;
    let directory = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Directory { entries },
    };
    let mut resolutions = BTreeMap::from([
        (directory_id, (None, Some(directory))),
        (child_id, (None, Some(child))),
    ]);
    (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut 8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        &mut WorkCounters::default(),
    ))
    .await?;
    assert!(resolutions.values().all(|(_, resolved)| resolved.is_none()));
    Ok(())
}

#[tokio::test]
async fn deleting_a_record_with_remaining_links_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let linked = record(92, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let mut resolutions = BTreeMap::from([(linked.file_id, (Some(linked), None))]);
    let failure = (adjust_test_link_counts(
        &store,
        &mut resolutions,
        &mut 8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        &mut WorkCounters::default(),
    ))
    .await
    .err()
    .ok_or("remaining-link deletion unexpectedly succeeded")?;
    assert!(matches!(failure.error, MergeGenerationError::InvalidDiff));
    Ok(())
}

#[tokio::test]
async fn pruning_deep_unbound_directories_visits_each_transition_once()
-> Result<(), Box<dyn std::error::Error>> {
    const DEPTH: u8 = 128;
    let store = MemoryObjectStore::default();
    let mut records = BTreeMap::new();
    let mut descendant = record(DEPTH, 1, 1, FilePayload::Empty, FileKind::Fifo);
    records.insert(descendant.file_id, (None, Some(descendant)));
    for index in (0..DEPTH).rev() {
        let entries = put_tree(
            &store,
            vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 8)?,
                file_id: descendant.file_id,
                kind: descendant.kind,
            }],
        )
        .await?;
        descendant = record(
            index,
            1,
            1,
            FilePayload::Directory { entries },
            FileKind::Directory,
        );
        records.insert(descendant.file_id, (None, Some(descendant)));
    }
    let mut remaining = u32::from(DEPTH) * 2;
    let budget = WorkBudget {
        items_examined: u64::from(DEPTH) * 256,
        ..WorkBudget::UNBOUNDED
    };
    let mut work = WorkCounters::UNCHARGED;
    adjust_test_link_counts(
        &store,
        &mut records,
        &mut remaining,
        DecodeLimits::default(),
        budget,
        &CancellationToken::new(),
        &mut work,
    )
    .await?;
    work.verify(budget)?;
    assert_eq!(
        remaining, 0,
        "each binding is introduced and removed exactly once"
    );
    assert!(records.values().all(|(_, resolved)| resolved.is_none()));
    Ok(())
}

#[test]
fn link_delta_insertion_accounts_for_every_copied_element() -> Result<(), Box<dyn std::error::Error>>
{
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut deltas = Owned::allocate(0, &mut accounting)?;
    for index in (0..=u8::MAX).rev() {
        add_delta(
            &mut deltas,
            FileId::from_bytes([index; 16]),
            1,
            &mut accounting,
        )?;
    }
    let inserted = accounting.spent();
    assert!(inserted.items_examined >= 256);
    assert!(
        inserted.bytes_copied >= 256 * crate::foundation::usize_to_u64(size_of::<(FileId, i64)>())
    );
    deltas.retire(&mut accounting)?;
    Ok(())
}

mod link_graph_properties {
    use super::*;
    use proptest::prelude::*;

    async fn check_graph(edges: &[u8], roots: u8) -> Result<(), Box<dyn std::error::Error>> {
        let store = MemoryObjectStore::default();
        let mut records = BTreeMap::new();
        let binding = |index: u8| -> Result<TreeEntry, Box<dyn std::error::Error>> {
            Ok(TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, vec![b'a' + index], 8)?,
                file_id: FileId::from_bytes([index + 1; 16]),
                kind: FileKind::Directory,
            })
        };
        let mut reachable = [false; 8];
        let mut links = [0_u64; 8];
        let mut root_entries = Vec::new();
        for (index, reached) in reachable.iter_mut().enumerate().take(edges.len()) {
            if roots & (1 << index) != 0 {
                *reached = true;
                links[index] += 1;
                root_entries.push(binding(u8::try_from(index)?)?);
            }
        }
        let empty = put_tree(&store, Vec::new()).await?;
        let before = record(
            0,
            1,
            1,
            FilePayload::Directory { entries: empty },
            FileKind::Directory,
        );
        let after = FileRecord {
            payload: FilePayload::Directory {
                entries: put_tree(&store, root_entries).await?,
            },
            ..before
        };
        records.insert(before.file_id, (Some(before), Some(after)));
        // Every edge points forward, so reachability and exact link counts have
        // a one-pass independent oracle, including shared descendants.
        for (index, mask) in edges.iter().enumerate() {
            let mut entries = Vec::new();
            for target in index + 1..edges.len() {
                if mask & (1 << target) != 0 {
                    entries.push(binding(u8::try_from(target)?)?);
                    if reachable[index] {
                        reachable[target] = true;
                        links[target] += 1;
                    }
                }
            }
            let value = record(
                u8::try_from(index + 1)?,
                1,
                1,
                FilePayload::Directory {
                    entries: put_tree(&store, entries).await?,
                },
                FileKind::Directory,
            );
            records.insert(value.file_id, (None, Some(value)));
        }
        let cancellation = CancellationToken::new();
        let mut work = WorkCounters::UNCHARGED;
        adjust_test_link_counts(
            &store,
            &mut records,
            &mut 256,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
            &mut work,
        )
        .await?;
        for index in 0..edges.len() {
            let id = FileId::from_bytes([u8::try_from(index + 1)?; 16]);
            let actual = records.get(&id).ok_or("missing graph record")?.1;
            assert_eq!(
                actual.is_some(),
                reachable[index],
                "node {index}, graph {edges:?}, roots {roots}"
            );
            if let Some(actual) = actual {
                assert_eq!(actual.link_count, links[index]);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn pruning_keeps_shared_children_reachable_from_an_independent_parent()
    -> Result<(), Box<dyn std::error::Error>> {
        check_graph(&[4, 4, 0], 2).await
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn pruning_matches_reachable_graph_and_shared_link_counts(
            edges in proptest::collection::vec(any::<u8>(), 1..=8), roots in any::<u8>(),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread().build()
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            runtime.block_on(check_graph(&edges, roots)).map_err(|error| TestCaseError::fail(error.to_string()))?;
        }
    }
}
