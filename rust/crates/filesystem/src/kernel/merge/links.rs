async fn fold_additions<S: AsyncObjectStore>(
    store: &S,
    requested: ResolutionView<'_>,
    maximum_conflicts: u32,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    while let Some((ours_id, theirs_id)) = state.folds.value.pop() {
        let (ours_before, ours) = state.record(ours_id, accounting)?;
        let (theirs_before, theirs) = state.record(theirs_id, accounting)?;
        let (Some(ours), Some(theirs)) = (ours, theirs) else {
            return Err(FsError::InvalidDiff);
        };
        let empty = FileRecord {
            payload: FilePayload::Directory {
                entries: empty_tree_async(store, limits, accounting, cancellation).await?,
            },
            ..ours
        };
        let record = merge_directory_record_with_resolutions_async(
            store,
            ours_id,
            empty,
            ours,
            theirs,
            requested,
            maximum_conflicts,
            limits,
            state,
            accounting,
            cancellation,
        )
        .await?;
        if let Some(record) = record {
            state.replace(ours_id, (ours_before, Some(record)), accounting)?;
        }
        state.replace(theirs_id, (theirs_before, None), accounting)?;
    }
    state.file_folds.value.reverse();
    while let Some(fold) = state.file_folds.value.pop() {
        let (ours_before, ours) = state.record(fold.ours, accounting)?;
        let (theirs_before, theirs) = state.record(fold.theirs, accounting)?;
        let (Some(ours), Some(theirs)) = (ours, theirs) else {
            return Err(FsError::InvalidDiff);
        };
        let metadata = if ours.kind == theirs.kind
            && ours.payload == theirs.payload
            && ours.link_count == 1
            && theirs.link_count == 1
        {
            converge_metadata_async(
                store,
                ours.metadata,
                theirs.metadata,
                limits,
                accounting,
                cancellation,
            )
            .await?
        } else {
            None
        };
        let bytes = TreeFormat::key_owned_bytes(&fold.name);
        let allocation =
            super::persistent_point::split_allocation(&mut state.file_folds.allocation, bytes)?;
        state.file_folds.bytes = state
            .file_folds
            .bytes
            .checked_sub(bytes)
            .ok_or(WorkError::Overflow)?;
        let name = Owned::new(fold.name, allocation, bytes);
        if let Some(metadata) = metadata {
            state.replace(
                fold.ours,
                (ours_before, Some(FileRecord { metadata, ..ours })),
                accounting,
            )?;
            state.replace(fold.theirs, (theirs_before, None), accounting)?;
            name.retire(accounting)?;
        } else {
            let deferred = state.defer_binding(
                fold.directory_id,
                &name.value,
                [
                    None,
                    Some((ours.file_id, ours.kind)),
                    Some((theirs.file_id, theirs.kind)),
                ],
                maximum_conflicts,
                limits,
                accounting,
            );
            name.retire(accounting)?;
            deferred?;
        }
    }
    Ok(())
}

// Each observed binding contributes once. Sorting and coalescing use the
// canonical funded sorter; no growing suffix is shifted for each child.
fn add_delta(
    deltas: &mut Owned<Vec<(FileId, i64)>>,
    file: FileId,
    delta: i64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    deltas.ensure_for_push(usize::MAX, accounting)?;
    let work = WorkCounters {
        items_examined: 1,
        bytes_copied: crate::foundation::usize_to_u64(std::mem::size_of::<(FileId, i64)>()),
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(work)?;
    deltas.value.push((file, delta));
    accounting.record_completed(work)
}

fn coalesce_link_deltas(
    deltas: &mut [(FileId, i64)],
    accounting: &mut KernelAccounting<'_>,
) -> Result<usize, FsError> {
    super::sort_by_accounted(deltas, accounting, |a, b| a.0.cmp(&b.0))?;
    let mut retained: usize = 0;
    for index in 0..deltas.len() {
        let work = WorkCounters {
            items_examined: 1,
            bytes_copied: crate::foundation::usize_to_u64(std::mem::size_of::<(FileId, i64)>()),
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(work)?;
        let (file, delta) = *deltas.get(index).ok_or(FsError::InvalidDiff)?;
        if let Some(previous) = retained
            .checked_sub(1)
            .and_then(|index| deltas.get_mut(index))
            && previous.0 == file
        {
            previous.1 = previous.1.checked_add(delta).ok_or(WorkError::Overflow)?;
        } else {
            *deltas.get_mut(retained).ok_or(FsError::InvalidDiff)? = (file, delta);
            retained += 1;
        }
        accounting.record_completed(work)?;
    }
    Ok(retained)
}

async fn read_link_changes<S: AsyncObjectStore>(
    store: &S,
    sides: [Option<ObjectId>; 2],
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Owned<super::persistent_diff::Diff<LogicalName, super::TreeEntry>>, FsError> {
    if state.remaining_changes == 0 {
        return Err(FsError::MergeChangeLimit);
    }
    let [before, after] = sides;
    let (diff, allocation) = diff_tree_entries_with_accounting(
        store,
        before,
        after,
        state.remaining_changes,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    let bytes = diff.retained_allocation_bytes;
    let diff = Owned::new(diff, allocation, bytes);
    if diff.value.truncated {
        diff.retire(accounting)?;
        return Err(FsError::MergeChangeLimit);
    }
    if let Err(error) = state.consume(diff.value.changes.len()) {
        diff.retire(accounting)?;
        return Err(error);
    }
    Ok(diff)
}

fn retire_unlinked_record(
    record: &mut Option<FileRecord>,
    links: u64,
    pending: &mut Owned<Vec<ObjectId>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let Some(value) = record.as_mut() else {
        return if links == 0 {
            Ok(())
        } else {
            Err(FsError::InvalidDiff)
        };
    };
    if links == 0 {
        if let Some(entries) = directory_entries(*record) {
            pending.ensure_for_push(usize::MAX, accounting)?;
            let work = WorkCounters {
                items_examined: 1,
                bytes_copied: crate::foundation::usize_to_u64(std::mem::size_of::<ObjectId>()),
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(work)?;
            pending.value.push(entries);
            accounting.record_completed(work)?;
        }
        *record = None;
    } else {
        value.link_count = links;
    }
    Ok(())
}

async fn initial_link_deltas<S: AsyncObjectStore>(
    store: &S,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Owned<Vec<(FileId, i64)>>, FsError> {
    let mut deltas = Owned::allocate(0, accounting)?;
    let result = async {
        for index in 0..state.records.value.len() {
            accounting.charge_items(1)?;
            let (_, (before, after)) =
                *state.records.value.get(index).ok_or(FsError::InvalidDiff)?;
            let before = directory_entries(before);
            let after = directory_entries(after);
            if before == after {
                continue;
            }
            let sides = [before, after];
            let diff =
                read_link_changes(store, sides, limits, state, accounting, cancellation).await?;
            let result = (|| {
                for change in &diff.value.changes {
                    if let Some(before) = &change.before {
                        add_delta(&mut deltas, before.file_id, -1, accounting)?;
                    }
                    if let Some(after) = &change.after {
                        add_delta(&mut deltas, after.file_id, 1, accounting)?;
                    }
                }
                Ok::<_, FsError>(())
            })();
            diff.retire(accounting)?;
            result?;
        }
        let retained = coalesce_link_deltas(&mut deltas.value, accounting)?;
        deltas.value.truncate(retained);
        for (file, delta) in &deltas.value {
            if *delta != 0 {
                state.record(*file, accounting)?;
            }
        }
        Ok::<_, FsError>(())
    }
    .await;
    if let Err(error) = result {
        deltas.retire(accounting)?;
        return Err(error);
    }
    Ok(deltas)
}

async fn adjust_link_counts<S: AsyncObjectStore>(
    store: &S,
    limits: DecodeLimits,
    state: &mut MergeState,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let deltas = initial_link_deltas(store, limits, state, accounting, cancellation).await?;
    let mut pending = match Owned::allocate(0, accounting) {
        Ok(pending) => pending,
        Err(error) => {
            deltas.retire(accounting)?;
            return Err(error);
        }
    };
    let result = async {
        for (file, (ours, resolved)) in &mut state.records.value {
            accounting.charge_items(1)?;
            let delta = super::search::binary_search_with_accounting(
                &deltas.value,
                accounting,
                |(id, _)| id.cmp(file),
            )?
            .ok()
            .and_then(|index| deltas.value.get(index))
            .map_or(0, |(_, delta)| *delta);
            let links = i128::from(ours.map_or(0, |record| record.link_count))
                .checked_add(i128::from(delta))
                .and_then(|value| u64::try_from(value).ok())
                .ok_or(FsError::InvalidDiff)?;
            retire_unlinked_record(resolved, links, &mut pending, accounting)?;
        }
        // Every directory enters once, on its only live-to-absent transition.
        // Removing its outgoing bindings advances only their exact targets,
        // rather than rescanning every record once per level of a deleted tree.
        while let Some(entries) = pending.value.pop() {
            let diff = read_link_changes(
                store,
                [Some(entries), None],
                limits,
                state,
                accounting,
                cancellation,
            )
            .await?;
            let result = (|| {
                for change in &diff.value.changes {
                    let before = change.before.as_ref().ok_or(FsError::InvalidDiff)?;
                    let index = super::search::binary_search_with_accounting(
                        &state.records.value,
                        accounting,
                        |(id, _)| id.cmp(&before.file_id),
                    )?
                    .map_err(|_| FsError::InvalidDiff)?;
                    let resolved = &mut state
                        .records
                        .value
                        .get_mut(index)
                        .ok_or(FsError::InvalidDiff)?
                        .1
                        .1;
                    let links = resolved
                        .as_ref()
                        .ok_or(FsError::InvalidDiff)?
                        .link_count
                        .checked_sub(1)
                        .ok_or(FsError::InvalidDiff)?;
                    retire_unlinked_record(resolved, links, &mut pending, accounting)?;
                }
                Ok::<_, FsError>(())
            })();
            diff.retire(accounting)?;
            result?;
        }
        Ok::<_, FsError>(())
    }
    .await;
    pending.retire(accounting)?;
    deltas.retire(accounting)?;
    result
}
