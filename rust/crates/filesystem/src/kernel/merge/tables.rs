#[derive(Clone, Copy)]
pub(crate) struct MergeFileTableRequest<'a> {
    pub(crate) base: &'a GenerationRoot,
    pub(crate) ours: &'a GenerationRoot,
    pub(crate) theirs: &'a GenerationRoot,
    pub(crate) maximum_changes: u32,
    pub(crate) maximum_conflicts: u32,
    pub(crate) content: ContentMerge,
}

enum MergeFileTableOutcome {
    Prepared(ObjectId),
    Conflicted {
        conflicts: Vec<MergeConflict>,
        truncated: bool,
    },
}

impl MergeFileTableRequest<'_> {
    fn validate(self, accounting: &KernelAccounting<'_>) -> Result<(), FsError> {
        accounting.check()?;
        if self.maximum_changes == 0 || self.maximum_conflicts == 0 {
            return Err(FsError::MergeChangeLimit);
        }
        if self.base.volume_id != self.ours.volume_id
            || self.base.volume_id != self.theirs.volume_id
            || self.base.root_file_id != self.ours.root_file_id
            || self.base.root_file_id != self.theirs.root_file_id
        {
            return Err(FsError::InvalidDiff);
        }
        Ok(())
    }
}

#[cfg(feature = "native-mount")]
pub(crate) async fn merge_file_tables_with_accounting<S: AsyncObjectStore>(
    store: &S,
    request: MergeFileTableRequest<'_>,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<ObjectId>, FsError> {
    let (outcome, allocation) = merge_file_tables_core(
        store,
        request,
        ResolutionView::DistinctAdditions,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    match outcome {
        MergeFileTableOutcome::Prepared(table) => Ok(Some(table)),
        MergeFileTableOutcome::Conflicted { conflicts, .. } => {
            let bytes = conflicts
                .capacity()
                .checked_mul(std::mem::size_of::<MergeConflict>())
                .ok_or(WorkError::Overflow)?;
            let bytes = conflicts.iter().try_fold(bytes, |bytes, conflict| {
                let nested = match conflict {
                    MergeConflict::File(_) => 0,
                    MergeConflict::Binding { name, .. } => name.retained_bytes(),
                };
                bytes.checked_add(nested).ok_or(WorkError::Overflow)
            })?;
            Owned::new(conflicts, allocation, bytes as u64).retire(accounting)?;
            Ok(None)
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
async fn prepare_file_tables_core<'a, 'accounting, S: AsyncObjectStore>(
    store: &'a S,
    request: MergeFileTableRequest<'a>,
    resolutions: ResolutionView<'a>,
    limits: DecodeLimits,
    accounting: &'a mut KernelAccounting<'accounting>,
    cancellation: &'a CancellationToken,
) -> Result<FileTablePreparation, FsError> {
    request.validate(accounting)?;
    let mut frame = None;
    let (future, bytes) = crate::heap_future::in_heap_admitted(
        (&mut *accounting, &mut frame),
        |(accounting, frame), bytes| {
            **frame = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
            Ok::<_, FsError>(())
        },
        |(accounting, _)| async move {
            let mut state = MergeState::new(request.maximum_changes, accounting)?;
            state.content = request.content;
            let result = async {
                let (ours, custody) = diff_file_records_with_accounting(
                    store,
                    Some(request.base.file_table),
                    Some(request.ours.file_table),
                    request.maximum_changes,
                    limits,
                    accounting,
                    cancellation,
                )
                .await?;
                let bytes = ours.retained_allocation_bytes;
                let ours = Owned::new(ours, custody, bytes);
                let result = async {
                    let (theirs, custody) = diff_file_records_with_accounting(
                        store,
                        Some(request.base.file_table),
                        Some(request.theirs.file_table),
                        request.maximum_changes,
                        limits,
                        accounting,
                        cancellation,
                    )
                    .await?;
                    let bytes = theirs.retained_allocation_bytes;
                    let theirs = Owned::new(theirs, custody, bytes);
                    let result = async {
                        if ours.value.truncated || theirs.value.truncated {
                            return Err(FsError::MergeChangeLimit);
                        }
                        let mut indices = [0, 0];
                        while let Some([left, right]) =
                            next_changes(&ours.value.changes, &theirs.value.changes, &mut indices)
                        {
                            accounting.charge(WorkCounters {
                                items_examined: 1,
                                ..WorkCounters::UNCHARGED
                            })?;
                            // The same frontier visit both admits and plans this
                            // file. Directory bindings consume the same remaining
                            // allowance; a later denial leaves only unpublished
                            // immutable objects, never a partial generation.
                            state.consume(1)?;
                            let file = left.or(right).ok_or(FsError::InvalidDiff)?.key;
                            let base = left.or(right).and_then(|change| change.before);
                            let ours_value = left.map_or(base, |change| change.after);
                            let theirs_value = right.map_or(base, |change| change.after);
                            let mut resolved = resolve_optional(base, ours_value, theirs_value);
                            if matches!(resolved, OptionalResolution::Conflict)
                                && base.is_none()
                                && let (Some(a), Some(b)) = (ours_value, theirs_value)
                                && a.kind == b.kind
                                && a.payload == b.payload
                                && let Some(metadata) = converge_metadata_async(
                                    store,
                                    a.metadata,
                                    b.metadata,
                                    limits,
                                    accounting,
                                    cancellation,
                                )
                                .await?
                            {
                                resolved = OptionalResolution::Resolved(Some(FileRecord {
                                    metadata,
                                    ..a
                                }));
                            }
                            let base = if base.is_none()
                                && matches!(resolved, OptionalResolution::Conflict)
                                && is_directory(ours_value)
                                && is_directory(theirs_value)
                            {
                                Some(FileRecord {
                                    payload: FilePayload::Directory {
                                        entries: empty_tree_async(
                                            store,
                                            limits,
                                            accounting,
                                            cancellation,
                                        )
                                        .await?,
                                    },
                                    ..ours_value.ok_or(FsError::InvalidDiff)?
                                })
                            } else {
                                base
                            };
                            if is_directory(base)
                                && is_directory(ours_value)
                                && is_directory(theirs_value)
                                && (matches!(resolved, OptionalResolution::Conflict)
                                    || resolutions.has_bindings(file, accounting)?)
                            {
                                if state.remaining_changes == 0 {
                                    return Err(FsError::MergeChangeLimit);
                                }
                                let record = merge_directory_record_with_resolutions_async(
                                    store,
                                    file,
                                    base.ok_or(FsError::InvalidDiff)?,
                                    ours_value.ok_or(FsError::InvalidDiff)?,
                                    theirs_value.ok_or(FsError::InvalidDiff)?,
                                    resolutions,
                                    request.maximum_conflicts,
                                    limits,
                                    &mut state,
                                    accounting,
                                    cancellation,
                                )
                                .await?;
                                if let Some(record) = record {
                                    resolved = OptionalResolution::Resolved(Some(record));
                                } else {
                                    state.records.ensure_for_push(usize::MAX, accounting)?;
                                    state.records.value.push((file, (ours_value, ours_value)));
                                    continue;
                                }
                            }
                            if matches!(resolved, OptionalResolution::Conflict)
                                && resolutions.converges_additions()
                                && replaceable([base, ours_value, theirs_value])
                            {
                                state.replacements.ensure_for_push(usize::MAX, accounting)?;
                                state
                                    .replacements
                                    .value
                                    .push((file, [base, ours_value, theirs_value]));
                                state.records.ensure_for_push(usize::MAX, accounting)?;
                                state.records.value.push((file, (ours_value, ours_value)));
                                continue;
                            }
                            let settled = settle_record(
                                store,
                                file,
                                [base, ours_value, theirs_value],
                                resolved,
                                resolutions,
                                limits,
                                &mut state,
                                accounting,
                                cancellation,
                            )
                            .await?;
                            let after = if let Some(after) = settled {
                                after
                            } else {
                                state.defer_file(
                                    file,
                                    [base, ours_value, theirs_value],
                                    false,
                                    request.maximum_conflicts,
                                    accounting,
                                )?;
                                ours_value
                            };
                            state.records.ensure_for_push(usize::MAX, accounting)?;
                            state.records.value.push((file, (ours_value, after)));
                        }
                        settle_replacements(
                            store,
                            [&ours.value.changes, &theirs.value.changes],
                            resolutions,
                            request.maximum_conflicts,
                            limits,
                            &mut state,
                            accounting,
                            cancellation,
                        )
                        .await
                    }
                    .await;
                    theirs.retire(accounting)?;
                    result
                }
                .await;
                ours.retire(accounting)?;
                result?;
                fold_additions(
                    store,
                    resolutions,
                    request.maximum_conflicts,
                    limits,
                    &mut state,
                    accounting,
                    cancellation,
                )
                .await?;
                Ok::<_, FsError>(())
            }
            .await;
            if let Err(error) = result {
                state.retire(accounting)?;
                return Err(error);
            }
            Ok(FileTablePreparation {
                original: request.ours.file_table,
                maximum_changes: request.maximum_changes,
                state,
            })
        },
    )?;
    let mut future: acyclic_stream::BoxProviderFuture<'_, _> = future;
    let result = future.as_mut().await;
    drop(future);
    if let Some(mut frame) = frame {
        accounting.release_owned(&mut frame, bytes)?;
    }
    result
}

#[cfg(feature = "native-mount")]
async fn merge_file_tables_core<S: AsyncObjectStore>(
    store: &S,
    request: MergeFileTableRequest<'_>,
    resolutions: ResolutionView<'_>,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(MergeFileTableOutcome, KernelOwnedAllocation), FsError> {
    request.validate(accounting)?;
    let mut frame = None;
    let (future, bytes) = crate::heap_future::in_heap_admitted(
        (&mut *accounting, &mut frame),
        |(accounting, frame), bytes| {
            **frame = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
            Ok::<_, FsError>(())
        },
        |(accounting, _)| async move {
            let mut prepared = prepare_file_tables_core(
                store,
                request,
                resolutions,
                limits,
                accounting,
                cancellation,
            )
            .await?;
            if let Err(error) = prepared
                .resolve(resolutions, store, limits, accounting, cancellation)
                .await
            {
                prepared.state.retire(accounting)?;
                return Err(error);
            }
            prepared
                .finish(store, limits, accounting, cancellation)
                .await
        },
    )?;
    let mut future: acyclic_stream::BoxProviderFuture<'_, _> = future;
    let result = future.as_mut().await;
    drop(future);
    if let Some(mut frame) = frame {
        accounting.release_owned(&mut frame, bytes)?;
    }
    result
}

/// The accepted mutations and unresolved frontier from the one table walk.
/// No input roots or diff iterators escape into the continuation.
struct FileTablePreparation {
    original: ObjectId,
    maximum_changes: u32,
    state: MergeState,
}

impl FileTablePreparation {
    fn into_conflicts(
        self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<(Vec<MergeConflict>, bool)>, FsError> {
        let MergeState {
            records,
            conflicts,
            folds,
            file_folds,
            replacements,
            pending_files,
            pending_bindings,
            pending_rebindings,
            truncated,
            ..
        } = self.state;
        records.retire(accounting)?;
        folds.retire(accounting)?;
        file_folds.retire(accounting)?;
        replacements.retire(accounting)?;
        pending_files.retire(accounting)?;
        pending_bindings.retire(accounting)?;
        pending_rebindings.retire(accounting)?;
        let Owned {
            value,
            allocation,
            bytes,
        } = conflicts;
        Ok(Owned::new((value, truncated), allocation, bytes))
    }

    async fn finish<S: AsyncObjectStore>(
        mut self,
        store: &S,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(MergeFileTableOutcome, KernelOwnedAllocation), FsError> {
        if !self.state.conflicts.value.is_empty() || self.state.truncated {
            let Owned {
                value: (conflicts, truncated),
                allocation,
                ..
            } = self.into_conflicts(accounting)?;
            return Ok((
                MergeFileTableOutcome::Conflicted {
                    conflicts,
                    truncated,
                },
                allocation,
            ));
        }
        let result = async {
            adjust_link_counts(store, limits, &mut self.state, accounting, cancellation).await?;
            let mut mutations = Owned::allocate(0, accounting)?;
            let result = async {
                for (file, (before, after)) in &self.state.records.value {
                    if before != after
                        && let Some(mutation) = file_table_mutation(*file, *before, *after)
                    {
                        mutations.ensure_for_push(usize::MAX, accounting)?;
                        mutations.value.push(mutation);
                    }
                }
                if mutations.value.is_empty() {
                    Ok(self.original)
                } else {
                    apply_file_table_mutations_with_accounting(
                        store,
                        self.original,
                        &mutations.value,
                        self.maximum_changes,
                        limits,
                        accounting,
                        cancellation,
                    )
                    .await
                    .map(|receipt| receipt.value)
                }
            }
            .await;
            mutations.retire(accounting)?;
            result
        }
        .await;
        self.state.retire(accounting)?;
        Ok((
            MergeFileTableOutcome::Prepared(result?),
            KernelOwnedAllocation::Standalone,
        ))
    }
}

#[allow(
    clippy::type_complexity,
    reason = "This pairs borrowed diff entries directly without another carrier or allocation."
)]
fn next_changes<'a, K: Ord, V>(
    left: &'a [super::persistent_diff::ValueChange<K, V>],
    right: &'a [super::persistent_diff::ValueChange<K, V>],
    indices: &mut [usize; 2],
) -> Option<[Option<&'a super::persistent_diff::ValueChange<K, V>>; 2]> {
    let [left_index, right_index] = indices;
    let (a, b) = (left.get(*left_index), right.get(*right_index));
    let result = match (a, b) {
        (Some(a), Some(b)) => match a.key.cmp(&b.key) {
            std::cmp::Ordering::Less => [Some(a), None],
            std::cmp::Ordering::Greater => [None, Some(b)],
            std::cmp::Ordering::Equal => [Some(a), Some(b)],
        },
        (Some(a), None) => [Some(a), None],
        (None, Some(b)) => [None, Some(b)],
        (None, None) => return None,
    };
    let [left_change, right_change] = result;
    *left_index += usize::from(left_change.is_some());
    *right_index += usize::from(right_change.is_some());
    Some(result)
}
