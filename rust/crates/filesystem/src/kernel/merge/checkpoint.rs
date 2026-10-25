/// Scalar checkpoint inputs retained independently of decoded merge roots.
#[derive(Clone, Copy)]
struct MergeCheckpointContext {
    base_generation: ObjectId,
    ours_generation: Option<ObjectId>,
    ours_table: ObjectId,
    theirs_generation: ObjectId,
    retain_theirs_parent: bool,
    volume_id: crate::foundation::VolumeId,
    root_file_id: FileId,
    required_features: u64,
}

pub(crate) struct GenerationMergePreparation {
    tables: FileTablePreparation,
    checkpoint: MergeCheckpointContext,
}

impl GenerationMergePreparation {
    pub(crate) fn retained_bytes(&self) -> Result<u64, FsError> {
        let state = &self.tables.state;
        [
            state.records.bytes,
            state.conflicts.bytes,
            state.folds.bytes,
            state.file_folds.bytes,
            state.replacements.bytes,
            state.pending_files.bytes,
            state.pending_bindings.bytes,
            state.pending_rebindings.bytes,
        ]
        .into_iter()
        .try_fold(0_u64, |sum, bytes| {
            sum.checked_add(bytes).ok_or(WorkError::Overflow)
        })
        .map_err(Into::into)
    }

    pub(crate) const fn original_table(&self) -> ObjectId {
        self.tables.original
    }

    pub(crate) fn conflicts(&self) -> (&[MergeConflict], bool) {
        (
            &self.tables.state.conflicts.value,
            self.tables.state.truncated,
        )
    }

    pub(crate) fn into_conflicts(
        self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<(Vec<MergeConflict>, bool)>, FsError> {
        self.tables.into_conflicts(accounting)
    }

    pub(crate) fn records(&self) -> impl Iterator<Item = FileRecord> + '_ {
        self.tables
            .state
            .records
            .value
            .iter()
            .filter_map(|(_, (_, after))| *after)
    }

    pub(crate) fn generations(&self) -> [ObjectId; 3] {
        [
            self.checkpoint.base_generation,
            self.checkpoint
                .ours_generation
                .unwrap_or(self.checkpoint.base_generation),
            self.checkpoint.theirs_generation,
        ]
    }

    pub(crate) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        self.tables.state.retire(accounting)
    }

    pub(crate) async fn prepare_map<S: AsyncObjectStore>(
        store: &S,
        request: MergeGenerationBorrowedRequest<'_>,
        resolutions: &BTreeMap<MergeConflict, MergeConflictResolution>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let indexed = index_resolutions(resolutions.iter(), accounting)?;
        Self::prepare_indexed(store, request, indexed, limits, accounting, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn prepare_slice<S: AsyncObjectStore>(
        store: &S,
        request: MergeGenerationBorrowedRequest<'_>,
        resolutions: &[(MergeConflict, MergeConflictResolution)],
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let indexed = index_resolutions(
            resolutions.iter().map(|(key, value)| (key, value)),
            accounting,
        )?;
        Self::prepare_indexed(store, request, indexed, limits, accounting, cancellation).await
    }

    async fn prepare_indexed<S: AsyncObjectStore>(
        store: &S,
        request: MergeGenerationBorrowedRequest<'_>,
        indexed: Owned<Vec<ResolutionEntry<'_>>>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let result = Self::prepare(
            store,
            request,
            ResolutionView::Entries(&indexed.value),
            limits,
            accounting,
            cancellation,
        )
        .await;
        indexed.retire(accounting)?;
        result
    }

    pub(crate) async fn resolve_entries<'a, S: AsyncObjectStore>(
        &mut self,
        store: &S,
        resolutions: impl ExactSizeIterator<Item = (&'a MergeConflict, &'a MergeConflictResolution)>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        let indexed = index_resolutions(resolutions, accounting)?;
        let result = self
            .tables
            .resolve(
                ResolutionView::Entries(&indexed.value),
                store,
                limits,
                accounting,
                cancellation,
            )
            .await;
        indexed.retire(accounting)?;
        result
    }

    pub(crate) async fn finish_resolved<S: AsyncObjectStore>(
        self,
        store: &S,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
        self.finish(
            store,
            ResolutionView::Entries(&[]),
            limits,
            accounting,
            cancellation,
        )
        .await
    }

    async fn prepare<S: AsyncObjectStore>(
        store: &S,
        request: MergeGenerationBorrowedRequest<'_>,
        resolutions: ResolutionView<'_>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Self, FsError> {
        let tables = prepare_file_tables_core(
            store,
            request.tables(),
            resolutions,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        Ok(Self {
            tables,
            checkpoint: MergeCheckpointContext {
                base_generation: request.base_generation,
                ours_generation: request.ours_generation,
                ours_table: request.ours.file_table,
                theirs_generation: request.theirs_generation,
                retain_theirs_parent: request.retain_theirs_parent,
                volume_id: request.ours.volume_id,
                root_file_id: request.ours.root_file_id,
                required_features: request.ours.continuation_features(),
            },
        })
    }

    async fn finish<S: AsyncObjectStore>(
        self,
        store: &S,
        resolutions: ResolutionView<'_>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
        let Self {
            mut tables,
            checkpoint,
        } = self;
        if let Err(error) = tables
            .resolve(resolutions, store, limits, accounting, cancellation)
            .await
        {
            tables.state.retire(accounting)?;
            return Err(error);
        }
        let (outcome, allocation) = tables
            .finish(store, limits, accounting, cancellation)
            .await?;
        match outcome {
            MergeFileTableOutcome::Prepared(table) => {
                finish_merged_generation(store, checkpoint, table, limits, accounting, cancellation)
                    .await
            }
            MergeFileTableOutcome::Conflicted {
                conflicts,
                truncated,
            } => Ok((
                MergeGenerationOutcome::Conflicted {
                    conflicts,
                    truncated,
                },
                allocation,
            )),
        }
    }
}

async fn merge_generation_core<S: AsyncObjectStore>(
    store: &S,
    request: MergeGenerationBorrowedRequest<'_>,
    resolutions: ResolutionView<'_>,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    request.tables().validate(accounting)?;
    let mut frame = None;
    let (future, bytes) = crate::heap_future::in_heap_admitted(
        (&mut *accounting, &mut frame),
        |(accounting, frame), bytes| {
            **frame = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
            Ok::<_, FsError>(())
        },
        |(accounting, _)| async move {
            GenerationMergePreparation::prepare(
                store,
                request,
                resolutions,
                limits,
                accounting,
                cancellation,
            )
            .await?
            .finish(store, resolutions, limits, accounting, cancellation)
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

async fn finish_merged_generation<S: AsyncObjectStore>(
    store: &S,
    context: MergeCheckpointContext,
    merged_table: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    let (parent, parent_id) = match context.ours_generation {
        Some(object) if object.kind == ObjectKind::GenerationRoot => {
            (object, GenerationId::new(object.digest))
        }
        Some(_) => return Err(FsError::InvalidDiff),
        None => {
            let parent = build_checkpoint_with_accounting(
                store,
                CheckpointRequest {
                    base: context.base_generation,
                    file_table: context.ours_table,
                    merge_parent: None,
                },
                limits,
                accounting,
                cancellation,
            )
            .await?;
            (parent, GenerationId::new(parent.digest))
        }
    };
    let merged = build_checkpoint_with_accounting(
        store,
        CheckpointRequest {
            base: parent,
            file_table: merged_table,
            merge_parent: context
                .retain_theirs_parent
                .then_some(context.theirs_generation),
        },
        limits,
        accounting,
        cancellation,
    )
    .await?;
    let parent_count = if context.retain_theirs_parent { 2 } else { 1 };
    let mut parents = Owned::allocate(parent_count, accounting)?;
    let delta = WorkCounters {
        bytes_copied: (parent_count * std::mem::size_of::<GenerationId>()) as u64,
        ..WorkCounters::UNCHARGED
    };
    let copied = accounting.admit(delta).and_then(|()| {
        parents.value.push(parent_id);
        if context.retain_theirs_parent {
            parents
                .value
                .push(GenerationId::new(context.theirs_generation.digest));
        }
        accounting.record_completed(delta)
    });
    if let Err(error) = copied {
        parents.retire(accounting)?;
        return Err(error);
    }
    let Owned {
        value: parents,
        allocation,
        ..
    } = parents;
    Ok((
        MergeGenerationOutcome::Prepared {
            generation_root: merged,
            generation_id: GenerationId::new(merged.digest),
            root: GenerationRoot {
                volume_id: context.volume_id,
                root_file_id: context.root_file_id,
                file_table: merged_table,
                parents,
                required_features: context.required_features,
            },
        },
        allocation,
    ))
}
