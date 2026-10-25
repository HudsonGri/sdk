#[derive(Clone, Copy)]
struct PendingFile {
    conflict: usize,
    values: [Option<FileRecord>; 3],
    directory_scalars: bool,
}

#[derive(Clone, Copy)]
struct PendingBinding {
    conflict: usize,
    values: [Option<(FileId, super::FileKind)>; 3],
}

impl MergeState {
    fn defer_rebinding(
        &mut self,
        file: FileId,
        (directory_id, name, replacement): (FileId, Owned<LogicalName>, FileId),
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let copied = WorkCounters {
            items_examined: 1,
            bytes_copied: crate::foundation::usize_to_u64(std::mem::size_of::<FileFold>()),
            ..WorkCounters::UNCHARGED
        };
        let admitted = self
            .pending_rebindings
            .ensure_for_push(usize::MAX, accounting)
            .and_then(|()| accounting.admit(copied));
        if let Err(error) = admitted {
            name.retire(accounting)?;
            return Err(error);
        }
        let total = self.pending_rebindings.bytes.checked_add(name.bytes);
        let Some(total) = total else {
            name.retire(accounting)?;
            return Err(WorkError::Overflow.into());
        };
        let Owned {
            value: name,
            mut allocation,
            bytes,
        } = name;
        if let Err(error) = self.pending_rebindings.allocation.absorb(&mut allocation) {
            Owned::new(name, allocation, bytes).retire(accounting)?;
            return Err(error);
        }
        self.pending_rebindings.bytes = total;
        self.pending_rebindings.value.push(FileFold {
            directory_id,
            name,
            ours: file,
            theirs: replacement,
        });
        accounting.record_completed(copied)
    }

    fn defer_file(
        &mut self,
        file: FileId,
        values: [Option<FileRecord>; 3],
        directory_scalars: bool,
        maximum: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let conflict = self.conflicts.value.len();
        self.conflict(MergeConflict::File(file), maximum, accounting)?;
        if self.conflicts.value.len() != conflict {
            self.pending_files.push_accounted(
                PendingFile {
                    conflict,
                    values,
                    directory_scalars,
                },
                usize::MAX,
                accounting,
            )?;
        }
        Ok(())
    }

    fn defer_binding(
        &mut self,
        directory_id: FileId,
        name: &LogicalName,
        values: [Option<(FileId, super::FileKind)>; 3],
        maximum: u32,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let maximum = usize::try_from(maximum).unwrap_or(usize::MAX);
        if self.conflicts.value.len() == maximum {
            self.truncated = true;
            return Ok(());
        }
        self.conflicts.ensure_for_push(maximum, accounting)?;
        let (name, allocation) = super::persistent_io::clone_key::<TreeFormat>(
            name,
            limits.maximum_name_bytes,
            accounting,
            None,
            0,
        )?;
        let bytes = TreeFormat::key_owned_bytes(&name);
        let name = Owned::new(name, allocation, bytes);
        let retained = self.pending_bindings.push_accounted(
            PendingBinding {
                conflict: self.conflicts.value.len(),
                values,
            },
            usize::MAX,
            accounting,
        );
        if let Err(error) = retained {
            name.retire(accounting)?;
            return Err(error);
        }
        let Owned {
            value: name,
            mut allocation,
            bytes,
        } = name;
        self.conflicts.allocation.absorb(&mut allocation)?;
        self.conflicts.bytes = self
            .conflicts
            .bytes
            .checked_add(bytes)
            .ok_or(WorkError::Overflow)?;
        self.conflicts
            .value
            .push(MergeConflict::Binding { directory_id, name });
        Ok(())
    }
}

fn selected_file(
    pending: PendingFile,
    resolution: &MergeConflictResolution,
) -> Result<Option<FileRecord>, FsError> {
    let [base, ours, theirs] = pending.values;
    let selected = match resolution {
        MergeConflictResolution::Select(side) => select_optional(*side, base, ours, theirs),
        MergeConflictResolution::File(record) => *record,
        MergeConflictResolution::Binding(_) => return Err(FsError::InvalidDiff),
    };
    if pending.directory_scalars && !is_directory(selected) {
        return Err(FsError::InvalidDiff);
    }
    Ok(selected)
}

fn selected_binding(
    pending: PendingBinding,
    resolution: &MergeConflictResolution,
) -> Result<Option<(FileId, super::FileKind)>, FsError> {
    let [base, ours, theirs] = pending.values;
    match resolution {
        MergeConflictResolution::Select(side) => Ok(select_optional(*side, base, ours, theirs)),
        MergeConflictResolution::Binding(id) => pending
            .values
            .into_iter()
            .find(|value| value.map(|(file, _)| file) == *id)
            .ok_or(FsError::InvalidDiff),
        MergeConflictResolution::File(_) => Err(FsError::InvalidDiff),
    }
}

impl FileTablePreparation {
    fn resolution<'a>(
        &self,
        index: usize,
        resolutions: ResolutionView<'a>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&'a MergeConflictResolution>, FsError> {
        let key = self
            .state
            .conflicts
            .value
            .get(index)
            .ok_or(FsError::InvalidDiff)?;
        resolutions.get_by(accounting, |candidate| candidate.cmp(key))
    }

    /// Applies a complete resolution frontier to retained accepted records.
    /// Missing selections leave the preparation untouched. No root diff or
    /// regular-file content merge can be reached from this continuation.
    async fn resolve<S: AsyncObjectStore>(
        &mut self,
        resolutions: ResolutionView<'_>,
        store: &S,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        if self.state.truncated {
            return Ok(false);
        }
        for index in 0..self.state.conflicts.value.len() {
            if self.resolution(index, resolutions, accounting)?.is_none() {
                return Ok(false);
            }
        }
        for index in 0..self.state.pending_files.value.len() {
            let pending = *self
                .state
                .pending_files
                .value
                .get(index)
                .ok_or(FsError::InvalidDiff)?;
            let Some(resolution) = self.resolution(pending.conflict, resolutions, accounting)?
            else {
                return Err(FsError::InvalidDiff);
            };
            let MergeConflict::File(file) = *self
                .state
                .conflicts
                .value
                .get(pending.conflict)
                .ok_or(FsError::InvalidDiff)?
            else {
                return Err(FsError::InvalidDiff);
            };
            let selected = selected_file(pending, resolution)?;
            if selected.is_some_and(|record| record.file_id != file) {
                return Err(FsError::InvalidDiff);
            }
            let (before, current) = self.state.record(file, accounting)?;
            let after = if pending.directory_scalars {
                let current = current.ok_or(FsError::InvalidDiff)?;
                let selected = selected.ok_or(FsError::InvalidDiff)?;
                Some(FileRecord {
                    metadata: selected.metadata,
                    link_count: selected.link_count,
                    ..current
                })
            } else {
                selected
            };
            self.state.replace(file, (before, after), accounting)?;
        }
        self.resolve_bindings(resolutions, store, limits, accounting, cancellation)
            .await?;
        self.resolve_rebindings(store, limits, accounting, cancellation)
            .await?;
        self.state.conflicts.clear(accounting)?;
        self.state.pending_files.clear(accounting)?;
        self.state.pending_bindings.clear(accounting)?;
        self.state.pending_rebindings.clear(accounting)?;
        Ok(true)
    }

    async fn resolve_rebindings<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        while let Some(binding) = self.state.pending_rebindings.value.pop() {
            let bytes = TreeFormat::key_owned_bytes(&binding.name);
            let allocation = super::persistent_point::split_allocation(
                &mut self.state.pending_rebindings.allocation,
                bytes,
            )?;
            self.state.pending_rebindings.bytes = self
                .state
                .pending_rebindings
                .bytes
                .checked_sub(bytes)
                .ok_or(WorkError::Overflow)?;
            let name = Owned::new(binding.name, allocation, bytes);
            let selected = (|| {
                let (_, selected) = self.state.record(binding.ours, accounting)?;
                let (before, _) = self.state.record(binding.theirs, accounting)?;
                self.state
                    .replace(binding.theirs, (before, None), accounting)?;
                Ok::<_, FsError>(selected.map(|_| binding.ours))
            })();
            let target = match selected {
                Ok(target) => target,
                Err(error) => {
                    name.retire(accounting)?;
                    return Err(error);
                }
            };
            rebind_replacement(
                store,
                binding.directory_id,
                name,
                binding.theirs,
                target,
                limits,
                &mut self.state,
                accounting,
                cancellation,
            )
            .await?;
        }
        Ok(())
    }

    async fn resolve_bindings<S: AsyncObjectStore>(
        &mut self,
        resolutions: ResolutionView<'_>,
        store: &S,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let conflicts = &self.state.conflicts.value;
        for pending in &self.state.pending_bindings.value {
            accounting.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if conflicts.get(pending.conflict).is_none() {
                return Err(FsError::InvalidDiff);
            }
        }
        super::sort::sort_by_accounted(
            &mut self.state.pending_bindings.value,
            accounting,
            |a, b| conflicts.get(a.conflict).cmp(&conflicts.get(b.conflict)),
        )?;
        let mut start = 0;
        while let Some(first) = self.state.pending_bindings.value.get(start) {
            let MergeConflict::Binding { directory_id, .. } = *self
                .state
                .conflicts
                .value
                .get(first.conflict)
                .ok_or(FsError::InvalidDiff)?
            else {
                return Err(FsError::InvalidDiff);
            };
            let mut end = start + 1;
            while let Some(next) = self.state.pending_bindings.value.get(end) {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if !matches!(*self.state.conflicts.value.get(next.conflict).ok_or(FsError::InvalidDiff)?, MergeConflict::Binding { directory_id: other, .. } if other == directory_id)
                {
                    break;
                }
                end += 1;
            }
            self.resolve_directory(
                (directory_id, start..end),
                resolutions,
                store,
                limits,
                accounting,
                cancellation,
            )
            .await?;
            start = end;
        }
        Ok(())
    }

    async fn resolve_directory<S: AsyncObjectStore>(
        &mut self,
        (directory, range): (FileId, std::ops::Range<usize>),
        resolutions: ResolutionView<'_>,
        store: &S,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let mut mutations = Owned::allocate(range.len(), accounting)?;
        let result = async {
            for index in range {
                let pending = *self
                    .state
                    .pending_bindings
                    .value
                    .get(index)
                    .ok_or(FsError::InvalidDiff)?;
                let resolution = self
                    .resolution(pending.conflict, resolutions, accounting)?
                    .ok_or(FsError::InvalidDiff)?;
                let selected = selected_binding(pending, resolution)?;
                let [_, before, _] = pending.values;
                if selected == before {
                    continue;
                }
                let MergeConflict::Binding { name, .. } = self
                    .state
                    .conflicts
                    .value
                    .get(pending.conflict)
                    .ok_or(FsError::InvalidDiff)?
                else {
                    return Err(FsError::InvalidDiff);
                };
                let (name, mut allocation) = super::persistent_io::clone_key::<TreeFormat>(
                    name,
                    limits.maximum_name_bytes,
                    accounting,
                    None,
                    0,
                )?;
                let bytes = TreeFormat::key_owned_bytes(&name);
                mutations.allocation.absorb(&mut allocation)?;
                mutations.bytes = mutations
                    .bytes
                    .checked_add(bytes)
                    .ok_or(WorkError::Overflow)?;
                mutations.value.push(match (before, selected) {
                    (Some((expected_file_id, _)), Some((file_id, kind))) => TreeMutation::Replace {
                        entry: super::TreeEntry {
                            name,
                            file_id,
                            kind,
                        },
                        expected_file_id,
                    },
                    (None, Some((file_id, kind))) => TreeMutation::Insert(super::TreeEntry {
                        name,
                        file_id,
                        kind,
                    }),
                    (Some((expected_file_id, _)), None) => TreeMutation::Remove {
                        name,
                        expected_file_id: Some(expected_file_id),
                    },
                    (None, None) => return Err(FsError::InvalidDiff),
                });
            }
            let (before, after) = self.state.record(directory, accounting)?;
            let record = after.ok_or(FsError::InvalidDiff)?;
            let entries = directory_entries(after).ok_or(FsError::InvalidDiff)?;
            let entries = if mutations.value.is_empty() {
                entries
            } else {
                apply_tree_mutations_with_accounting(
                    store,
                    entries,
                    &mutations.value,
                    self.maximum_changes,
                    limits,
                    accounting,
                    cancellation,
                )
                .await?
                .value
            };
            self.state.replace(
                directory,
                (
                    before,
                    Some(FileRecord {
                        payload: FilePayload::Directory { entries },
                        ..record
                    }),
                ),
                accounting,
            )
        }
        .await;
        mutations.retire(accounting)?;
        result
    }
}
