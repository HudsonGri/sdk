impl NativeWatch {
    #[cfg(windows)]
    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    fn watch_directory_wide_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self,
        directory: &NamespacePath,
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::demand::DemandError;
        let mut retained_bytes = 0_u64;
        let mut path_bytes = 0_u64;
        let mut backing_claim = 0_u64;
        let result = (|| {
            let path = work.with_original_leaf(|work| {
                NamespacePath::validate_component_lengths(
                    directory
                        .components()
                        .iter()
                        .map(|name| name.as_bytes().len()),
                    self.windows_delivery.context.limits,
                )?;
                crate::native_capture::namespace_host_units_with_work(
                    directory,
                    work,
                    u16::from(b'\\'),
                    crate::native_capture::namespace_component_wide,
                )
                .map(|(path, _allocation)| path)
            })?;
            path_bytes = (path.capacity() as u64)
                .checked_mul(2)
                .ok_or(WorkError::Overflow)?;
            let result = (|| {
                let mut held = self
                    .held_root
                    .root
                    .as_ref()
                    .ok_or(DemandError::from(NativeWatchError::SourceUnavailable))?
                    .open_dir_held(&[], work)?;
                let mut component_start = 0;
                for (index, &unit) in path.iter().enumerate() {
                    work.started_io(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::default()
                    })?;
                    if unit != u16::from(b'\\') && index + 1 != path.len() {
                        continue;
                    }
                    let length = if unit == u16::from(b'\\') {
                        index
                    } else {
                        index + 1
                    };
                    let prefix = path.get(..length).ok_or(DemandError::InvalidRequest)?;
                    let (next, identity) = match crate::native_host::open_windows_directory(
                        &held,
                        path.get(component_start..length)
                            .ok_or(DemandError::InvalidRequest)?,
                        false,
                        work,
                    ) {
                        Ok(observed) => observed,
                        Err(DemandError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotFound =>
                        {
                            return Ok(());
                        }
                        Err(DemandError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotADirectory =>
                        {
                            return Err(DemandError::from(
                                NativeWatchError::ObservedPathNotDirectory,
                            ));
                        }
                        Err(DemandError::InvalidRequest) => {
                            return Err(DemandError::from(NativeWatchError::SymbolicLinkDirectory));
                        }
                        Err(error) => return Err(error),
                    };
                    held = next;
                    component_start = index + 1;
                    let mut found = None;
                    for (at, observed) in self.windows_directories.iter().enumerate() {
                        work.started_io(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::default()
                        })?;
                        if observed.path.0.as_slice() == prefix {
                            found = Some(at);
                            break;
                        }
                    }
                    if let Some(at) = found {
                        let observed = self
                            .windows_directories
                            .get(at)
                            .ok_or(DemandError::InvalidRequest)?;
                        if observed.identity == identity && observed.watcher.is_exact() {
                            continue;
                        }
                        let copied = WorkCounters {
                            bytes_copied: ((self.windows_directories.len() - at - 1) as u64)
                                .checked_mul(size_of::<WindowsDirectoryWatch>() as u64)
                                .ok_or(WorkError::Overflow)?,
                            ..WorkCounters::default()
                        };
                        work.admit(copied)?;
                        self.windows_directories
                            .get_mut(at)
                            .ok_or(DemandError::InvalidRequest)?
                            .watcher
                            .stop_with_work(work)?;
                        if !self
                            .windows_directories
                            .get(at)
                            .ok_or(DemandError::InvalidRequest)?
                            .watcher
                            .is_stopped()
                        {
                            return Err(DemandError::WorkerUnavailable);
                        }
                        work.record(copied)?;
                        let prior = self.windows_directories.remove(at);
                        drop(prior);
                    }
                    if self.windows_directories.len() == self.windows_directories.capacity() {
                        let count = self
                            .windows_directories
                            .len()
                            .checked_add(1)
                            .ok_or(WorkError::Overflow)?;
                        let bytes = (count as u64)
                            .checked_mul(size_of::<WindowsDirectoryWatch>() as u64)
                            .ok_or(WorkError::Overflow)?;
                        let old_bytes = (self.windows_directories.capacity() as u64)
                            .checked_mul(size_of::<WindowsDirectoryWatch>() as u64)
                            .ok_or(WorkError::Overflow)?;
                        let copied = WorkCounters {
                            bytes_copied: old_bytes,
                            ..WorkCounters::default()
                        };
                        work.admit(copied)?;
                        let allocation = watch_claim_allocation(bytes, work)?;
                        let mut grown = Vec::with_capacity(count);
                        grown.append(&mut self.windows_directories);
                        self.windows_directories = grown;
                        drop(self.windows_backing_allocation.take());
                        if allocation.is_none() {
                            work.release(backing_claim)?;
                            retained_bytes = retained_bytes
                                .checked_sub(backing_claim)
                                .and_then(|prior| prior.checked_add(bytes))
                                .ok_or(WorkError::Overflow)?;
                            backing_claim = bytes;
                        }
                        self.windows_backing_allocation = allocation;
                        work.record(copied)?;
                    }
                    let key_bytes = (prefix.len() as u64)
                        .checked_mul(2)
                        .ok_or(WorkError::Overflow)?;
                    let copied = WorkCounters {
                        bytes_copied: key_bytes,
                        ..WorkCounters::default()
                    };
                    work.admit(copied)?;
                    let allocation = watch_claim_allocation(key_bytes, work)?;
                    if allocation.is_none() {
                        retained_bytes = retained_bytes
                            .checked_add(key_bytes)
                            .ok_or(WorkError::Overflow)?;
                    }
                    let mut key = (prefix.to_vec(), allocation);
                    work.record(copied)?;
                    let key_arc_bytes = std::mem::size_of_val(&key) as u64;
                    let mut allocation = watch_claim_allocation(key_arc_bytes, work)?;
                    if let (Some(key_allocation), Some(arc_allocation)) =
                        (&mut key.1, &mut allocation)
                    {
                        key_allocation
                            .absorb(arc_allocation)
                            .map_err(NativeWatchError::from)?;
                    } else {
                        retained_bytes = retained_bytes
                            .checked_add(key_arc_bytes)
                            .ok_or(WorkError::Overflow)?;
                    }
                    let key = Arc::new(key);
                    let sink_bytes = size_of::<WindowsDirectorySink>() as u64;
                    let allocation = watch_claim_allocation(sink_bytes, work)?;
                    if allocation.is_none() {
                        retained_bytes = retained_bytes
                            .checked_add(sink_bytes)
                            .ok_or(WorkError::Overflow)?;
                    }
                    let sink = Arc::new(WindowsDirectorySink {
                        prefix: Arc::clone(&key),
                        delivery: Arc::clone(&self.windows_delivery),
                        _allocation: allocation,
                    });
                    let watcher =
                        match crate::source_watch::NativeSourceWatch::start_directory_with_work(
                            &held,
                            sink,
                            self.complete,
                            work,
                        ) {
                            Ok(watcher) => watcher,
                            Err(crate::demand::DemandError::SourceWatchStartup(owner)) => {
                                self.pending_source_watch = Some(Arc::clone(&owner));
                                return Err(crate::demand::DemandError::SourceWatchStartup(owner));
                            }
                            Err(error) => return Err(error),
                        };
                    self.windows_directories.push(WindowsDirectoryWatch {
                        path: key,
                        identity,
                        watcher,
                    });
                }
                Ok(())
            })();
            drop(path);
            work.release(path_bytes)?;
            path_bytes = 0;
            result
        })();
        work.release(path_bytes)?;
        work.release(retained_bytes)?;
        result
    }

    #[cfg(windows)]
    fn clear_windows_directories_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        while let Some(observed) = self.windows_directories.last_mut() {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            observed.watcher.stop_with_work(work)?;
            if !observed.watcher.is_stopped() {
                return Err(crate::demand::DemandError::WorkerUnavailable);
            }
            let removed = self
                .windows_directories
                .pop()
                .ok_or(crate::demand::DemandError::InvalidRequest)?;
            drop(removed);
        }
        self.windows_directories = Vec::new();
        drop(self.windows_backing_allocation.take());
        Ok(())
    }

    /// Adds every directory beneath the root, except the root names in `excluded`, to a non-recursive native watcher: what a baseline that captures the whole tree needs before it starts, so a change to anything it captured is reported.
    pub fn watch_tree(&mut self, excluded: &[&str]) -> Result<(), NativeWatchError> {
        if self.recursive {
            return Ok(());
        }
        let mut pending = vec![self.root.clone()];
        while let Some(directory) = pending.pop() {
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(NativeWatchError::Io(error)),
            };
            for entry in entries {
                let entry = entry.map_err(NativeWatchError::Io)?;
                let is_directory = entry
                    .file_type()
                    .is_ok_and(|file_type| file_type.is_dir() && !file_type.is_symlink());
                if !is_directory
                    || (directory == self.root
                        && excluded.iter().any(|name| entry.file_name() == *name))
                {
                    continue;
                }
                let path = entry.path();
                let relative = path
                    .strip_prefix(&self.root)
                    .map_err(|_| NativeWatchError::UnrepresentablePath)?;
                let cancellation = CancellationToken::new();
                let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
                let (namespace, bytes, allocation) =
                    namespace_hint_with_work(relative, self.profile, self.limits, &mut work)
                        .map_err(demand_watch_error)?;
                let observed = self.watch_directory(&namespace);
                drop(namespace);
                let guarded = allocation.is_some();
                drop(allocation);
                if !guarded {
                    work.release(bytes).map_err(demand_watch_error)?;
                }
                match observed {
                    Ok(()) => {}
                    Err(_) if !path.is_dir() => continue,
                    Err(error) => return Err(error),
                }
                pending.push(path);
            }
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    fn forget_removed_directories_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self,
        changes: &[WatchChange],
        work: &mut W,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), NativeWatchError> {
        #[cfg(target_os = "macos")]
        let _ = work;
        #[cfg(any(target_os = "linux", windows))]
        if matches!(self.watcher, NativeWatchDriver::Source(_)) {
            for change in changes {
                let removed = match change {
                    WatchChange::Removed(path) => [Some(path), None],
                    WatchChange::Renamed { from, to } => [Some(from), Some(to)],
                    _ => [None, None],
                };
                for path in removed.into_iter().flatten() {
                    let (relative, bytes, allocation) = if let Some(mut original) = original {
                        let (relative, allocation) =
                            crate::native_capture::namespace_to_host_path_with_work(
                                path,
                                &mut original,
                            )?;
                        (relative, 0, Some(allocation))
                    } else {
                        let (relative, bytes) =
                            crate::native_capture::namespace_to_host_path_with_work(path, work)
                                .map_err(demand_watch_error)?;
                        (relative, bytes, None)
                    };
                    #[cfg(target_os = "linux")]
                    let result = {
                        let NativeWatchDriver::Source(watcher) = &self.watcher else {
                            unreachable!("the authoritative source driver remains owned");
                        };
                        // The source owns registrations; never copy an SDK root cache or adopt previously queued path data.
                        watcher
                            .unadmit_with_work(&relative, work)
                            .map_err(demand_watch_error)
                    };
                    #[cfg(windows)]
                    let result = (|| {
                        use std::os::windows::ffi::OsStrExt;
                        let mut at = 0;
                        while at < self.windows_directories.len() {
                            work.started_io(WorkCounters {
                                items_examined: 1,
                                ..WorkCounters::UNCHARGED
                            })
                            .map_err(demand_watch_error)?;
                            let observed = self
                                .windows_directories
                                .get(at)
                                .ok_or(NativeWatchError::WorkerUnavailable)?;
                            let mut registered = observed.path.0.iter();
                            let matches = relative
                                .as_os_str()
                                .encode_wide()
                                .all(|unit| registered.next() == Some(&unit))
                                && registered
                                    .next()
                                    .is_none_or(|unit| matches!(*unit, 47 | 92));
                            if !matches {
                                at += 1;
                                continue;
                            }
                            self.windows_directories
                                .get_mut(at)
                                .ok_or(NativeWatchError::WorkerUnavailable)?
                                .watcher
                                .stop_with_work(work)
                                .map_err(demand_watch_error)?;
                            if !self
                                .windows_directories
                                .get(at)
                                .ok_or(NativeWatchError::WorkerUnavailable)?
                                .watcher
                                .is_stopped()
                            {
                                return Err(NativeWatchError::WorkerUnavailable);
                            }
                            work.started_io(WorkCounters {
                                bytes_copied: ((self.windows_directories.len() - at - 1) as u64)
                                    .checked_mul(size_of::<WindowsDirectoryWatch>() as u64)
                                    .ok_or(WorkError::Overflow)?,
                                ..WorkCounters::UNCHARGED
                            })
                            .map_err(demand_watch_error)?;
                            drop(self.windows_directories.remove(at));
                        }
                        Ok(())
                    })();
                    drop(relative);
                    drop(allocation);
                    let released = work.release(bytes).map_err(demand_watch_error);
                    result.and(released)?;
                }
            }
            return Ok(());
        }
        if self.recursive {
            return Ok(());
        }
        if original.is_some() {
            return Err(NativeWatchError::BoundedObservationUnavailable);
        }
        // Only ordinary adapters retain their legacy opaque cache operations.
        let mut removed = Vec::new();
        for change in changes {
            match change {
                WatchChange::Removed(path) => removed.push(path),
                WatchChange::Renamed { from, to } => {
                    removed.push(from);
                    removed.push(to);
                }
                WatchChange::Created(_)
                | WatchChange::Modified(_)
                | WatchChange::Arrived(_)
                | WatchChange::MetadataChanged(_) => {}
            }
        }
        for path in removed {
            let Ok(relative) = crate::native_capture::namespace_to_host_path(path) else {
                continue;
            };
            let removed = self.root.join(relative);
            let stale = self
                .watched_directories
                .iter()
                .filter(|watched| **watched != self.root && watched.starts_with(&removed))
                .cloned()
                .collect::<Vec<_>>();
            for watched in stale {
                #[cfg(not(windows))]
                if let NativeWatchDriver::Adapter(watcher) = &mut self.watcher {
                    let _ = watcher.unwatch(&watched);
                }
                self.watched_directories
                    .retain(|candidate| candidate != &watched);
            }
        }
        Ok(())
    }

    /// Admits a demand-backed lazy baseline without enumerating the root.
    pub fn accept_lazy_baseline(&mut self) -> Result<WatchBatch, NativeWatchError> {
        self.accept_lazy_baseline_measured(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }

    /// Admits the same lazy baseline with cumulative queue/cleanup/root work.
    pub fn accept_lazy_baseline_measured(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<WatchBatch>, NativeWatchError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = self.accept_lazy_baseline_with_work(&mut work);
        result
            .map(|value| OperationReceipt {
                value,
                work: work.work,
            })
            .map_err(|error| OperationFailure::new(error, work.work))
    }

    /// Admits the lazy baseline on the caller's original native work.
    pub fn accept_lazy_baseline_with_native_work(
        &mut self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<WatchBatch, NativeWatchError> {
        let mut source = crate::NativeMaterializationWork::clone(work);
        self.accept_lazy_baseline_with_work(&mut source)
    }

    fn accept_lazy_baseline_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<WatchBatch, NativeWatchError> {
        self.begin_rescan_with_work(work)?;
        self.finish_rescan_with_work(work)
    }

    /// Returns the exact root identity admitted when the watcher opened.
    #[must_use]
    pub const fn root_identity(&self) -> NativeRootIdentity {
        self.root_identity
    }

    /// Verifies that a separately held capture root is the directory admitted by this watcher.
    pub fn verify_root_identity(
        &self,
        observed: NativeRootIdentity,
        native: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeWatchError> {
        let mut work = crate::NativeMaterializationWork::clone(native);
        crate::native_host::NativeHostWork::check(&work).map_err(demand_watch_error)?;
        if observed == self.root_identity
            && self
                .root_unchanged_with_work(&mut work)
                .map_err(demand_watch_error)?
        {
            return Ok(());
        }
        self.shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(WatchInvalidationReason::RootChanged);
        Err(NativeWatchError::RootChanged)
    }

    /// Starts a baseline scan and atomically discards only pre-scan hints.
    pub fn begin_rescan(&mut self) -> Result<u64, NativeWatchError> {
        let cancellation = CancellationToken::new();
        self.begin_rescan_with_work(&mut crate::demand::DemandWork::new(
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
    }

    fn begin_rescan_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<u64, NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        if self.rescan_in_progress {
            return Err(NativeWatchError::RescanAlreadyInProgress);
        }
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        let queued = self.queued.load(Ordering::Acquire);
        for _ in 0..queued {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })
            .map_err(demand_watch_error)?;
            let removed = self
                .receiver
                .get_mut()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .try_recv()
                .map_err(|_| NativeWatchError::StatePoisoned)?;
            self.queued.fetch_sub(1, Ordering::AcqRel);
            // Queued hints retain their service custody; this observer only discards the actual payload, even on a different allocation ledger.
            drop(removed);
            work.with_original_leaf(|actual| actual.reconcile_native_controls())
                .map_err(NativeWatchError::Work)?;
        }
        shared.invalidation = None;
        #[cfg(target_os = "linux")]
        {
            shared.pending_rename = None;
        }
        self.epoch = self
            .epoch
            .checked_add(1)
            .ok_or(NativeWatchError::SequenceExhausted)?;
        self.next_sequence = 0;
        drop(shared);
        self.rescan_in_progress = true;
        #[cfg(windows)]
        self.clear_windows_directories_with_work(work)
            .map_err(demand_watch_error)?;
        #[cfg(target_os = "linux")]
        if !self.recursive {
            self.clear_linux_directories_with_work(work)
                .map_err(demand_watch_error)?;
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        if !self.recursive {
            if self
                .watched_directories
                .iter()
                .any(|path| *path != self.root)
            {
                work.started_io(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::default()
                })
                .map_err(demand_watch_error)?;
                work.check().map_err(demand_watch_error)?;
                return Err(NativeWatchError::BoundedObservationUnavailable);
            }
        }
        Ok(self.epoch)
    }

    /// Admits a baseline only when the Windows USN journal proves continuity.
    #[cfg(target_os = "windows")]
    pub fn accept_windows_usn_baseline(
        &mut self,
        checkpoint: crate::WindowsUsnCheckpoint,
    ) -> Result<Result<crate::WindowsUsnContinuity, crate::WindowsUsnError>, NativeWatchError> {
        if self.rescan_in_progress {
            return Err(NativeWatchError::RescanAlreadyInProgress);
        }
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        if shared.invalidation != Some(WatchInvalidationReason::InitialSnapshotRequired)
            || self.epoch != 0
        {
            return Err(NativeWatchError::RescanAlreadyInProgress);
        }
        let verified = match crate::validate_windows_usn_checkpoint(&self.root, checkpoint) {
            Ok(crate::WindowsUsnContinuity::Unchanged) => crate::WindowsUsnContinuity::Unchanged,
            Ok(_) => {
                return Ok(Ok(crate::WindowsUsnContinuity::BaselineRequired(
                    crate::WindowsUsnDiscontinuity::VolumeAdvanced,
                )));
            }
            Err(error) => return Ok(Err(error)),
        };
        // The journal verifies prior hints; this lock fences later callbacks.
        self.epoch = self
            .epoch
            .checked_add(1)
            .ok_or(NativeWatchError::SequenceExhausted)?;
        self.next_sequence = 0;
        shared.invalidation = None;
        Ok(Ok(verified))
    }

    /// Seals the caller's baseline scan.
    pub fn finish_rescan(&mut self) -> Result<WatchBatch, NativeWatchError> {
        let cancellation = CancellationToken::new();
        self.finish_rescan_with_work(&mut crate::demand::DemandWork::new(
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
    }

    fn finish_rescan_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self,
        work: &mut W,
    ) -> Result<WatchBatch, NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        if !self.rescan_in_progress {
            return Err(NativeWatchError::NoRescanInProgress);
        }
        let unchanged = self
            .root_unchanged_with_work(work)
            .map_err(demand_watch_error)?;
        work.check().map_err(demand_watch_error)?;
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        if !unchanged {
            shared.invalidation = Some(WatchInvalidationReason::RootChanged);
        }
        self.rescan_in_progress = false;
        Ok(shared.seal_invalidation().map_or(
            WatchBatch::Changes {
                epoch: self.epoch,
                first_sequence: self.next_sequence,
                next_sequence: self.next_sequence,
                changes: Vec::new(),
                original: WatchBatchOriginal::default(),
            },
            |reason| WatchBatch::RescanRequired {
                epoch: self.epoch,
                reason,
            },
        ))
    }

    /// Aborts an in-progress baseline and returns the watcher to an explicitly invalidated, retryable state.
    pub fn abort_rescan(
        &mut self,
        reason: WatchInvalidationReason,
    ) -> Result<(), NativeWatchError> {
        if !self.rescan_in_progress {
            return Err(NativeWatchError::NoRescanInProgress);
        }
        self.rescan_in_progress = false;
        self.shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(reason);
        Ok(())
    }

    /// Stops reporting hints for events wholly inside `subtrees`.
    pub fn exclude_subtrees(&mut self, subtrees: &[NamespacePath]) -> Result<(), NativeWatchError> {
        let cancellation = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        self.exclude_subtrees_with_work(subtrees, &mut work, None)
    }

    /// Copies exclusions on the caller's original native allocation ledger.
    pub fn exclude_subtrees_with_native_work(
        &mut self,
        subtrees: &[NamespacePath],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeWatchError> {
        let mut source = crate::NativeMaterializationWork::clone(work);
        self.exclude_subtrees_with_work(subtrees, &mut source, Some(work))
    }
}
