impl NativeWatch {
    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    fn exclude_subtrees_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self,
        subtrees: &[NamespacePath],
        work: &mut W,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        // Keep the physical arrays before their ownership guards on every success and denial path; input and old arrays remain borrowed.
        let mut replacement = (Vec::<PathBuf>::new(), None);
        let shared_bytes = subtrees
            .len()
            .checked_mul(std::mem::size_of::<PathBuf>())
            .ok_or(WorkError::Overflow)?;
        replacement.1 = if shared_bytes == 0 {
            None
        } else if let Some(original) = original {
            Some(original.claim(
                shared_bytes as u64,
                u64::from((shared_bytes as u64) != 0),
                crate::WorkCounters::UNCHARGED,
            )?)
        } else {
            work.claim_allocation(
                shared_bytes as u64,
                u64::from((shared_bytes as u64) != 0),
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(demand_watch_error)?;
            None
        };
        replacement
            .0
            .try_reserve_exact(subtrees.len())
            .map_err(|error| {
                NativeWatchError::NativeWork(
                    crate::NativeTreeMaterializationError::AllocationFailed(error),
                )
            })?;
        let actual = replacement
            .0
            .capacity()
            .checked_mul(std::mem::size_of::<PathBuf>())
            .ok_or(WorkError::Overflow)?;
        Self::reconcile_exclusion_capacity(
            &mut replacement.1,
            shared_bytes as u64,
            actual as u64,
            work,
        )?;
        for subtree in subtrees {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })
            .map_err(demand_watch_error)?;
            if subtree.is_root() {
                return Err(NativeWatchError::UnrepresentablePath);
            }
            let relative = if let Some(mut native) = original {
                crate::native_capture::namespace_to_host_path_with_work(subtree, &mut native)
                    .map(|(path, allocation)| (path, Some(allocation), 0))?
            } else {
                crate::native_capture::namespace_to_host_path_with_work(subtree, work)
                    .map(|(path, bytes)| (path, None, bytes))
                    .map_err(demand_watch_error)?
            };
            let mut host = if let Some(original) = original {
                original
                    .path(&self.root, Some(&relative.0))
                    .map(|(path, allocation)| (path, Some(allocation)))?
            } else {
                let copied = self
                    .root
                    .as_os_str()
                    .len()
                    .checked_add(relative.0.as_os_str().len())
                    .ok_or(WorkError::Overflow)?;
                let capacity = copied.checked_add(1).ok_or(WorkError::Overflow)?;
                work.claim_allocation(
                    capacity as u64,
                    u64::from((capacity as u64) != 0),
                    WorkCounters {
                        bytes_copied: copied as u64,
                        ..WorkCounters::default()
                    },
                )
                .map_err(demand_watch_error)?;
                let mut path = PathBuf::new();
                path.try_reserve_exact(capacity).map_err(|error| {
                    NativeWatchError::NativeWork(
                        crate::NativeTreeMaterializationError::AllocationFailed(error),
                    )
                })?;
                Self::reconcile_exclusion_capacity(
                    &mut None,
                    capacity as u64,
                    path.capacity() as u64,
                    work,
                )?;
                path.push(&self.root);
                path.push(&relative.0);
                (path, None)
            };
            let relative_bytes = relative.2;
            drop(relative);
            if relative_bytes != 0 {
                work.release(relative_bytes).map_err(demand_watch_error)?;
            }
            if let Some(allocation) = &mut host.1 {
                allocation.resize(host.0.capacity() as u64)?;
            }
            if let Some(owned) = &mut replacement.1 {
                if let Some(incoming) = &mut host.1 {
                    owned.absorb(incoming)?;
                }
            } else {
                replacement.1 = host.1.take();
            }
            replacement.0.push(host.0);
        }
        work.check().map_err(demand_watch_error)?;
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        let old = (
            std::mem::replace(&mut shared.excluded, replacement.0),
            std::mem::replace(&mut shared.excluded_allocation, replacement.1),
        );
        drop(shared);
        drop(old);
        Ok(())
    }

    fn reconcile_exclusion_capacity<W: crate::demand::SourceDemandReadWork>(
        allocation: &mut Option<crate::NativeAllocation>,
        admitted: u64,
        actual: u64,
        work: &mut W,
    ) -> Result<(), NativeWatchError> {
        if let Some(allocation) = allocation {
            allocation.resize(actual)?;
        } else if actual > admitted {
            // This is new allocator-selected capacity, not adopted old data.
            work.claim_allocation(actual - admitted, 0, crate::WorkCounters::UNCHARGED)
                .map_err(demand_watch_error)?;
        } else if admitted > actual {
            work.release(admitted - actual)
                .map_err(demand_watch_error)?;
        }
        Ok(())
    }

    /// Fences the actual native producers on the caller's original work.
    pub fn fence_with_native_work(
        &mut self,
        timeout: Duration,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeWatchError> {
        let mut source = crate::NativeMaterializationWork::clone(work);
        self.fence_with_work(timeout, &mut source)
    }

    fn fence_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self,
        timeout: Duration,
        work: &mut W,
    ) -> Result<(), NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        let pending = match (&self.pending_source_watch, &self.watcher) {
            (Some(owner), _) | (_, NativeWatchDriver::StartupFailure(owner)) => Some(owner),
            _ => None,
        };
        if let Some(owner) = pending {
            let error = demand_watch_error(crate::demand::DemandError::SourceWatchStartup(
                Arc::clone(owner),
            ));
            self.shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .invalidation
                .get_or_insert(WatchInvalidationReason::NativeRescanRequired);
            self.fenced.notify_all();
            return Err(error);
        }
        let started = Instant::now();
        if timeout.is_zero() {
            self.shared
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .invalidation
                .get_or_insert(WatchInvalidationReason::NativeRescanRequired);
            self.fenced.notify_all();
            return Ok(());
        }
        if !self
            .root_unchanged_with_work(work)
            .map_err(demand_watch_error)?
        {
            self.shared
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .invalidation = Some(WatchInvalidationReason::RootChanged);
            self.fenced.notify_all();
            return Ok(());
        }
        #[cfg(windows)]
        let mut children_delivered = true;
        #[cfg(windows)]
        for observed in &self.windows_directories {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })
            .map_err(demand_watch_error)?;
            children_delivered &= observed
                .watcher
                .fence_with_work(timeout.saturating_sub(started.elapsed()), work)
                .map_err(demand_watch_error)?;
        }
        let delivered = match &self.watcher {
            NativeWatchDriver::Source(watcher) => watcher
                .fence_with_work(timeout.saturating_sub(started.elapsed()), work)
                .map_err(demand_watch_error)?,
            NativeWatchDriver::StartupFailure(owner) => {
                return Err(demand_watch_error(
                    crate::demand::DemandError::SourceWatchStartup(Arc::clone(owner)),
                ));
            }
            #[cfg(not(windows))]
            NativeWatchDriver::Adapter(_) => {
                #[cfg(target_os = "macos")]
                {
                    false
                }
                #[cfg(not(target_os = "macos"))]
                {
                    return Err(NativeWatchError::BoundedObservationUnavailable);
                }
            }
        };
        #[cfg(windows)]
        let delivered = delivered && children_delivered;
        let unchanged = self
            .root_unchanged_with_work(work)
            .map_err(demand_watch_error)?;
        let mut state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        if !unchanged {
            state.invalidation = Some(WatchInvalidationReason::RootChanged);
        } else if !delivered {
            state
                .invalidation
                .get_or_insert(WatchInvalidationReason::NativeRescanRequired);
        }
        drop(state);
        self.fenced.notify_all();
        Ok(())
    }

    fn root_unchanged_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<bool, crate::demand::DemandError> {
        work.check()?;
        let Some(root) = self.held_root.root.as_ref() else {
            return Ok(false);
        };
        let held = root
            .stat(
                #[cfg(unix)]
                Path::new(""),
                #[cfg(windows)]
                &[],
                work,
            )?
            .identity();
        match crate::native_host::root_path_identity(&self.root, work) {
            Ok(identity) => Ok(held == self.root_identity && identity == self.root_identity),
            Err(crate::demand::DemandError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Waits until every host change completed before this call is queued.
    pub fn fence(&mut self, timeout: Duration) -> Result<(), NativeWatchError> {
        if let Some(work) = self.original_work.clone() {
            return self.fence_with_native_work(timeout, &work);
        }
        #[cfg(not(windows))]
        let adapter = matches!(self.watcher, NativeWatchDriver::Adapter(_));
        #[cfg(windows)]
        let adapter = false;
        if !adapter || self.pending_source_watch.is_some() {
            let mut work =
                crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
            return self.fence_with_work(timeout, &mut work);
        }
        let delivered = self.fence_cookie().is_ok_and(|cookie| {
            self.await_cookie(&cookie, timeout)
                .is_ok_and(|delivered| delivered)
        });
        let mut state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        state.fence = None;
        if !delivered && state.invalidation.is_none() {
            state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        }
        Ok(())
    }

    fn fence_cookie(&mut self) -> Result<PathBuf, NativeWatchError> {
        let host_directory = self.fence_directory()?;
        let relative = host_directory
            .strip_prefix(&self.root)
            .map_err(|_| NativeWatchError::UnrepresentablePath)?;
        let directory =
            crate::native_capture::host_path_to_namespace(relative, self.profile, self.limits)
                .map_err(|_| NativeWatchError::UnrepresentablePath)?;
        self.watch_directory(&directory)?;
        self.fences = self.fences.wrapping_add(1);
        Ok(host_directory.join(format!(
            ".acyclic-fence-{}-{}",
            std::process::id(),
            self.fences
        )))
    }

    /// Writes `cookie` and reports whether its notification arrived in time.
    fn await_cookie(&self, cookie: &Path, timeout: Duration) -> Result<bool, NativeWatchError> {
        let mut state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        if state.invalidation.is_some() {
            return Ok(false);
        }
        state.fence = Some(cookie.to_path_buf());
        drop(state);
        std::fs::File::create_new(cookie)
            .and_then(|_| std::fs::remove_file(cookie))
            .map_err(NativeWatchError::Io)?;
        let deadline = Instant::now() + timeout;
        let mut state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        while state.fence.is_some() && state.invalidation.is_none() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            state = self
                .fenced
                .wait_timeout(state, remaining)
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .0;
        }
        Ok(state.invalidation.is_none())
    }

    fn fence_directory(&self) -> Result<PathBuf, NativeWatchError> {
        let state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        for host in &state.excluded {
            match host.symlink_metadata() {
                Ok(metadata) if metadata.is_dir() => return Ok(host.clone()),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(NativeWatchError::Io(error)),
            }
        }
        let host = state
            .excluded
            .first()
            .cloned()
            .ok_or(NativeWatchError::InvalidOptions)?;
        drop(state);
        std::fs::create_dir(&host).map_err(NativeWatchError::Io)?;
        Ok(host)
    }

    /// Moves at most `maximum_changes` contiguous hints from the native queue.
    pub fn poll(
        &mut self,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<WatchBatch>, NativeWatchError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        match self.poll_with_work(maximum_changes, &mut work, None, |_| Ok(())) {
            Ok((value, _)) => Ok(OperationReceipt {
                value,
                work: work.work,
            }),
            Err(error) => Err(OperationFailure::new(error, work.work)),
        }
    }

    /// Runs the consumer's guarded publication before committing watch progress.
    pub fn poll_with_native_work_guarded<F>(
        &mut self,
        maximum_changes: u32,
        original: &crate::NativeMaterializationWork,
        before_publish: F,
    ) -> Result<(WatchBatch, crate::NativeAllocation), NativeWatchError>
    where
        F: FnMut(&WatchBatch) -> Result<(), crate::demand::DemandError>,
    {
        let mut work = crate::NativeMaterializationWork::clone(original);
        let (batch, allocation) =
            self.poll_with_work(maximum_changes, &mut work, Some(original), before_publish)?;
        Ok((
            batch,
            allocation.ok_or(NativeWatchError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?,
        ))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    fn poll_with_work<W, F>(
        &mut self,
        maximum_changes: u32,
        work: &mut W,
        original: Option<&crate::NativeMaterializationWork>,
        mut before_publish: F,
    ) -> Result<(WatchBatch, Option<crate::NativeAllocation>), NativeWatchError>
    where
        W: crate::demand::SourceDemandReadWork,
        F: FnMut(&WatchBatch) -> Result<(), crate::demand::DemandError>,
    {
        work.check().map_err(demand_watch_error)?;
        if maximum_changes == 0 {
            return Err(NativeWatchError::ZeroPollLimit);
        }
        if self.rescan_in_progress {
            return Err(NativeWatchError::RescanInProgress);
        }
        // Data always drops before either its old payload or new buffer guards.
        let mut owned = (
            Vec::<WatchChange>::new(),
            WatchBatchOriginal::default(),
            original
                .map(|original| original.claim(0, 0, crate::WorkCounters::UNCHARGED))
                .transpose()?,
        );
        let mut ordinary_bytes = 0;
        let mut dequeued = false;
        let result = (|| {
            let mut reason = self.invalidation()?;
            if reason.is_none() {
                self.verify_current_root_with_work(work)?;
                reason = self.invalidation()?;
            }
            let mut batch = if let Some(reason) = reason {
                WatchBatch::RescanRequired {
                    epoch: self.epoch,
                    reason,
                }
            } else {
                let queued = self.queued.load(Ordering::Acquire).min(maximum_changes);
                let capacity =
                    usize::try_from(queued).map_err(|_| NativeWatchError::InvalidOptions)?;
                let next = self
                    .next_sequence
                    .checked_add(u64::from(queued))
                    .ok_or(NativeWatchError::SequenceExhausted)?;
                let bytes = capacity
                    .checked_mul(size_of::<WatchChange>())
                    .map(crate::foundation::usize_to_u64)
                    .ok_or(WorkError::Overflow)?;
                if capacity != 0 {
                    if let Some(original) = original {
                        owned.2 = Some(original.claim(
                            bytes,
                            u64::from((bytes) != 0),
                            crate::WorkCounters::UNCHARGED,
                        )?);
                    } else {
                        work.claim_allocation(
                            bytes,
                            u64::from((bytes) != 0),
                            crate::WorkCounters::UNCHARGED,
                        )
                        .map_err(demand_watch_error)?;
                        ordinary_bytes = bytes;
                    }
                    owned.0.try_reserve_exact(capacity).map_err(|_| {
                        NativeWatchError::Io(std::io::ErrorKind::OutOfMemory.into())
                    })?;
                    let actual = owned
                        .0
                        .capacity()
                        .checked_mul(size_of::<WatchChange>())
                        .map(crate::foundation::usize_to_u64)
                        .ok_or(WorkError::Overflow)?;
                    if let Some(allocation) = &mut owned.2 {
                        allocation.reconcile_capacity(actual)?;
                    } else {
                        work.with_original_leaf(|work| {
                            work.reconcile_native_allocation(&mut ordinary_bytes, actual)
                        })
                        .map_err(demand_watch_error)?;
                    }
                }
                owned.1 = WatchBatchOriginal::reserve(
                    capacity,
                    original.is_some() || self.original_work.is_some(),
                    work,
                )?;
                owned.1.storage = owned
                    .2
                    .as_mut()
                    .map(|allocation| allocation.split_off(0))
                    .transpose()?;
                while owned.0.len() < capacity {
                    // The queued count witnesses actual items before their sole consumer takes them; no post-dequeue quota check substitutes.
                    work.started_io(WorkCounters {
                        items_examined: 1,
                        items_returned: 1,
                        ..WorkCounters::UNCHARGED
                    })
                    .map_err(demand_watch_error)?;
                    let change = match self
                        .receiver
                        .get_mut()
                        .map_err(|_| NativeWatchError::StatePoisoned)?
                        .try_recv()
                    {
                        Ok(change) => change,
                        Err(TryRecvError::Empty) => {
                            self.invalidate_after_poll_failure(
                                WatchInvalidationReason::BackendError,
                            );
                            return Err(NativeWatchError::NativeWork(
                                crate::NativeTreeMaterializationError::InvalidLayout,
                            ));
                        }
                        Err(TryRecvError::Disconnected) => {
                            self.invalidate_after_poll_failure(
                                WatchInvalidationReason::BackendError,
                            );
                            return Err(NativeWatchError::SourceUnavailable);
                        }
                    };
                    dequeued = true;
                    self.queued.fetch_sub(1, Ordering::AcqRel);
                    let guarded = change.allocations.iter().any(Option::is_some);
                    if owned.1.allocations.capacity() != 0 {
                        if owned.1.allocations.len() == owned.1.allocations.capacity() {
                            return Err(NativeWatchError::NativeWork(
                                crate::NativeTreeMaterializationError::InvalidLayout,
                            ));
                        }
                        owned.1.allocations.push(change.allocations);
                    } else if guarded {
                        return Err(NativeWatchError::NativeWork(
                            crate::NativeTreeMaterializationError::InvalidLayout,
                        ));
                    }
                    owned.0.push(change.change);
                }
                self.forget_removed_directories_with_work(&owned.0, work, original)?;
                WatchBatch::Changes {
                    epoch: self.epoch,
                    first_sequence: self.next_sequence,
                    next_sequence: next,
                    changes: std::mem::take(&mut owned.0),
                    original: std::mem::take(&mut owned.1),
                }
            };
            // Serialize invalidation with publication, but never hold the work meter across arbitrary consumer code or physical data destruction.
            let shared = self.shared.clone();
            let mut state = shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
            if let Some(reason) = state.seal_invalidation() {
                drop(batch);
                batch = WatchBatch::RescanRequired {
                    epoch: self.epoch,
                    reason,
                };
                if let Some(allocation) = &mut owned.2 {
                    allocation.resize(0)?;
                } else if ordinary_bytes != 0 {
                    work.release(ordinary_bytes).map_err(demand_watch_error)?;
                    ordinary_bytes = 0;
                }
            }
            before_publish(&batch).map_err(demand_watch_error)?;
            work.check().map_err(demand_watch_error)?;
            if let WatchBatch::Changes { next_sequence, .. } = &batch {
                self.next_sequence = *next_sequence;
            }
            Ok(batch)
        })();
        match result {
            Ok(batch) => Ok((batch, owned.2.take())),
            Err(error) => {
                drop(owned);
                if ordinary_bytes != 0 {
                    let _ = work.release(ordinary_bytes);
                }
                if dequeued {
                    self.invalidate_after_poll_failure(
                        WatchInvalidationReason::NativeRescanRequired,
                    );
                }
                Err(error)
            }
        }
    }

    fn verify_current_root_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), NativeWatchError> {
        if self.held_root.root.is_none() {
            return Err(NativeWatchError::SourceUnavailable);
        }
        if self
            .root_unchanged_with_work(work)
            .map_err(demand_watch_error)?
        {
            return Ok(());
        }
        self.shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(WatchInvalidationReason::RootChanged);
        Ok(())
    }

    fn invalidation(&self) -> Result<Option<WatchInvalidationReason>, NativeWatchError> {
        self.shared
            .lock()
            .map(|mut state| state.seal_invalidation())
            .map_err(|_| NativeWatchError::StatePoisoned)
    }

    fn invalidate_after_poll_failure(&self, reason: WatchInvalidationReason) {
        // Recovery must remain possible even after cancellation or poisoning.
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.invalidation.get_or_insert(reason);
        self.fenced.notify_all();
    }
}
