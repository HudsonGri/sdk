impl NativeWatch {
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
