impl NativeWatch {
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
}
