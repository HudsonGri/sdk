impl NativeWatch {
    /// Opens the platform-recommended event source.
    pub fn open(
        root: impl AsRef<Path>,
        options: NativeWatchOptions,
    ) -> Result<Self, NativeWatchError> {
        Self::open_with_profile(root, native_filesystem_profile(), options)
    }

    /// Opens a watcher whose emitted names use the supplied volume profile.
    pub fn open_with_profile(
        root: impl AsRef<Path>,
        profile: FilesystemProfile,
        options: NativeWatchOptions,
    ) -> Result<Self, NativeWatchError> {
        Self::open_with_profile_measured(
            root,
            profile,
            options,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    }

    /// Transfers an actual already-paid owner allocation on this same ledger.
    pub fn retain_owner_allocation(
        &mut self,
        incoming: &mut crate::NativeAllocation,
    ) -> Result<(), crate::NativeTreeMaterializationError> {
        let original = self
            .original_work
            .as_ref()
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
        if !original.shares_ledger(incoming.work())
            || !original.shares_resource_allowance(incoming.work())
        {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout);
        }
        if let Some(owner) = &mut self.owner_allocation {
            owner.absorb(incoming)
        } else {
            self.owner_allocation = Some(incoming.split_off(incoming.bytes())?);
            Ok(())
        }
    }

    /// Settles the same native producer, its child producers, and held root.
    pub fn shutdown_with_native_work(
        &mut self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeWatchError> {
        original.admit(WorkCounters::UNCHARGED)?;
        let mut work = crate::NativeMaterializationWork::clone(original);
        if let Some(pending) = self.pending_source_watch.as_ref() {
            pending
                .stop_with_native_work(original)
                .map_err(demand_watch_error)?;
            if !pending.is_stopped() {
                return Err(NativeWatchError::SourceUnavailable);
            }
            drop(self.pending_source_watch.take());
        }
        #[cfg(windows)]
        {
            self.clear_windows_directories_with_work(&mut work)
                .map_err(demand_watch_error)?;
        }
        match &self.watcher {
            NativeWatchDriver::Source(watcher) => {
                watcher
                    .stop_with_work(&mut work)
                    .map_err(demand_watch_error)?;
                if !watcher.is_stopped() {
                    return Err(NativeWatchError::SourceUnavailable);
                }
            }
            NativeWatchDriver::StartupFailure(owner) => {
                owner
                    .stop_with_native_work(original)
                    .map_err(demand_watch_error)?;
                if !owner.is_stopped() {
                    return Err(NativeWatchError::SourceUnavailable);
                }
            }
            #[cfg(not(windows))]
            NativeWatchDriver::Adapter(_) => {
                return Err(NativeWatchError::BoundedObservationUnavailable);
            }
        }
        self.held_root
            .close_with_native_work(original, self.original_work.as_ref())?;
        loop {
            crate::native_host::NativeHostWork::started_io(
                &mut work,
                WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                },
            )
            .map_err(demand_watch_error)?;
            match self
                .receiver
                .get_mut()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .try_recv()
            {
                Ok(change) => {
                    self.queued.fetch_sub(1, Ordering::AcqRel);
                    drop(change);
                    original.record_admitted(WorkCounters::UNCHARGED)?;
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        let mut state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        #[cfg(any(target_os = "linux", windows))]
        let pending = state.pending_rename.take();
        let fence = state.fence.take();
        state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        drop(state);
        #[cfg(any(target_os = "linux", windows))]
        drop(pending);
        drop(fence);
        original.record_admitted(WorkCounters::UNCHARGED)?;
        Ok(true)
    }

    /// Opens the existing watcher with explicit preparation work/cancellation.
    pub fn open_with_profile_measured(
        root: impl AsRef<Path>,
        profile: FilesystemProfile,
        options: NativeWatchOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<Self>, NativeWatchError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result =
            Self::open_with_profile_engine(root.as_ref(), profile, options, &mut work, None);
        let spent = work.receipt();
        result
            .map(|value| OperationReceipt { value, work: spent })
            .map_err(|error| OperationFailure::new(error, spent))
    }

    /// Opens the same held-root watcher on the invocation's original ledger.
    pub fn open_with_profile_with_native_work(
        root: &Path,
        profile: FilesystemProfile,
        options: NativeWatchOptions,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeWatchError> {
        Self::open_with_profile_engine(
            root,
            profile,
            options,
            &mut crate::NativeMaterializationWork::clone(original),
            Some(original),
        )
    }

    fn copy_startup_path<W: crate::demand::SourceDemandReadWork>(
        path: &Path,
        work: &mut W,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(PathBuf, Option<crate::NativeAllocation>), NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        if let Some(original) = original {
            let (path, allocation) = original.path(path, None)?;
            Ok((path, Some(allocation)))
        } else {
            let (path, _) =
                crate::native_host::clone_path(path, work).map_err(demand_watch_error)?;
            Ok((path, None))
        }
    }

    fn startup_control<W: crate::demand::SourceDemandReadWork>(
        work: &mut W,
    ) -> Result<(), NativeWatchError> {
        // Refcount/control headers are not logical data, but this known controlled allocator call is still admitted before it happens.
        work.started_io(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(demand_watch_error)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    fn open_with_profile_engine<W: crate::demand::SourceDemandReadWork>(
        requested_root: &Path,
        profile: FilesystemProfile,
        options: NativeWatchOptions,
        work: &mut W,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        let options = options.validate()?;
        if cfg!(target_os = "linux")
            && options.recursive
            && (original.is_some()
                || work
                    .remaining()
                    .map_err(demand_watch_error)?
                    .has_finite_allowances())
        {
            return Err(NativeWatchError::InvalidOptions);
        }
        if original.is_some() && !cfg!(any(target_os = "linux", target_os = "macos", windows)) {
            return Err(NativeWatchError::NativeWorkUnavailable);
        }
        let use_adapter = if cfg!(target_os = "linux") {
            options.recursive
        } else if cfg!(windows) {
            false
        } else {
            original.is_none()
        };
        let ordinary_root;
        let root_path = if original.is_some() {
            requested_root
        } else {
            work.started_io(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(demand_watch_error)?;
            if requested_root.symlink_metadata()?.file_type().is_symlink() {
                return Err(NativeWatchError::RootIsNotDirectory);
            }
            work.started_io(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(demand_watch_error)?;
            ordinary_root = requested_root.canonicalize()?;
            &ordinary_root
        };
        let cleanup = original
            .map(|work| work.allocation_work().reserve_cleanup_write())
            .transpose()?;
        let held =
            crate::native_host::HostRoot::open(root_path, work).map_err(demand_watch_error)?;
        let held_root = NativeWatchHeldRoot {
            root: Some(held),
            cleanup,
        };
        let held = held_root
            .root
            .as_ref()
            .ok_or(NativeWatchError::SourceUnavailable)?;
        let metadata = held
            .stat(
                #[cfg(unix)]
                Path::new(""),
                #[cfg(windows)]
                &[],
                work,
            )
            .map_err(demand_watch_error)?;
        if !metadata.is_dir() {
            return Err(NativeWatchError::RootIsNotDirectory);
        }
        let root_identity = held.identity();
        if metadata.identity() != root_identity {
            return Err(NativeWatchError::RootChanged);
        }
        #[cfg(windows)]
        let complete = crate::native_host::reports_every_change(&held.directory_handle(), work)
            .map_err(demand_watch_error)?;
        #[cfg(not(windows))]
        let complete = true;
        let (root, root_allocation) = Self::copy_startup_path(root_path, work, original)?;
        let capacity = usize::try_from(options.maximum_queued_changes)
            .map_err(|_| NativeWatchError::InvalidOptions)?;
        let queue_bytes = (capacity as u64)
            .checked_mul(size_of::<WatchChange>() as u64)
            .ok_or(WorkError::Overflow)?;
        let queue_allocation = if let Some(original) = original {
            Some(original.claim(
                queue_bytes,
                u64::from((queue_bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )?)
        } else {
            work.claim_allocation(
                queue_bytes,
                u64::from((queue_bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )
            .map_err(demand_watch_error)?;
            None
        };
        Self::startup_control(work)?;
        let (sender, receiver) = sync_channel(capacity);
        Self::startup_control(work)?;
        let queued = Arc::new(AtomicU32::new(0));
        Self::startup_control(work)?;
        let mut state = SharedState::new(Some(WatchInvalidationReason::InitialSnapshotRequired));
        state._queue_allocation = queue_allocation;
        let shared = Arc::new(Mutex::new(state));
        Self::startup_control(work)?;
        let fenced = Arc::new(Waiter::new());
        #[cfg(windows)]
        let (context_root, context_allocation) = Self::copy_startup_path(&root, work, original)?;
        #[cfg(windows)]
        let context = NativeEventContext {
            root: context_root,
            #[cfg(test)]
            root_identity,
            profile,
            limits: options.limits,
            #[cfg(any(target_os = "linux", windows))]
            maximum_queued_changes: options.maximum_queued_changes,
            _root_allocation: context_allocation,
        };
        #[cfg(windows)]
        let windows_delivery = {
            Self::startup_control(work)?;
            Arc::new(NativeEventDelivery {
                context,
                sender,
                shared: Arc::clone(&shared),
                queued: Arc::clone(&queued),
                fenced: Arc::clone(&fenced),
            })
        };
        #[cfg(target_os = "linux")]
        let linux_sender = sender.clone();
        #[cfg(not(windows))]
        let callback = if use_adapter {
            let (context_root, context_allocation) =
                Self::copy_startup_path(&root, work, original)?;
            let context = NativeEventContext {
                root: context_root,
                root_identity,
                profile,
                limits: options.limits,
                #[cfg(target_os = "linux")]
                maximum_queued_changes: options.maximum_queued_changes,
                _root_allocation: context_allocation,
            };
            let callback_shared = Arc::clone(&shared);
            let callback_fenced = Arc::clone(&fenced);
            let callback_queued = Arc::clone(&queued);
            Some(move |event: notify::Result<Event>| {
                accept_native_event(
                    &event,
                    &context,
                    &sender,
                    &callback_shared,
                    &callback_queued,
                );
                if let (Ok(event), Ok(mut state)) = (&event, callback_shared.lock())
                    && state
                        .fence
                        .as_ref()
                        .is_some_and(|fence| event.paths.contains(fence))
                {
                    state.fence = None;
                    callback_fenced.notify_all();
                }
            })
        } else {
            None
        };
        let mut watched_directories = Vec::new();
        if use_adapter {
            work.claim_allocation(0, 0, crate::WorkCounters::UNCHARGED)
                .map_err(demand_watch_error)?;
            watched_directories.try_reserve_exact(1).map_err(|error| {
                NativeWatchError::NativeWork(
                    crate::NativeTreeMaterializationError::AllocationFailed(error),
                )
            })?;
            let (directory, _) =
                crate::native_host::clone_path(&root, work).map_err(demand_watch_error)?;
            watched_directories.push(directory);
        }
        #[cfg(target_os = "linux")]
        let source_result = if options.recursive {
            None
        } else {
            let (event_root, event_allocation) = Self::copy_startup_path(&root, work, original)?;
            Self::startup_control(work)?;
            let events = Arc::new(NativeEventDelivery {
                context: NativeEventContext {
                    root: event_root,
                    root_identity,
                    profile,
                    limits: options.limits,
                    maximum_queued_changes: options.maximum_queued_changes,
                    _root_allocation: event_allocation,
                },
                sender: linux_sender,
                shared: Arc::clone(&shared),
                queued: Arc::clone(&queued),
                fenced: Arc::clone(&fenced),
            });
            let sink = Arc::clone(&shared);
            Some(
                crate::source_watch::NativeSourceWatch::start_with_events_with_work(
                    held, sink, events, work,
                ),
            )
        };
        #[cfg(windows)]
        let source_result = {
            Self::startup_control(work)?;
            let prefix = Arc::new((Vec::new(), None));
            Self::startup_control(work)?;
            let sink = Arc::new(WindowsDirectorySink {
                prefix,
                delivery: Arc::clone(&windows_delivery),
                _allocation: None,
            });
            Some(
                crate::source_watch::NativeSourceWatch::start_held_with_sink_with_work(
                    held,
                    sink,
                    complete,
                    options.recursive,
                    work,
                ),
            )
        };
        #[cfg(target_os = "macos")]
        let source_result = if use_adapter {
            None
        } else {
            let sink = Arc::clone(&shared);
            Some(crate::source_watch::NativeSourceWatch::start(
                held, sink, work,
            ))
        };
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        let source_result: Option<
            Result<crate::source_watch::NativeSourceWatch, crate::DemandError>,
        > = None;
        let watcher = if let Some(source) = source_result {
            match source {
                Ok(watcher) => NativeWatchDriver::Source(watcher),
                Err(crate::DemandError::SourceWatchStartup(owner)) => {
                    NativeWatchDriver::StartupFailure(owner)
                }
                Err(error) => return Err(demand_watch_error(error)),
            }
        } else {
            Self::startup_control(work)?;
            #[cfg(not(windows))]
            let adapter = NativeEventWatcher::new(
                callback.ok_or(NativeWatchError::SourceUnavailable)?,
                notify::Config::default(),
            )
            .map_err(NativeWatchError::Backend)?;
            #[cfg(windows)]
            unreachable!("Windows always uses its original held source");
            #[cfg(not(windows))]
            {
                work.started_io(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(demand_watch_error)?;
                let mut adapter = adapter;
                adapter
                    .watch(
                        &root,
                        if options.recursive {
                            RecursiveMode::Recursive
                        } else {
                            RecursiveMode::NonRecursive
                        },
                    )
                    .map_err(NativeWatchError::Backend)?;
                NativeWatchDriver::Adapter(adapter)
            }
        };
        let watch = Self {
            watcher,
            root,
            profile,
            limits: options.limits,
            recursive: options.recursive,
            watched_directories,
            receiver: Mutex::new(receiver),
            held_root,
            #[cfg(windows)]
            windows_directories: Vec::new(),
            #[cfg(windows)]
            windows_backing_allocation: None,
            #[cfg(windows)]
            windows_delivery,
            queued,
            shared,
            fenced,
            fences: 0,
            epoch: 0,
            next_sequence: 0,
            rescan_in_progress: false,
            root_identity,
            complete,
            original_work: original.map(crate::NativeMaterializationWork::allocation_work),
            _root_allocation: root_allocation,
            owner_allocation: None,
            pending_source_watch: None,
        };
        if let NativeWatchDriver::StartupFailure(cause) = &watch.watcher {
            return Err(NativeWatchError::SourceStartupFailed {
                cause: Arc::clone(cause),
                watch: Box::new(watch),
            });
        }
        let checked = if let Some(original) = original {
            original.admit(WorkCounters::UNCHARGED)
        } else {
            work.check()
                .map_err(crate::materializer::native_materialization_work_error)
        };
        match checked {
            Ok(()) => Ok(watch),
            Err(cause) => Err(NativeWatchError::StartupFailed {
                cause,
                watch: Box::new(watch),
            }),
        }
    }

    /// Whether the root's file system reports every change, or that it lost some.
    #[must_use]
    pub const fn reports_every_change(&self) -> bool {
        self.complete
    }
}
