impl NativeWatch {
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
        let started = deadline::now();
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
                .fence_with_work(timeout.saturating_sub(deadline::elapsed(started)), work)
                .map_err(demand_watch_error)?;
        }
        let delivered = match &self.watcher {
            NativeWatchDriver::Source(watcher) => watcher
                .fence_with_work(timeout.saturating_sub(deadline::elapsed(started)), work)
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
        let deadline = Deadline::after(timeout);
        loop {
            let generation = self.fenced.listen();
            let state = self
                .shared
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?;
            if state.fence.is_none() || state.invalidation.is_some() {
                return Ok(state.invalidation.is_none());
            }
            if deadline.expired() {
                return Ok(false);
            }
            drop(state);
            self.fenced.wait_after(generation, Some(deadline));
        }
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
}
