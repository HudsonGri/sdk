#[cfg(windows)]
fn wide_hint_names(
    path: &[u16],
) -> impl Iterator<Item = Result<HintName<'_>, crate::demand::DemandError>> + Clone {
    path.split(|unit| matches!(*unit, 47 | 92))
        .filter(|name| !name.is_empty())
        .map(|name| Ok(HintName::Wide(name)))
}

#[cfg(windows)]
struct WindowsDirectoryWatch {
    path: Arc<(Vec<u16>, Option<crate::NativeAllocation>)>,
    identity: NativeRootIdentity,
    watcher: crate::source_watch::NativeSourceWatch,
}

#[cfg(windows)]
struct WindowsDirectorySink {
    prefix: Arc<(Vec<u16>, Option<crate::NativeAllocation>)>,
    delivery: Arc<NativeEventDelivery>,
    _allocation: Option<crate::materializer::NativeAllocation>,
}

#[cfg(windows)]
impl crate::source_watch::HostChangeSink for WindowsDirectorySink {
    fn logical_owned_bytes(&self) -> u64 {
        size_of::<Self>() as u64
            + if self.prefix.0.is_empty() {
                std::mem::size_of_val(&*self.prefix) as u64 + self.prefix.0.capacity() as u64 * 2
            } else {
                0
            }
    }
    fn host_changed(
        &self,
        changes: &[crate::source_watch::HostChange],
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.host_changed_with_work(changes, work)
    }

    fn host_changed_with_native_work(
        &self,
        changes: &[crate::source_watch::HostChange],
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.host_changed_with_work(changes, work)
    }

    fn host_changed_wide(
        &self,
        changes: &[crate::source_watch::HostChangeWide<'_>],
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.host_changed_wide_with_work(changes, work)
    }

    fn host_changed_wide_with_native_work(
        &self,
        changes: &[crate::source_watch::HostChangeWide<'_>],
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.host_changed_wide_with_work(changes, work)
    }
}

#[cfg(windows)]
impl WindowsDirectorySink {
    fn host_changed_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        changes: &[crate::source_watch::HostChange],
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::source_watch::HostChange;
        for change in changes {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            match change {
                HostChange::Rebound(path) | HostChange::Altered(path) => {
                    let names = wide_hint_names(&self.prefix.0)
                        .chain(path.components().map(host_hint_name));
                    self.deliver_names_measured(names, None, work)?;
                }
                HostChange::File(_) => {}
                HostChange::Everything | HostChange::Unconfirmed => {
                    self.invalidate_measured(work)?;
                }
            }
        }
        Ok(())
    }

    fn host_changed_wide_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        changes: &[crate::source_watch::HostChangeWide<'_>],
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::source_watch::HostChangeWide;
        let mut file = 1;
        for change in changes {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            if let HostChangeWide::File(identity) = change {
                file = usize::try_from(*identity).map_err(|_| WorkError::Overflow)?;
                break;
            }
        }
        for change in changes {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            match change {
                HostChangeWide::Path { action, path } => {
                    self.deliver_names_measured(
                        wide_hint_names(&self.prefix.0).chain(wide_hint_names(path)),
                        self.prefix.0.is_empty().then_some((*action, file)),
                        work,
                    )?;
                }
                HostChangeWide::File(_) => {}
                HostChangeWide::Everything | HostChangeWide::Unconfirmed => {
                    self.invalidate_measured(work)?;
                }
            }
        }
        Ok(())
    }

    fn invalidate_measured<W: crate::demand::SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        self.delivery
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        self.delivery.fenced.notify_all();
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "This state transition keeps admission, effects and resource retirement in one frame."
    )]
    fn deliver_names_measured<'a, W: crate::demand::SourceDemandReadWork>(
        &self,
        names: impl Iterator<Item = Result<HintName<'a>, crate::demand::DemandError>> + Clone,
        action: Option<(u32, usize)>,
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::demand::DemandError;
        work.check()?;
        let mut state = self
            .delivery
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        if state.invalidation.is_some() {
            return Ok(());
        }
        let completes_rename = action.is_some_and(|(action, cookie)| {
            action == 5
                && state
                    .pending_rename
                    .as_ref()
                    .is_some_and(|pending| pending.tracker == cookie)
        });
        if !completes_rename
            && self
                .delivery
                .queued
                .load(Ordering::Acquire)
                .saturating_add(u32::from(state.pending_rename.is_some()))
                >= self.delivery.context.maximum_queued_changes
        {
            state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
            return Ok(());
        }
        let payload = match namespace_hint_names_with_work(
            names,
            self.delivery.context.profile,
            self.delivery.context.limits,
            work,
        ) {
            Ok(path) => path,
            Err(DemandError::InvalidRequest) => {
                state.invalidation = Some(WatchInvalidationReason::UnrepresentablePath);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let bytes = payload.1;
        let result = (|| {
            let payload = payload;
            for excluded in &state.excluded {
                work.started_io(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::default()
                })?;
                let relative = excluded
                    .strip_prefix(&self.delivery.context.root)
                    .map_err(|_| DemandError::InvalidRequest)?;
                if namespace_starts_with_host_measured(&payload.0, relative, work)? {
                    return Ok(());
                }
            }
            if payload.0.is_root() {
                state.invalidation = Some(WatchInvalidationReason::RootChanged);
                return Ok(());
            }
            if let Some((4, cookie)) = action {
                if state.pending_rename.is_some() {
                    state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                } else {
                    state.pending_rename = Some(PendingRename {
                        tracker: cookie,
                        from: payload.0,
                        allocation: payload.2,
                    });
                }
                return Ok(());
            }
            if action.is_some_and(|(action, cookie)| {
                action == 5
                    && !state
                        .pending_rename
                        .as_ref()
                        .is_some_and(|pending| pending.tracker == cookie)
            }) {
                state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                return Ok(());
            }
            work.started_io(WorkCounters {
                items_returned: 1,
                ..WorkCounters::default()
            })?;
            let change = match action {
                Some((1, _)) => QueuedWatchChange {
                    change: WatchChange::Created(payload.0),
                    allocations: [payload.2, None],
                },
                Some((2, _)) => QueuedWatchChange {
                    change: WatchChange::Removed(payload.0),
                    allocations: [payload.2, None],
                },
                Some((5, _)) => {
                    let pending = state
                        .pending_rename
                        .take()
                        .ok_or(DemandError::InvalidRequest)?;
                    QueuedWatchChange {
                        change: WatchChange::Renamed {
                            from: pending.from,
                            to: payload.0,
                        },
                        allocations: [pending.allocation, payload.2],
                    }
                }
                _ => QueuedWatchChange {
                    change: WatchChange::Modified(payload.0),
                    allocations: [payload.2, None],
                },
            };
            match self.delivery.sender.try_send(change) {
                Ok(()) => {
                    self.delivery.queued.fetch_add(1, Ordering::Release);
                }
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                    state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
                }
            }
            Ok::<(), DemandError>(())
        })();
        if bytes != 0 {
            work.release(bytes)?;
        }
        result
    }
}

#[cfg(windows)]
fn namespace_starts_with_host_measured<W: crate::demand::SourceDemandReadWork>(
    path: &NamespacePath,
    host: &Path,
    work: &mut W,
) -> Result<bool, crate::demand::DemandError> {
    use std::os::windows::ffi::OsStrExt as _;
    let mut names = path.components().iter();
    for component in host.components() {
        work.started_io(WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        })?;
        let (Component::Normal(host), Some(name)) = (component, names.next()) else {
            return Ok(false);
        };
        if name.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le {
            let (units, _) = name.as_bytes().as_chunks::<2>();
            let mut bytes = units.iter();
            for unit in host.encode_wide() {
                work.started_io(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::default()
                })?;
                if bytes.next() != Some(&unit.to_le_bytes()) {
                    return Ok(false);
                }
            }
            if bytes.next().is_some() {
                return Ok(false);
            }
        } else {
            let Some(host) = host.to_str() else {
                return Ok(false);
            };
            if name.as_bytes().len() != host.len() {
                return Ok(false);
            }
            for (expected, actual) in name.as_bytes().iter().zip(host.as_bytes()) {
                work.started_io(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::default()
                })?;
                if expected != actual {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// One live native watcher over a materialized checkout root.
pub struct NativeWatch {
    watcher: NativeWatchDriver,
    root: PathBuf,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    recursive: bool,
    watched_directories: Vec<PathBuf>,
    receiver: Mutex<Receiver<QueuedWatchChange>>,
    held_root: NativeWatchHeldRoot,
    #[cfg(windows)]
    windows_directories: Vec<WindowsDirectoryWatch>,
    #[cfg(windows)]
    windows_backing_allocation: Option<crate::materializer::NativeAllocation>,
    #[cfg(windows)]
    windows_delivery: Arc<NativeEventDelivery>,
    queued: Arc<AtomicU32>,
    // Lock order: shared state -> waiter -> virtual clock. The fence releases
    // shared state before parking; waiter and clock never lock shared state.
    shared: Arc<Mutex<SharedState>>,
    fenced: Arc<Waiter>,
    fences: u64,
    epoch: u64,
    next_sequence: u64,
    rescan_in_progress: bool,
    root_identity: NativeRootIdentity,
    complete: bool,
    original_work: Option<crate::NativeMaterializationWork>,
    _root_allocation: Option<crate::NativeAllocation>,
    pending_source_watch: Option<Arc<crate::source_watch::SourceWatchStartupFailure>>,
    owner_allocation: Option<crate::NativeAllocation>,
}

impl std::fmt::Debug for NativeWatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeWatch")
            .field("root_identity", &self.root_identity)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

struct NativeWatchHeldRoot {
    root: Option<crate::native_host::HostRoot>,
    cleanup: Option<crate::materializer::NativeCleanupWrite>,
}

impl NativeWatchHeldRoot {
    fn close_with_native_work(
        &mut self,
        work: &crate::NativeMaterializationWork,
        birth: Option<&crate::NativeMaterializationWork>,
    ) -> Result<bool, crate::NativeTreeMaterializationError> {
        work.admit(WorkCounters::UNCHARGED)?;
        if self.root.is_none() {
            return Ok(false);
        }
        let mut superseded_cleanup = None;
        let cleanup = if birth.is_some_and(|original| original.shares_ledger(work)) {
            self.cleanup
                .as_mut()
                .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?
                .bind_current_work(work)?;
            self.cleanup
                .take()
                .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?
        } else {
            // Admit the current invocation's close before taking custody. The birth reservation remains resident until physical close.
            let cleanup = work.reserve_cleanup_write()?;
            superseded_cleanup = self.cleanup.take();
            cleanup
        };
        let root = self
            .root
            .take()
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
        let ((), recorded) = cleanup.attempt(|| drop(root));
        drop(superseded_cleanup);
        recorded?;
        Ok(true)
    }
}

impl Drop for NativeWatchHeldRoot {
    fn drop(&mut self) {
        if let Some(root) = self.root.take() {
            if let Some(cleanup) = self.cleanup.take() {
                // This exact original close was reserved before opening. The primitive releases the meter before physical close.
                let _ = cleanup.attempt(|| drop(root));
            } else {
                drop(root);
            }
        }
    }
}
