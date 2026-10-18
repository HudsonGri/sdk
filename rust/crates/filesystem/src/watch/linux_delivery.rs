#[cfg(any(target_os = "linux", windows))]
struct NativeEventDelivery {
    context: NativeEventContext,
    sender: SyncSender<QueuedWatchChange>,
    shared: Arc<Mutex<SharedState>>,
    queued: Arc<AtomicU32>,
    fenced: Arc<Waiter>,
}

#[cfg(target_os = "linux")]
impl crate::source_watch::NativeEventSink for NativeEventDelivery {
    fn logical_owned_bytes(&self) -> u64 {
        size_of::<Self>() as u64 + self.context.root.capacity() as u64
    }

    fn invalidate_measured(
        &self,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.invalidate_with_work(work)
    }

    fn event_measured(
        &self,
        relative: &Path,
        mask: u32,
        cookie: usize,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.event_with_work(relative, mask, cookie, work)
    }

    fn event_with_native_work(
        &self,
        relative: &Path,
        mask: u32,
        cookie: usize,
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.event_with_work(relative, mask, cookie, work)
    }

    fn invalidate_with_native_work(
        &self,
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.invalidate_with_work(work)
    }
}

#[cfg(target_os = "linux")]
impl NativeEventDelivery {
    fn invalidate_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        {
            let mut state = self
                .shared
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?;
            state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        }
        self.fenced.notify_all();
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "The event transition holds the watcher state lock through queue admission and commit."
    )]
    fn event_with_work<W: crate::demand::SourceDemandReadWork>(
        &self,
        relative: &Path,
        mask: u32,
        cookie: usize,
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::demand::DemandError;
        work.check()?;
        let mut state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        if state
            .fence
            .as_ref()
            .is_some_and(|fence| fence.strip_prefix(&self.context.root) == Ok(relative))
        {
            state.fence = None;
            self.fenced.notify_all();
        }
        if state.invalidation.is_some() {
            return Ok(());
        }
        for excluded in &state.excluded {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            if let Ok(path) = excluded.strip_prefix(&self.context.root)
                && relative.starts_with(path)
            {
                return Ok(());
            }
        }
        if mask
            & (libc::IN_CREATE
                | libc::IN_DELETE
                | libc::IN_MODIFY
                | libc::IN_ATTRIB
                | libc::IN_MOVED_FROM
                | libc::IN_MOVED_TO)
            == 0
        {
            return Ok(());
        }
        if relative.as_os_str().is_empty() {
            state.invalidation = Some(WatchInvalidationReason::RootChanged);
            return Ok(());
        }
        let completes_rename = mask & libc::IN_MOVED_TO != 0
            && state
                .pending_rename
                .as_ref()
                .is_some_and(|pending| pending.tracker == cookie);
        if !completes_rename
            && self
                .queued
                .load(Ordering::Acquire)
                .saturating_add(u32::from(state.pending_rename.is_some()))
                >= self.context.maximum_queued_changes
        {
            state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
            return Ok(());
        }
        let payload = match namespace_hint_with_work(
            relative,
            self.context.profile,
            self.context.limits,
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
            if mask & libc::IN_MOVED_FROM != 0 {
                if cookie == 0 || state.pending_rename.is_some() {
                    state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                } else {
                    state.pending_rename = Some(PendingRename {
                        tracker: cookie,
                        from: payload.0,
                        to: None,
                        allocation: payload.2,
                    });
                }
                return Ok(());
            }
            if mask & libc::IN_MOVED_TO != 0
                && (cookie == 0
                    || !state
                        .pending_rename
                        .as_ref()
                        .is_some_and(|pending| pending.tracker == cookie))
            {
                state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                return Ok(());
            }
            work.started_io(WorkCounters {
                items_returned: 1,
                ..WorkCounters::default()
            })?;
            let change = if mask & libc::IN_MOVED_TO != 0 {
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
            } else {
                let change = if mask & libc::IN_CREATE != 0 {
                    WatchChange::Created(payload.0)
                } else if mask & libc::IN_DELETE != 0 {
                    WatchChange::Removed(payload.0)
                } else if mask & libc::IN_ATTRIB != 0 {
                    WatchChange::MetadataChanged(payload.0)
                } else {
                    WatchChange::Modified(payload.0)
                };
                QueuedWatchChange {
                    change,
                    allocations: [payload.2, None],
                }
            };
            match self.sender.try_send(change) {
                Ok(()) => {
                    self.queued.fetch_add(1, Ordering::Release);
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

enum HintName<'a> {
    Host(&'a std::ffi::OsStr),
    #[cfg(windows)]
    Wide(&'a [u16]),
}

fn host_hint_name(component: Component<'_>) -> Result<HintName<'_>, crate::demand::DemandError> {
    match component {
        Component::Normal(name) => Ok(HintName::Host(name)),
        _ => Err(crate::demand::DemandError::InvalidRequest),
    }
}

fn namespace_hint_with_work<W: crate::demand::SourceDemandReadWork>(
    relative: &Path,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    work: &mut W,
) -> Result<(NamespacePath, u64, Option<crate::NativeAllocation>), crate::demand::DemandError> {
    namespace_hint_names_with_work(
        relative.components().map(host_hint_name),
        profile,
        limits,
        work,
    )
}

#[allow(
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
fn namespace_hint_names_with_work<'a, W: crate::demand::SourceDemandReadWork>(
    names: impl Iterator<Item = Result<HintName<'a>, crate::demand::DemandError>> + Clone,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    work: &mut W,
) -> Result<(NamespacePath, u64, Option<crate::NativeAllocation>), crate::demand::DemandError> {
    use crate::demand::DemandError;
    work.check()?;
    let original = if work.is_native_original() {
        Some(
            work.native_source_work()
                .ok_or(DemandError::NativeWorkUnavailable)?,
        )
    } else {
        None
    };
    let mut count = 0_usize;
    for name in names.clone() {
        work.started_io(WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        })?;
        let _ = name?;
        count = count.checked_add(1).ok_or(WorkError::Overflow)?;
        if count > usize::from(limits.maximum_path_depth) {
            return Err(DemandError::InvalidRequest);
        }
    }
    let backing = count
        .checked_mul(size_of::<LogicalName>())
        .ok_or(WorkError::Overflow)? as u64;
    let allocation = if let Some(original) = &original {
        Some(
            original
                .claim(backing, u64::from(backing != 0), WorkCounters::UNCHARGED)
                .map_err(crate::native_name::original_name_error)?,
        )
    } else {
        work.claim_allocation(
            backing,
            u64::from((backing) != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        None
    };
    // Drop physical components before their native allocation guard.
    let mut owned = (Vec::<LogicalName>::new(), allocation);
    let mut claimed = if owned.1.is_some() { 0 } else { backing };
    let result = (|| {
        let reserved = owned.0.try_reserve_exact(count);
        let actual = owned
            .0
            .capacity()
            .checked_mul(size_of::<LogicalName>())
            .ok_or(WorkError::Overflow)? as u64;
        let reconciled = if let Some(allocation) = &mut owned.1 {
            allocation
                .reconcile_capacity(actual)
                .map_err(crate::native_name::original_name_error)
        } else {
            work.reconcile_allocation(&mut claimed, actual)
        };
        reserved.map_err(DemandError::Allocation)?;
        reconciled?;
        for name in names {
            work.started_io(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })?;
            let name = name?;
            let ((encoding, bytes), mut allocation) = match name {
                HintName::Host(name) => crate::native_name::host_name_bytes_with_source_work(
                    name,
                    profile,
                    limits.maximum_component_bytes,
                    work,
                ),
                #[cfg(windows)]
                HintName::Wide(name) => crate::native_name::host_wide_name_bytes_with_source_work(
                    name,
                    profile,
                    limits.maximum_component_bytes,
                    work,
                ),
            }?;
            if allocation.is_none() {
                claimed = claimed
                    .checked_add(bytes.capacity() as u64)
                    .ok_or(WorkError::Overflow)?;
            }
            let name = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                .map_err(|_| DemandError::InvalidRequest)?;
            // Keep the name before its guard on every custody-transfer error.
            let mut incoming = (name, allocation.take());
            if let (Some(owner), Some(allocation)) = (&mut owned.1, &mut incoming.1) {
                owner.absorb(allocation).map_err(NativeWatchError::from)?;
            }
            owned.0.push(incoming.0);
        }
        NamespacePath::new(std::mem::take(&mut owned.0), limits)
            .map_err(|_| DemandError::InvalidRequest)
    })();
    match result {
        Ok(path) => Ok((path, claimed, owned.1.take())),
        Err(error) => {
            drop(owned);
            if claimed != 0 {
                work.release(claimed)?;
            }
            Err(error)
        }
    }
}
