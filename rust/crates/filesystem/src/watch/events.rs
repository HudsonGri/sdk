#[cfg(windows)]
fn watch_claim_allocation<W: crate::demand::SourceDemandReadWork>(
    bytes: u64,
    work: &mut W,
) -> Result<Option<crate::materializer::NativeAllocation>, crate::demand::DemandError> {
    work.check()?;
    if work.is_native_original() {
        let native = work
            .native_source_work()
            .ok_or(crate::demand::DemandError::NativeWorkUnavailable)?;
        return native
            .claim(
                bytes,
                u64::from((bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )
            .map(Some)
            .map_err(|error| NativeWatchError::from(error).into());
    }
    work.claim_allocation(
        bytes,
        u64::from((bytes) != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    Ok(None)
}

impl crate::demand::DemandDirectoryObserver for Mutex<NativeWatch> {
    fn observe_directory(
        &self,
        directory: &NamespacePath,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), crate::demand::DemandError> {
        let mut work = crate::demand::native::callback_work(accounting)?;
        let result = (|| {
            crate::native_host::NativeHostWork::check(&work)?;
            self.lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .watch_directory_with_work(directory, &mut work)
        })();
        crate::demand::native::finish_context(result, work, accounting)
    }
}

#[cfg(target_os = "linux")]
/// Grows one owned path only after its logical capacity and copied bytes have been admitted.
fn grow_watch_path_with_work<W: crate::demand::SourceDemandReadWork>(
    candidate: &mut PathBuf,
    part: &Path,
    work: &mut W,
    owned_bytes: &mut u64,
) -> Result<(), crate::demand::DemandError> {
    let last = candidate.as_os_str().as_encoded_bytes().last().copied();
    #[cfg(unix)]
    let trailing_separator = last == Some(b'/');
    let separator = usize::from(!candidate.as_os_str().is_empty() && !trailing_separator);
    let capacity = candidate
        .as_os_str()
        .len()
        .checked_add(part.as_os_str().len())
        .and_then(|bytes| bytes.checked_add(separator))
        .ok_or(WorkError::Overflow)?;
    let bytes = capacity as u64;
    let copied = WorkCounters {
        bytes_copied: bytes,
        ..WorkCounters::default()
    };
    work.admit(copied)?;
    work.claim_allocation(
        bytes,
        u64::from((bytes) != 0),
        crate::WorkCounters::UNCHARGED,
    )?;
    let mut grown = PathBuf::with_capacity(capacity);
    grown.push(candidate.as_path());
    grown.push(part);
    if let Err(error) = work.record(copied) {
        drop(grown);
        work.release(bytes)?;
        return Err(error);
    }
    drop(std::mem::replace(candidate, grown));
    work.release(*owned_bytes)?;
    *owned_bytes = bytes;
    Ok(())
}

#[cfg(target_os = "linux")]
/// The same exact spelling conversion as native capture, with each temporary owned buffer admitted rather than hiding conversion behind a watch charge.
fn watch_host_name_with_work<W: crate::demand::SourceDemandReadWork>(
    component: &LogicalName,
    work: &mut W,
) -> Result<(std::ffi::OsString, u64), crate::demand::DemandError> {
    use crate::kernel::NameEncoding;
    use std::ffi::OsString;
    let source = component.as_bytes();
    match component.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStringExt as _;
                let bytes = u64::try_from(source.len()).map_err(|_| WorkError::Overflow)?;
                work.admit(WorkCounters {
                    bytes_copied: bytes,
                    ..WorkCounters::default()
                })?;
                work.claim_allocation(
                    bytes,
                    u64::from((bytes) != 0),
                    crate::WorkCounters::UNCHARGED,
                )?;
                let name = OsString::from_vec(source.to_vec());
                if let Err(error) = work.record(WorkCounters {
                    bytes_copied: bytes,
                    ..WorkCounters::default()
                }) {
                    drop(name);
                    work.release(bytes)?;
                    return Err(error);
                }
                Ok((name, bytes))
            }
        }
        NameEncoding::WindowsUtf16Le => {
            let units = crate::kernel::types::utf16le_units(source)
                .ok_or(NativeWatchError::UnrepresentablePath)?;
            let mut bytes = 0_usize;
            let mut decoded_items = 0_u64;
            for decoded in char::decode_utf16(units.clone()) {
                work.started_io(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::default()
                })?;
                let decoded = decoded.map_err(|_| NativeWatchError::UnrepresentablePath)?;
                bytes = bytes
                    .checked_add(decoded.len_utf8())
                    .ok_or(WorkError::Overflow)?;
                decoded_items = decoded_items.checked_add(1).ok_or(WorkError::Overflow)?;
            }
            let logical = bytes as u64;
            let copied = WorkCounters {
                bytes_copied: logical,
                items_examined: decoded_items,
                ..WorkCounters::default()
            };
            work.admit(copied)?;
            work.claim_allocation(
                logical,
                u64::from((logical) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
            let mut name = String::with_capacity(bytes);
            let result = (|| {
                for decoded in char::decode_utf16(units) {
                    name.push(decoded.map_err(|_| NativeWatchError::UnrepresentablePath)?);
                }
                work.record(copied)
            })();
            if let Err(error) = result {
                drop(name);
                work.release(logical)?;
                return Err(error);
            }
            Ok((OsString::from(name), logical))
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "This state transition keeps admission, effects and resource retirement in one frame."
)]
#[cfg(any(test, not(windows)))]
fn accept_native_event(
    event: &notify::Result<Event>,
    context: &NativeEventContext,
    sender: &SyncSender<QueuedWatchChange>,
    shared: &Arc<Mutex<SharedState>>,
    queued: &Arc<AtomicU32>,
) {
    let Ok(mut state) = shared.lock() else {
        return;
    };
    if state.invalidation.is_some() {
        return;
    }
    if let Ok(event) = event
        && !event.paths.is_empty()
        && event.paths.iter().all(|path| {
            state
                .excluded
                .iter()
                .any(|excluded| path.starts_with(excluded))
        })
    {
        return;
    }
    let mapped = (|| {
        let event = event
            .as_ref()
            .map_err(|_| WatchInvalidationReason::BackendError)?;
        validate_replayed_root_creation(event, &context.root, context.root_identity)?;
        #[cfg(target_os = "linux")]
        if let EventKind::Modify(ModifyKind::Name(mode)) = event.kind {
            if event.need_rescan() {
                return Err(WatchInvalidationReason::NativeRescanRequired);
            }
            let tracker = event
                .attrs
                .tracker()
                .ok_or(WatchInvalidationReason::AmbiguousRename)?;
            if matches!(mode, RenameMode::From | RenameMode::To) {
                let [path] = event.paths.as_slice() else {
                    return Err(WatchInvalidationReason::AmbiguousRename);
                };
                let path =
                    relative_namespace_path(&context.root, path, context.profile, context.limits)?;
                if path.is_root() {
                    return Err(WatchInvalidationReason::RootChanged);
                }
                match mode {
                    RenameMode::From if state.pending_rename.is_none() => {
                        if queued.load(Ordering::Acquire) >= context.maximum_queued_changes {
                            return Err(WatchInvalidationReason::QueueOverflow);
                        }
                        state.pending_rename = Some(PendingRename {
                            tracker,
                            from: path,
                            to: None,
                            allocation: None,
                        });
                    }
                    RenameMode::To => {
                        let pending = state
                            .pending_rename
                            .as_mut()
                            .ok_or(WatchInvalidationReason::AmbiguousRename)?;
                        if pending.tracker != tracker || pending.to.is_some() {
                            return Err(WatchInvalidationReason::AmbiguousRename);
                        }
                        pending.to = Some(path);
                    }
                    _ => return Err(WatchInvalidationReason::AmbiguousRename),
                }
                return Ok(Vec::new());
            }
            if mode == RenameMode::Both {
                let changes = map_event(event, &context.root, context.profile, context.limits)?;
                let [WatchChange::Renamed { from, to }] = changes.as_slice() else {
                    return Err(WatchInvalidationReason::AmbiguousRename);
                };
                let matched = state.pending_rename.as_ref().is_some_and(|pending| {
                    pending.tracker == tracker
                        && &pending.from == from
                        && pending.to.as_ref() == Some(to)
                });
                if !matched {
                    return Err(WatchInvalidationReason::AmbiguousRename);
                }
                state.pending_rename = None;
                return Ok(changes);
            }
        }
        map_event(event, &context.root, context.profile, context.limits)
    })();
    let changes = match mapped {
        Ok(changes) => changes,
        Err(reason) => {
            state.invalidation = Some(reason);
            return;
        }
    };
    for change in changes {
        #[cfg(target_os = "linux")]
        if queued
            .load(Ordering::Acquire)
            .saturating_add(u32::from(state.pending_rename.is_some()))
            >= context.maximum_queued_changes
        {
            state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
            break;
        }
        match sender.try_send(QueuedWatchChange {
            change,
            allocations: [None, None],
        }) {
            Ok(()) => {
                queued.fetch_add(1, Ordering::Release);
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
                break;
            }
        }
    }
}

#[cfg(any(test, not(windows)))]
fn validate_replayed_root_creation(
    event: &Event,
    root: &Path,
    admitted_root_identity: NativeRootIdentity,
) -> Result<(), WatchInvalidationReason> {
    #[cfg(target_os = "macos")]
    if event.paths.iter().any(|path| path == root) && matches!(event.kind, EventKind::Create(_)) {
        let observed = open_native_root(root)
            .and_then(|file| NativeRootIdentity::from_file(&file))
            .ok();
        if observed != Some(admitted_root_identity) {
            return Err(WatchInvalidationReason::RootChanged);
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = (event, root, admitted_root_identity);
    Ok(())
}

#[cfg(any(test, not(windows)))]
fn map_event(
    event: &Event,
    root: &Path,
    profile: FilesystemProfile,
    limits: VolumeLimits,
) -> Result<Vec<WatchChange>, WatchInvalidationReason> {
    if event.need_rescan() {
        return Err(WatchInvalidationReason::NativeRescanRequired);
    }
    if matches!(event.kind, EventKind::Access(_)) {
        return Ok(Vec::new());
    }
    if event.paths.is_empty() || event.paths.len() > 2 {
        return Err(WatchInvalidationReason::UnrepresentablePath);
    }
    let mut paths = Vec::new();
    paths
        .try_reserve_exact(event.paths.len())
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    for path in &event.paths {
        // A replayed root creation is harmless; root removal and rename still invalidate continuity.
        #[cfg(target_os = "macos")]
        if path == root && matches!(event.kind, EventKind::Create(_)) {
            continue;
        }
        paths.push(relative_namespace_path(root, path, profile, limits)?);
    }
    if paths.iter().any(NamespacePath::is_root)
        && matches!(
            event.kind,
            EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_))
        )
    {
        return Err(WatchInvalidationReason::RootChanged);
    }
    // Coalesced root hints require a rescan because the changed descendant is unknown.
    #[cfg(target_os = "macos")]
    if paths.iter().any(NamespacePath::is_root) {
        return Err(WatchInvalidationReason::NativeRescanRequired);
    }
    if let EventKind::Modify(ModifyKind::Name(mode)) = event.kind {
        return match (mode, paths.as_slice()) {
            (RenameMode::Both, [from, to]) => Ok(vec![WatchChange::Renamed {
                from: from.clone(),
                to: to.clone(),
            }]),
            // inotify pairs both halves by cookie before they reach here (see `accept_native_event`), so a lone half on Linux means the other half was lost.
            #[cfg(target_os = "linux")]
            _ => Err(WatchInvalidationReason::AmbiguousRename),
            // Windows identifies arrival and departure halves, so unpaired renames retain exact direction.
            #[cfg(windows)]
            (RenameMode::To, [to]) => Ok(vec![WatchChange::Arrived(to.clone())]),
            #[cfg(windows)]
            (RenameMode::From, [from]) => Ok(vec![WatchChange::Removed(from.clone())]),
            // FSEvents rename halves require final-state observation, including subtree removal when absent.
            #[cfg(not(target_os = "linux"))]
            (_, paths) => Ok(paths.iter().cloned().map(WatchChange::Modified).collect()),
        };
    }
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(paths.len())
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    for path in paths {
        changes.push(match event.kind {
            // Darwin creation hints may be replayed; observe final state without replacing identity.
            #[cfg(target_os = "macos")]
            EventKind::Create(_) => WatchChange::Modified(path),
            #[cfg(not(target_os = "macos"))]
            EventKind::Create(_) => WatchChange::Created(path),
            EventKind::Remove(_) => WatchChange::Removed(path),
            // Darwin metadata hints may include coalesced content changes, so restage final state.
            #[cfg(target_os = "macos")]
            EventKind::Modify(ModifyKind::Metadata(_)) => WatchChange::Modified(path),
            #[cfg(not(target_os = "macos"))]
            EventKind::Modify(ModifyKind::Metadata(_)) => WatchChange::MetadataChanged(path),
            EventKind::Modify(_) | EventKind::Any | EventKind::Other => WatchChange::Modified(path),
            EventKind::Access(_) => continue,
        });
    }
    Ok(changes)
}

#[cfg(any(test, not(windows)))]
fn relative_namespace_path(
    root: &Path,
    path: &Path,
    profile: FilesystemProfile,
    limits: VolumeLimits,
) -> Result<NamespacePath, WatchInvalidationReason> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    let mut components = Vec::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(WatchInvalidationReason::UnrepresentablePath);
        };
        let (encoding, bytes) =
            crate::native_name::host_name_bytes(name, profile, limits.maximum_component_bytes)
                .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
        let logical = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
            .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
        components.push(logical);
    }
    NamespacePath::new(components, limits)
        .map_err(|_error: NamespacePathError| WatchInvalidationReason::UnrepresentablePath)
}

/// Native watcher failures that are not recoverable through a baseline rescan.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum NativeWatchError {
    /// Queue or volume bounds are invalid.
    #[error("native watcher options are invalid")]
    InvalidOptions,
    /// The compiled nonrecursive adapter has no actual bounded admission API.
    #[error("bounded native directory observation is unavailable")]
    BoundedObservationUnavailable,
    /// Watch root is not a directory.
    #[error("native watcher root is not a directory")]
    RootIsNotDirectory,
    /// A demanded directory traverses a symbolic link and cannot be mapped back to one exact namespace path.
    #[error("native watcher directory traverses a symbolic link")]
    SymbolicLinkDirectory,
    /// A demanded observer path is not a directory.
    #[error("native watcher observed path is not a directory")]
    ObservedPathNotDirectory,
    /// A demanded namespace path cannot be represented exactly on this host.
    #[error("native watcher directory is not representable on this host")]
    UnrepresentablePath,
    /// A separately opened capture root does not match the watched directory.
    #[error("native watcher root changed identity")]
    RootChanged,
    /// Root path I/O failed.
    #[error("native watcher I/O failed: {0}")]
    Io(#[source] std::io::Error),
    /// Native backend setup failed.
    #[error("native watcher backend failed: {0}")]
    Backend(#[source] notify::Error),
    /// Original partial source startup cause, already retained before failure.
    #[error(transparent)]
    SourceStartup(Arc<crate::source_watch::SourceWatchStartupFailure>),
    /// A partial source and the complete same root watcher remain owned.
    #[error("native source watcher startup failed: {cause}")]
    SourceStartupFailed {
        /// Original preallocated source failure custody.
        #[source]
        cause: Arc<crate::source_watch::SourceWatchStartupFailure>,
        /// Same actual watcher, retained for explicitly bounded shutdown.
        watch: Box<NativeWatch>,
    },
    /// Original native preparation admission or allocation cause.
    #[error(transparent)]
    NativeWork(#[from] crate::NativeTreeMaterializationError),
    /// A started watcher remains owned after a later preparation failure.
    #[error("native watcher startup failed: {cause}")]
    StartupFailed {
        /// Original typed preparation cause.
        #[source]
        cause: crate::NativeTreeMaterializationError,
        /// Same actual watcher, retained for explicitly bounded shutdown.
        watch: Box<NativeWatch>,
    },
    /// Original source reference is stale.
    #[error("source reference is stale")]
    StaleSource,
    /// Original source version is stale.
    #[error("source node changed during the request")]
    StaleVersion,
    /// Original directory cursor is stale.
    #[error("source directory cursor is expired or mismatched")]
    StaleCursor,
    /// Original directory cursor is busy.
    #[error("source directory cursor is in use")]
    CursorBusy,
    /// Original held source root is unavailable.
    #[error("source root is unavailable")]
    SourceUnavailable,
    /// Original requested source path is absent.
    #[error("source path is absent")]
    Absent,
    /// Original source node is not a regular file.
    #[error("source node is not a regular file")]
    NotRegularFile,
    /// Original source node is not a directory.
    #[error("source node is not a directory")]
    NotDirectory,
    /// Original source request is invalid.
    #[error("source request is invalid")]
    InvalidRequest,
    /// Original native source worker is unavailable.
    #[error("native source worker failed")]
    WorkerUnavailable,
    /// Original source does not support native custody.
    #[error("source does not support original native work custody")]
    NativeWorkUnavailable,
    /// Original mounted source cause, moved without formatting or boxing.
    #[cfg(feature = "native-mount")]
    #[error(transparent)]
    Mount(crate::native_mount::MountSourceError),
    /// Native metadata could not be represented.
    #[cfg(all(
        any(target_os = "linux", target_os = "macos", windows),
        any(feature = "native-mount", test)
    ))]
    #[error(transparent)]
    Metadata(crate::native_host::NativeMetadataError),
    /// Watcher state synchronization was poisoned.
    #[error("native watcher state is poisoned")]
    StatePoisoned,
    /// A baseline scan is already active.
    #[error("native watcher baseline scan is already active")]
    RescanAlreadyInProgress,
    /// No baseline scan is active.
    #[error("native watcher baseline scan is not active")]
    NoRescanInProgress,
    /// Polling while a baseline is being built is ambiguous.
    #[error("native watcher baseline scan is in progress")]
    RescanInProgress,
    /// Poll output must have a positive bound.
    #[error("native watcher poll limit must be non-zero")]
    ZeroPollLimit,
    /// Process-local cursor arithmetic overflowed.
    #[error("native watcher sequence is exhausted")]
    SequenceExhausted,
    /// Polling was cooperatively cancelled.
    #[error(transparent)]
    Cancelled(#[from] CancellationError),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

impl From<std::io::Error> for NativeWatchError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

fn demand_watch_error(error: crate::demand::DemandError) -> NativeWatchError {
    match error {
        crate::demand::DemandError::Observation(error) => *error,
        crate::demand::DemandError::Work(error) => NativeWatchError::Work(error),
        crate::demand::DemandError::Cancelled => NativeWatchError::Cancelled(CancellationError),
        crate::demand::DemandError::Io(error) => error.into(),
        crate::demand::DemandError::SourceWatchStartup(owner) => {
            NativeWatchError::SourceStartup(owner)
        }
        crate::demand::DemandError::StaleSource => NativeWatchError::StaleSource,
        crate::demand::DemandError::StaleVersion => NativeWatchError::StaleVersion,
        crate::demand::DemandError::StaleCursor => NativeWatchError::StaleCursor,
        crate::demand::DemandError::CursorBusy => NativeWatchError::CursorBusy,
        crate::demand::DemandError::SourceUnavailable => NativeWatchError::SourceUnavailable,
        crate::demand::DemandError::Absent => NativeWatchError::Absent,
        crate::demand::DemandError::NotRegularFile => NativeWatchError::NotRegularFile,
        crate::demand::DemandError::NotDirectory => NativeWatchError::NotDirectory,
        crate::demand::DemandError::InvalidRequest => NativeWatchError::InvalidRequest,
        crate::demand::DemandError::WorkerUnavailable => NativeWatchError::WorkerUnavailable,
        crate::demand::DemandError::OriginalWorkPoisoned => NativeWatchError::StatePoisoned,
        crate::demand::DemandError::Allocation(error) => NativeWatchError::NativeWork(
            crate::NativeTreeMaterializationError::AllocationFailed(error),
        ),
        crate::demand::DemandError::NativeWorkUnavailable => {
            NativeWatchError::NativeWorkUnavailable
        }
        #[cfg(feature = "native-mount")]
        crate::demand::DemandError::Mount(error) => NativeWatchError::Mount(*error),
        #[cfg(all(
            any(target_os = "linux", target_os = "macos", windows),
            any(feature = "native-mount", test)
        ))]
        crate::demand::DemandError::NativeMetadata(error) => NativeWatchError::Metadata(error),
    }
}
