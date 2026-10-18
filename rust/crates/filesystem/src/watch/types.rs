use crate::NativeRootIdentity;
use crate::cancellation::{CancellationError, CancellationToken};
#[cfg(any(test, not(windows)))]
use crate::kernel::NamespacePathError;
use crate::kernel::{LogicalName, NamespacePath};
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use acyclic_native_runtime::deadline::{self, Deadline, Waiter};
#[cfg(not(windows))]
use notify::RecursiveMode;
#[cfg(not(windows))]
use notify::Watcher;
#[cfg(any(test, not(windows)))]
use notify::event::{ModifyKind, RenameMode};
#[cfg(any(test, not(windows)))]
use notify::{Event, EventKind};
use std::mem::size_of;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

/// The host's native event watcher.
#[cfg(target_os = "macos")]
pub type NativeEventWatcher = crate::fsevents::FsEventsWatcher;
/// The host's native event watcher.
#[cfg(not(target_os = "macos"))]
pub type NativeEventWatcher = notify::RecommendedWatcher;

/// Exact guarantees supplied by the compiled native watcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeWatchCapabilities {
    /// Host notification mechanism selected at compile time.
    pub backend: &'static str,
    /// Whether descendants are observed recursively.
    pub recursive: bool,
    /// Whether a persisted cursor can prove continuity across process restart.
    pub persistent_restart: bool,
    /// Whether every poll fences replacement of the admitted root identity.
    pub root_identity_fencing: bool,
}

/// Returns compile-time native watcher guarantees without touching a user path.
#[must_use]
pub const fn native_watch_capabilities() -> NativeWatchCapabilities {
    #[cfg(target_os = "linux")]
    let backend = "linux-inotify";
    #[cfg(target_os = "macos")]
    let backend = "macos-fsevents";
    #[cfg(target_os = "windows")]
    let backend = "windows-read-directory-changes";
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    let backend = "unsupported";

    NativeWatchCapabilities {
        backend,
        recursive: cfg!(any(target_os = "linux", target_os = "macos", windows)),
        persistent_restart: false,
        root_identity_fencing: cfg!(any(unix, windows)),
    }
}

/// One bounded host-side change hint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchChange {
    /// A namespace entry appeared.
    Created(NamespacePath),
    /// File content or an unspecified property may have changed.
    Modified(NamespacePath),
    /// A rename whose other end is unknown bound this name: what it names now, subtree and all, whatever was there before.
    Arrived(NamespacePath),
    /// Metadata or named attributes may have changed.
    MetadataChanged(NamespacePath),
    /// A namespace entry disappeared.
    Removed(NamespacePath),
    /// One paired rename with exact old and new relative paths.
    Renamed {
        /// Old relative path.
        from: NamespacePath,
        /// New relative path.
        to: NamespacePath,
    },
}

/// Why watcher hints can no longer prove a contiguous change interval.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WatchInvalidationReason {
    /// Every newly opened watcher starts without an authenticated baseline.
    #[error("an initial authenticated baseline is required")]
    InitialSnapshotRequired,
    /// The bounded notification queue overflowed.
    #[error("the native notification queue overflowed")]
    QueueOverflow,
    /// The native backend reported lost or unreliable events.
    #[error("the native backend requires a rescan")]
    NativeRescanRequired,
    /// The native backend returned an asynchronous error.
    #[error("the native backend reported an asynchronous error")]
    BackendError,
    /// A demanded namespace path cannot be represented exactly on this host.
    #[error("a native path cannot be represented by volume semantics")]
    UnrepresentablePath,
    /// A rename could not be paired without guessing.
    #[error("a native rename could not be paired exactly")]
    AmbiguousRename,
    /// A separately opened capture root does not match the watched directory.
    #[error("the watched root changed identity")]
    RootChanged,
}

/// One bounded watcher poll result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchBatch {
    /// A contiguous process-local interval of change hints.
    Changes {
        /// Invalidated watcher epoch.
        epoch: u64,
        /// Sequence assigned to the first returned change.
        first_sequence: u64,
        /// Sequence to request after applying every returned change.
        next_sequence: u64,
        /// Bounded hints moved from the native queue without path copies.
        changes: Vec<WatchChange>,
        /// Original queued-payload custody; keep it until the moved hints die.
        original: WatchBatchOriginal,
    },
    /// All hints from this epoch must be discarded and a baseline rescanned.
    RescanRequired {
        /// Invalidated watcher epoch.
        epoch: u64,
        /// Exact invalidation class.
        reason: WatchInvalidationReason,
    },
}

/// Physical allocation custody moved with a native hint interval.
#[derive(Debug, Default)]
pub struct WatchBatchOriginal {
    allocations: Vec<[Option<crate::NativeAllocation>; 2]>,
    // A zero-byte split of the actual externally returned change-vector guard.
    storage: Option<crate::NativeAllocation>,
}

impl Clone for WatchBatchOriginal {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl PartialEq for WatchBatchOriginal {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for WatchBatchOriginal {}

impl WatchBatchOriginal {
    pub(crate) fn change_has_native_payload_custody(&self, index: usize) -> bool {
        self.allocations
            .get(index)
            .is_some_and(|allocations| allocations.iter().any(Option::is_some))
    }

    pub(crate) fn has_native_changes_storage_custody(&self) -> bool {
        self.storage.is_some()
    }

    fn reserve<W: crate::demand::SourceDemandReadWork>(
        capacity: usize,
        retain_guards: bool,
        work: &mut W,
    ) -> Result<Self, NativeWatchError> {
        if !retain_guards {
            return Ok(Self::default());
        }
        let mut allocations = Vec::new();
        if capacity != 0 {
            // Guard headers are private metadata, not newly owned hint data.
            work.started_io(WorkCounters {
                allocation_operations: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(demand_watch_error)?;
            allocations
                .try_reserve_exact(capacity)
                .map_err(|_| NativeWatchError::Io(std::io::ErrorKind::OutOfMemory.into()))?;
        }
        Ok(Self {
            allocations,
            storage: None,
        })
    }
}

/// Bounded native watcher configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeWatchOptions {
    /// Volume path/name bounds used while converting host paths.
    pub limits: VolumeLimits,
    /// Maximum queued native changes before typed invalidation.
    pub maximum_queued_changes: u32,
    /// Whether descendants are observed recursively.
    pub recursive: bool,
}

#[cfg(target_os = "macos")]
fn open_native_root(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW);
    options.open(path)
}

const fn native_filesystem_profile() -> FilesystemProfile {
    #[cfg(unix)]
    {
        FilesystemProfile::Posix
    }
    #[cfg(windows)]
    {
        FilesystemProfile::Windows
    }
    #[cfg(not(any(unix, windows)))]
    {
        FilesystemProfile::Portable
    }
}

impl NativeWatchOptions {
    /// Conservative recursive defaults.
    #[must_use]
    pub const fn new(limits: VolumeLimits) -> Self {
        Self {
            limits,
            maximum_queued_changes: 16_384,
            recursive: true,
        }
    }

    fn validate(self) -> Result<Self, NativeWatchError> {
        if self.maximum_queued_changes == 0 {
            return Err(NativeWatchError::InvalidOptions);
        }
        Ok(self)
    }
}

#[derive(Debug)]
struct SharedState {
    invalidation: Option<WatchInvalidationReason>,
    #[cfg(any(target_os = "linux", windows))]
    pending_rename: Option<PendingRename>,
    /// Host path of the outstanding fence cookie, cleared on delivery.
    fence: Option<PathBuf>,
    /// Host subtrees whose events are never hints, in fence preference order.
    excluded: Vec<PathBuf>,
    excluded_allocation: Option<crate::NativeAllocation>,
    _queue_allocation: Option<crate::NativeAllocation>,
}

#[cfg(any(target_os = "linux", windows))]
#[derive(Debug)]
struct PendingRename {
    tracker: usize,
    from: NamespacePath,
    #[cfg(target_os = "linux")]
    to: Option<NamespacePath>,
    allocation: Option<crate::NativeAllocation>,
}

struct QueuedWatchChange {
    change: WatchChange,
    allocations: [Option<crate::NativeAllocation>; 2],
}

impl SharedState {
    const fn new(invalidation: Option<WatchInvalidationReason>) -> Self {
        Self {
            invalidation,
            #[cfg(any(target_os = "linux", windows))]
            pending_rename: None,
            fence: None,
            excluded: Vec::new(),
            excluded_allocation: None,
            _queue_allocation: None,
        }
    }

    fn seal_invalidation(&mut self) -> Option<WatchInvalidationReason> {
        #[cfg(any(target_os = "linux", windows))]
        if self.invalidation.is_none() && self.pending_rename.is_some() {
            self.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
        }
        self.invalidation
    }
}

struct NativeEventContext {
    root: PathBuf,
    #[cfg(any(test, not(windows)))]
    root_identity: NativeRootIdentity,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    #[cfg(any(target_os = "linux", windows))]
    maximum_queued_changes: u32,
    _root_allocation: Option<crate::NativeAllocation>,
}

enum NativeWatchDriver {
    #[cfg(not(windows))]
    Adapter(NativeEventWatcher),
    Source(crate::source_watch::NativeSourceWatch),
    StartupFailure(Arc<crate::source_watch::SourceWatchStartupFailure>),
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl crate::source_watch::HostChangeSink for Mutex<SharedState> {
    fn logical_owned_bytes(&self) -> u64 {
        0
    }
    fn host_changed(
        &self,
        changes: &[crate::source_watch::HostChange],
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        invalidate_root_hints(self, changes, work)
    }

    fn host_changed_with_native_work(
        &self,
        changes: &[crate::source_watch::HostChange],
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), crate::demand::DemandError> {
        invalidate_root_hints(self, changes, work)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn invalidate_root_hints<W: crate::demand::SourceDemandReadWork>(
    state: &Mutex<SharedState>,
    changes: &[crate::source_watch::HostChange],
    work: &mut W,
) -> Result<(), crate::demand::DemandError> {
    work.check()?;
    for change in changes {
        work.started_io(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        if cfg!(target_os = "macos")
            || matches!(change, crate::source_watch::HostChange::Everything)
        {
            state
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        }
    }
    Ok(())
}
