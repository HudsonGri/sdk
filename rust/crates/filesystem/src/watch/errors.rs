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
        error @ crate::demand::DemandError::NativeMountAdmissionMissing => {
            NativeWatchError::Io(std::io::Error::other(error))
        }
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

#[cfg(all(test, any(windows, target_os = "macos")))]
mod demand_admission_tests {
    use super::*;
    use crate::demand::{DemandDirectoryObserver, DemandError};

    fn namespace(path: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
        Ok(NamespacePath::from_portable(
            &crate::path::PortablePath::parse(path, VolumeLimits::default())?,
            VolumeLimits::default(),
        )?)
    }

    fn observed(
        watch: &Mutex<NativeWatch>,
        directory: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::demand::DemandResult<()> {
        use crate::facade::FsWork;
        let mut accounting = budget.accounting(cancellation);
        let result = watch.observe_directory(directory, &mut accounting);
        budget.finish(result, &accounting)
    }

    fn watcher(root: &Path) -> Result<Mutex<NativeWatch>, NativeWatchError> {
        let mut options = NativeWatchOptions::new(VolumeLimits::default());
        options.recursive = false;
        Ok(Mutex::new(NativeWatch::open_with_profile(
            root,
            FilesystemProfile::Portable,
            options,
        )?))
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn nonrecursive_macos_observation_reports_unavailable_without_opaque_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let failure = observed(
            &watch,
            &namespace("/a")?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .expect_err("opaque nonrecursive admission is not bounded");
        assert!(matches!(failure.error,
            DemandError::Observation(error) if matches!(error.as_ref(), NativeWatchError::BoundedObservationUnavailable)));
        assert_eq!(*failure.work, WorkCounters::default());
        Ok(())
    }

    #[cfg(windows)]
    fn windows_budget() -> WorkBudget {
        WorkBudget {
            backend_read_operations: 64,
            backend_write_operations: 16,
            allocation_operations: 64,
            peak_allocation_bytes: 512 * 1024,
            bytes_copied: 1024 * 1024,
            bytes_encoded: 1024 * 1024,
            items_examined: 512,
            source_path_components: 512,
            ..WorkBudget::default()
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_later_prefix_denial_retains_the_admitted_ancestor_and_actual_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = watcher(root.path())?;
        let failure = observed(
            &watch,
            &namespace("/a/b")?,
            WorkBudget {
                backend_read_operations: 7,
                ..windows_budget()
            },
            &CancellationToken::new(),
        )
        .expect_err("the later prefix must stop at the native limit");
        assert!(matches!(
            failure.error,
            DemandError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations",
                ..
            })
        ));
        observed(
            &watch,
            &namespace("/a/b")?,
            windows_budget(),
            &CancellationToken::new(),
        )?;
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_native_child_notifications_reach_the_existing_hint_queue_and_rescan_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        std::fs::write(root.path().join("a/f"), b"before")?;
        let watch = watcher(root.path())?;
        watch
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .accept_lazy_baseline()?;
        observed(
            &watch,
            &namespace("/a")?,
            windows_budget(),
            &CancellationToken::new(),
        )?;
        std::fs::rename(root.path().join("a"), root.path().join("old"))?;
        std::fs::create_dir(root.path().join("a"))?;
        std::fs::write(root.path().join("a/f"), b"before")?;
        observed(
            &watch,
            &namespace("/a")?,
            windows_budget(),
            &CancellationToken::new(),
        )?;
        std::fs::write(root.path().join("a/f"), b"after")?;
        let expected = namespace("/a/f")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let receipt = watch
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .poll(16, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
            if matches!(receipt.value, WatchBatch::Changes { ref changes, .. }
                if changes.iter().any(|change| matches!(change,
                    WatchChange::Modified(path) | WatchChange::MetadataChanged(path) if path == &expected)))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the native child hint was not delivered"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut held = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        held.begin_rescan()?;
        assert!(held.windows_directories.is_empty());
        Ok(())
    }
}
