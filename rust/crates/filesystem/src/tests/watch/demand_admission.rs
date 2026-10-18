use super::*;
use crate::demand::{DemandDirectoryObserver, DemandError};
#[cfg(windows)]
use std::time::Instant;

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
