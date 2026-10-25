use super::*;

fn fixture() -> Result<
    (
        tempfile::TempDir,
        NativeMountSession,
        NativeMaterializationWork,
        PathBuf,
    ),
    Box<dyn std::error::Error>,
> {
    registry::initialize()?;
    let root = tempfile::tempdir()?;
    let destination = root.path().join("retired-mount");
    std::fs::create_dir(&destination)?;
    std::fs::write(root.path().join("authored"), b"keep")?;
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .map_err(|(cause, _)| cause)?;
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)?;
    let held = guard.as_mut().ok_or("destination fence absent")?;
    let fence = held.lock_path.clone();
    assert!(fence.is_file(), "fixture has no physical destination fence");
    held.prepare_destination_cleanup(&destination, &work)?;
    let mut session = NativeMountSession::new(
        NativeMountRequest {
            mount_id: MountId::new(),
            volume_id: VolumeId::new(),
            destination,
            writable: false,
        },
        None,
        None,
    );
    session.destination_guard = guard;
    session.namespace_detach_confirmed = true;
    Ok((root, session, work, fence))
}

fn retired(
    root: &Path,
    fence: &Path,
    before: crate::WorkCounters,
    work: &NativeMaterializationWork,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(!root.join("retired-mount").exists());
    assert!(!fence.exists(), "retired namespace kept its physical fence");
    assert_eq!(std::fs::read(root.join("authored"))?, b"keep");
    assert_eq!(
        work.receipt().backend_write_operations,
        before.backend_write_operations + 4,
        "directory removal, registry lock, fence unlink and unlock use original credit"
    );
    Ok(())
}

#[test]
fn detached_driver_retirement_completes_fence_after_waiter_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, mut session, work, fence) = fixture()?;
    let mut packet = NativeMountRetirement {
        retained_source: None,
        startup_source_watch: None,
        destination_guard: session.destination_guard.take(),
        #[cfg(windows)]
        warm_namespace: None,
    };
    let before = work.receipt();
    work.cancellation().cancel();
    let wait = CancellationToken::new();
    wait.cancel();
    let result = packet.retire(Some(&wait));
    assert!(
        matches!(result, Ok(true)),
        "detached driver did not finish accepted fence release: {result:?}"
    );
    assert!(packet.is_retired());
    retired(root.path(), &fence, before, &work)?;
    let completed = work.receipt();
    assert!(!packet.retire(Some(&wait))?);
    assert_eq!(work.receipt(), completed);
    Ok(())
}

#[test]
fn detached_session_retirement_completes_fence_after_waiter_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    for mode in 0..3 {
        let (root, mut session, work, fence) = fixture()?;
        let before = work.receipt();
        work.cancellation().cancel();
        let wait = CancellationToken::new();
        wait.cancel();
        let stop = match mode {
            0 => NativeSessionStopWork::Meter(&work),
            1 => NativeSessionStopWork::AcceptedWait(&wait),
            _ => NativeSessionStopWork::AcceptedDrop,
        };
        assert!(session.stop_with_context(&stop)?);
        assert!(session.cleanup_state().cleanup_complete());
        retired(root.path(), &fence, before, &work)?;
        let completed = work.receipt();
        assert!(!session.stop_with_context(&stop)?);
        assert_eq!(work.receipt(), completed);
    }
    Ok(())
}
