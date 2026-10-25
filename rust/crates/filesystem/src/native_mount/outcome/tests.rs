use super::*;

fn check_callback(error: &NativeMountError, expected: NativeFailure) {
    assert_eq!(callback_failure(error), expected, "{error:?}");
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert_eq!(crate::native_mount::callback_errno(error), expected.errno());
    #[cfg(target_os = "linux")]
    assert_eq!(
        crate::native_mount::fuse::callback_errno(error),
        expected.errno()
    );
    #[cfg(target_os = "macos")]
    assert_eq!(
        crate::native_mount::darwin_mount::callback_errno(error),
        expected.errno()
    );
    #[cfg(windows)]
    assert_eq!(
        crate::native_mount::projfs::native_hresult(error),
        windows::core::HRESULT::from_win32(match expected {
            NativeFailure::Cancelled => {
                windows::Win32::Foundation::ERROR_OPERATION_ABORTED.0
            }
            NativeFailure::Stale => 1006,
            NativeFailure::NotFound => 2,
            NativeFailure::AccessDenied => 5,
            NativeFailure::Os(code) => u32::from_ne_bytes(code.to_ne_bytes()),
            _ => panic!("use an explicit Windows status for this contract"),
        })
    );
}

fn cancelled() -> crate::CancellationError {
    let token = crate::CancellationToken::new();
    token.cancel();
    token.check().expect_err("the real token was cancelled")
}

#[test]
fn cancellation_is_not_staleness_in_any_error_transport() {
    let sources = [
        MountSourceError::Cancelled,
        MountSourceError::Filesystem(crate::FsError::Cancelled(cancelled())),
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled),
        MountSourceError::WorkspaceCause(crate::WorkspaceError::Cancelled(cancelled())),
        MountSourceError::WorkspaceCause(crate::WorkspaceError::Filesystem(
            crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Cancelled),
        )),
    ];
    for source in sources {
        assert_eq!(source_failure(&source), NativeFailure::Cancelled);
        check_callback(&NativeMountError::from(source), NativeFailure::Cancelled);
    }
    check_callback(
        &NativeMountError::Demand(crate::DemandError::Cancelled),
        NativeFailure::Cancelled,
    );
    check_callback(
        &NativeMountError::Demand(crate::DemandError::Mount(Box::new(
            MountSourceError::Cancelled,
        ))),
        NativeFailure::Cancelled,
    );
    check_callback(
        &NativeMountError::Source(MountSourceError::MutationFinalization {
            primary: Box::new(Some(MountSourceError::Cancelled)),
            cleanup: Box::new(None),
            custody: None,
        }),
        NativeFailure::Cancelled,
    );
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert_ne!(
        NativeFailure::Cancelled.errno(),
        NativeFailure::Stale.errno()
    );
}

#[test]
fn stale_identity_remains_distinct_from_cancellation() {
    for source in [
        MountSourceError::Stale,
        MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale),
        MountSourceError::WorkspaceCause(crate::WorkspaceError::StaleIdentity),
        MountSourceError::WorkspaceCause(crate::WorkspaceError::Filesystem(
            crate::FsError::SourceSnapshotStale,
        )),
    ] {
        check_callback(&NativeMountError::Source(source), NativeFailure::Stale);
    }
}

#[test]
fn workspace_causes_keep_their_semantics_through_source_and_demand() {
    for (workspace, expected) in [
        (
            crate::WorkspaceError::AccessDenied,
            NativeFailure::AccessDenied,
        ),
        (crate::WorkspaceError::NotFound, NativeFailure::NotFound),
        (
            crate::WorkspaceError::Filesystem(crate::FsError::NotFound),
            NativeFailure::NotFound,
        ),
    ] {
        let source = MountSourceError::WorkspaceCause(workspace);
        assert_eq!(source_failure(&source), expected);
        check_callback(
            &NativeMountError::Demand(crate::DemandError::Mount(Box::new(source))),
            expected,
        );
    }
}

#[test]
fn real_host_error_keeps_its_native_code_through_every_wrapper()
-> Result<(), Box<dyn std::error::Error>> {
    // This fixture is captured from an actual host open, never a hand-written
    // errno/HRESULT. Each transport owns the original error returned by open.
    let root = tempfile::tempdir()?;
    let absent = root.path().join("not-created");
    let mut native_code = None;
    for transport in 0..4 {
        let error = std::fs::File::open(&absent).expect_err("fixture path is absent");
        let code = error.raw_os_error().ok_or("host open has no native code")?;
        assert_eq!(*native_code.get_or_insert(code), code);
        let error = match transport {
            0 => NativeMountError::Io(error),
            1 => NativeMountError::Source(MountSourceError::Io(error, None)),
            2 => NativeMountError::Demand(crate::DemandError::Mount(Box::new(
                MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Io(error)),
            ))),
            _ => NativeMountError::Source(MountSourceError::WorkspaceCause(
                crate::WorkspaceError::Filesystem(crate::FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::Io(error),
                )),
            )),
        };
        check_callback(&error, NativeFailure::Os(code));
    }
    Ok(())
}
