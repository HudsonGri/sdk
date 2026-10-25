use super::*;

fn convert(error: DemandError, lazy: bool) -> MountSourceError {
    if lazy {
        super::super::metadata::lazy_error(LazyWorkspaceError::Demand(error))
    } else {
        error.into()
    }
}

#[test]
fn demand_causes_keep_their_type_without_a_diagnostic_allocation() {
    use std::error::Error;
    for lazy in [false, true] {
        for cause in [
            DemandError::NativeWorkUnavailable,
            DemandError::OriginalWorkPoisoned,
            DemandError::WorkerUnavailable,
            DemandError::SourceUnavailable,
            DemandError::CursorBusy,
            DemandError::NotRegularFile,
            DemandError::InvalidRequest,
        ] {
            let expected = std::mem::discriminant(&cause);
            let mut converted = None;
            let allocations = acyclic_test_support::measure(|| {
                converted = Some(convert(cause, lazy));
            });
            let converted = converted.expect("conversion completed");
            let retained = converted
                .source()
                .and_then(|error| error.downcast_ref::<DemandError>())
                .expect("original demand cause must remain typed");
            assert_eq!(std::mem::discriminant(retained), expected);
            assert_eq!(allocations.count_total, 0, "{converted:?}");
        }
    }
}

#[cfg(feature = "native-watch")]
#[test]
fn observation_causes_keep_their_original_box_without_allocating() {
    for lazy in [false, true] {
        let original = Box::new(crate::watch::NativeWatchError::BoundedObservationUnavailable);
        let identity = std::ptr::from_ref(original.as_ref());
        let mut converted = None;
        let allocations = acyclic_test_support::measure(|| {
            converted = Some(convert(DemandError::Observation(original), lazy));
        });
        let converted = converted.expect("conversion completed");
        let MountSourceError::Demand(DemandError::Observation(retained)) = &converted else {
            panic!("original observation cause must remain typed: {converted:?}");
        };
        assert_eq!(std::ptr::from_ref(retained.as_ref()), identity);
        assert_eq!(allocations.count_total, 0);
    }
}

#[test]
fn lazy_and_direct_demand_conversion_use_the_same_callback_semantics() {
    use crate::native_mount::outcome::{NativeFailure, source_failure};
    for lazy in [false, true] {
        for (cause, expected) in [
            (DemandError::Cancelled, NativeFailure::Cancelled),
            (DemandError::StaleSource, NativeFailure::Stale),
            (DemandError::StaleVersion, NativeFailure::Stale),
            (DemandError::StaleCursor, NativeFailure::Stale),
            (DemandError::Absent, NativeFailure::NotFound),
            (DemandError::NotDirectory, NativeFailure::NotDirectory),
            (
                DemandError::Work(crate::WorkError::Overflow),
                NativeFailure::Io,
            ),
            (
                DemandError::Mount(Box::new(MountSourceError::AccessDenied)),
                NativeFailure::AccessDenied,
            ),
        ] {
            let converted = convert(cause, lazy);
            assert_eq!(source_failure(&converted), expected, "{converted:?}");
            let callback = NativeMountError::Source(converted);
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            assert_eq!(
                crate::native_mount::callback_errno(&callback),
                expected.errno()
            );
            #[cfg(target_os = "linux")]
            assert_eq!(
                crate::native_mount::fuse::callback_errno(&callback),
                expected.errno()
            );
            #[cfg(target_os = "macos")]
            assert_eq!(
                crate::native_mount::darwin_mount::callback_errno(&callback),
                expected.errno()
            );
            #[cfg(windows)]
            {
                let code = match expected {
                    NativeFailure::Cancelled => 995,
                    NativeFailure::Stale => 1006,
                    NativeFailure::NotFound => 2,
                    NativeFailure::NotDirectory => 267,
                    NativeFailure::AccessDenied => 5,
                    NativeFailure::Io => 1117,
                    _ => unreachable!("fixture uses explicit Windows statuses"),
                };
                assert_eq!(
                    crate::native_mount::projfs::native_hresult(&callback),
                    windows::core::HRESULT::from_win32(code)
                );
            }
        }
    }
}

#[test]
fn real_demand_io_keeps_its_host_code_through_both_conversions()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let absent = root.path().join("not-created");
    for lazy in [false, true] {
        // Capture the actual host failure; no fabricated OS-tool fixture.
        let error = std::fs::File::open(&absent).expect_err("fixture path is absent");
        let code = error.raw_os_error().ok_or("host open has no native code")?;
        let converted = convert(DemandError::Io(error), lazy);
        assert_eq!(
            crate::native_mount::outcome::source_failure(&converted),
            crate::native_mount::outcome::NativeFailure::Os(code)
        );
        assert!(
            matches!(converted, MountSourceError::Io(error, None) if error.raw_os_error() == Some(code))
        );
    }
    Ok(())
}
