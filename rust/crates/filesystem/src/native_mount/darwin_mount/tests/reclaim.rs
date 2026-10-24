use super::*;
use std::io::{BufRead as _, Write as _};
use std::process::Stdio;

const CHILD: &str = "native_mount::darwin_mount::tests::reclaim::crashed_owner";

fn request(
    destination: &Path,
    source: &MemorySource,
) -> Result<crate::NativeMountRequest, NativeMountError> {
    Ok(crate::NativeMountRequest {
        mount_id: crate::MountId::new(),
        volume_id: source.volume_id()?,
        destination: destination.to_path_buf(),
        writable: true,
    })
}

#[test]
#[ignore = "subprocess fixture for the live macOS crash recovery test"]
fn crashed_owner() -> TestResult {
    let Some(root) = std::env::var_os("ACYCLIC_RECLAIM_TEST_ROOT") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    let (source, _, work) = checkout_context(MountPublication::Manual)?;
    let _owned = crate::mount_native(request(&root.join("owned"), &source)?, source.clone())?;
    // Another user of the same NFS transport is not an SDK-owned destination.
    let (foreign, _, _) = checkout_context(MountPublication::Manual)?;
    let mut session = None;
    DarwinMountSession::start_into(
        &request(&root.join("foreign"), &foreign)?,
        foreign,
        &work,
        &mut session,
        &mut None,
        |_| Ok(()),
    )?;
    println!("RECLAIM_READY");
    std::io::stdout().flush()?;
    let mut command = String::new();
    std::io::stdin().read_line(&mut command)?;
    Err("crash fixture must be killed".into())
}

struct CrashRoots(Option<tempfile::TempDir>);

impl Drop for CrashRoots {
    fn drop(&mut self) {
        let Some(root) = self.0.take() else { return };
        let cleanup = (|| -> TestResult {
            let canonical = root.path().canonicalize()?;
            for mount in super::super::reclaim::mount_table()? {
                let destination = super::super::reclaim::field(&mount.f_mntonname)?.to_bytes();
                if ["owned", "foreign"]
                    .into_iter()
                    .any(|name| canonical.join(name).as_os_str().as_bytes() == destination)
                {
                    // The fixture owns both exact destinations, including its
                    // deliberately unfenced foreign transport.
                    super::super::reclaim::detach_stale_mount(&mount)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = cleanup {
            // Never recursively remove a directory while a dead NFS mount may
            // cover it. Preserve the fixture for recovery and fail the proof.
            let retained = root.keep();
            eprintln!("crash fixture retirement failed at {retained:?}: {error}");
            assert!(std::thread::panicking(), "crash fixture retirement failed");
        }
    }
}

#[test]
#[ignore = "requires live macOS NFS mounts and SIGKILL"]
fn startup_reclaims_crashed_sdk_mounts_and_preserves_live_and_foreign_mounts() -> TestResult {
    check_startup_recovery(false, crate::initialize_native_mount_services)
}

#[test]
#[ignore = "requires live macOS NFS mounts and SIGKILL"]
fn permission_denied_startup_preserves_live_and_foreign_mounts() -> TestResult {
    check_startup_recovery(true, crate::initialize_native_mount_services)
}

#[test]
#[ignore = "requires live macOS NFS mounts and SIGKILL"]
fn negative_control_direct_only_startup_cannot_retire_after_eperm() -> TestResult {
    let attempted = std::cell::Cell::new(false);
    let result = check_startup_recovery(true, || {
        attempted.set(true);
        super::super::reclaim::reclaim_stale_mounts_direct_only()
    });
    assert!(
        attempted.get(),
        "fixture failed before exercising retirement"
    );
    let error = result.expect_err("direct-only startup unexpectedly recovered after EPERM");
    assert!(matches!(
        error.downcast_ref::<NativeMountError>(),
        Some(NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::EPERM)
    ));
    println!("RECLAIM_DIRECT_ONLY_REJECTED errno=EPERM");
    Ok(())
}

fn check_startup_recovery(
    deny_direct_unmount: bool,
    startup: impl FnOnce() -> Result<(), NativeMountError>,
) -> TestResult {
    let root = CrashRoots(Some(tempfile::tempdir()?));
    let canonical = root
        .0
        .as_ref()
        .ok_or("missing crash fixture")?
        .path()
        .canonicalize()?;
    for name in ["owned", "foreign"] {
        std::fs::create_dir(canonical.join(name))?;
    }
    let (live_root, _live_source, mut live, _) = live_mount()?;
    let live_socket = mount_socket(live_root.path())?;
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .env("ACYCLIC_RECLAIM_TEST_ROOT", &canonical)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let output = child.stdout.take().ok_or("missing child stdout")?;
    let ready = std::io::BufReader::new(output)
        .lines()
        .find_map(|line| match line {
            Ok(line) if line == "RECLAIM_READY" => Some(Ok(())),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .ok_or("child exited before mounting")?;
    ready?;
    child.kill()?;
    child.wait()?;
    // Negative witness: SIGKILL itself leaves both kernel mounts behind.
    let mounted = |path: &Path| -> Result<bool, NativeMountError> {
        Ok(super::super::reclaim::mount_table()?.iter().any(|mount| {
            super::super::reclaim::field(&mount.f_mntonname)
                .is_ok_and(|name| name.to_bytes() == path.as_os_str().as_bytes())
        }))
    };
    assert!(mounted(&canonical.join("owned"))?);
    assert!(mounted(&canonical.join("foreign"))?);
    println!("RECLAIM_CRASH_WITNESS owned=attached foreign=attached");
    // A historical SDK fence alone must not authorize detaching a different
    // transport's mount subsequently placed at the same destination.
    let (foreign_fence, _) =
        crate::native_mount::fence::MountDestinationGuard::paths_for_canonical(
            &canonical.join("foreign"),
        )?;
    drop(crate::native_mount::fence::acquire_mount_lock(
        &foreign_fence,
        false,
    )?);
    if deny_direct_unmount {
        super::super::reclaim::with_direct_unmount_denied(startup)?;
    } else {
        startup()?;
    }
    assert!(
        !mounted(&canonical.join("owned"))?,
        "startup left the crashed SDK mount attached"
    );
    assert!(
        mounted(&canonical.join("foreign"))?,
        "startup reclaimed a foreign mount"
    );
    assert_eq!(mount_socket(live_root.path())?, live_socket);
    let (source, _, _) = checkout_context(MountPublication::Manual)?;
    let mut replacement = crate::mount_native(request(&canonical.join("owned"), &source)?, source)?;
    std::fs::write(canonical.join("owned/reconnected"), b"ready")?;
    assert_eq!(
        std::fs::read(canonical.join("owned/reconnected"))?,
        b"ready"
    );
    replacement.stop()?;
    live.stop()?;
    println!(
        "RECLAIM_RECOVERY_VERIFIED owned=retired live=preserved foreign=preserved replacement=writable"
    );
    Ok(())
}
