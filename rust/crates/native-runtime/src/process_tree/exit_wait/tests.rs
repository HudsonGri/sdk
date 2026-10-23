use super::*;
use std::sync::atomic::Ordering;

#[test]
fn exit_watch_prepays_and_records_its_descriptor_release() -> io::Result<()> {
    let mut tree = child("0")?;
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    let watch = open(tree.owned_child_id()?, Some(&meter))?;
    assert_eq!(meter.write_attempts.load(Ordering::Acquire), 0);
    drop(watch);
    assert_eq!(
        meter.write_attempts.load(Ordering::Acquire),
        1,
        "the exit descriptor close must stay on its original ledger"
    );
    #[cfg(target_os = "linux")]
    assert_eq!(meter.read_attempts.load(Ordering::Acquire), 1);
    #[cfg(target_vendor = "apple")]
    assert_eq!(meter.read_attempts.load(Ordering::Acquire), 2);

    let refused = crate::tests::ReadBudgetMeter::new(1, u64::MAX);
    assert!(
        open(tree.owned_child_id()?, Some(&refused)).is_err(),
        "an exit watch must reserve its close before opening"
    );
    assert_eq!(refused.used_operations.load(Ordering::Acquire), 0);
    assert_eq!(refused.read_attempts.load(Ordering::Acquire), 0);
    tree.terminate(None)?;
    assert!(tree.is_reaped());
    Ok(())
}

#[test]
fn exit_wait_fixture() {
    if std::env::var_os("ACYCLIC_TEST_EXIT_WAIT").is_some() {
        std::thread::sleep(Duration::from_millis(200));
        match std::env::var("ACYCLIC_TEST_EXIT_STATUS").as_deref() {
            Ok("37") => std::process::exit(37),
            Ok("signal") => {
                // SAFETY: the isolated fixture deliberately terminates itself.
                unsafe { libc::raise(libc::SIGTERM) };
            }
            _ => {}
        }
    }
}

fn child(status: &str) -> io::Result<ProcessTree> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::exit_wait::tests::exit_wait_fixture",
        ])
        .env("ACYCLIC_TEST_EXIT_WAIT", "1")
        .env("ACYCLIC_TEST_EXIT_STATUS", status)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    ProcessTree::spawn(&mut command)
}

#[test]
fn quiet_wait_observes_exit_without_periodic_wakeups_or_reaping() -> io::Result<()> {
    let mut tree = child("0")?;
    OBSERVATIONS.with(|count| count.set(0));
    let started = Instant::now();
    let status = tree.wait(Duration::from_secs(2))?;
    let observations = OBSERVATIONS.with(std::cell::Cell::get);
    eprintln!(
        "EXIT_WAIT observations={observations} elapsed_us={}",
        started.elapsed().as_micros()
    );
    assert!(status.success());
    assert!(
        observations <= 3,
        "quiet wait must use an exit notification"
    );
    assert!(
        !tree.is_reaped(),
        "exit observation must retain tree custody"
    );
    assert_eq!(tree.wait(Duration::ZERO)?, status);
    tree.terminate(None)?;
    assert!(tree.is_reaped());
    Ok(())
}

#[test]
fn exit_deadline_preserves_the_same_live_child_for_a_later_wait() -> io::Result<()> {
    let mut tree = child("0")?;
    assert_eq!(
        tree.wait(Duration::from_millis(20))
            .expect_err("child must remain live")
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert!(!tree.is_reaped());
    assert!(tree.wait(Duration::from_secs(2))?.success());
    tree.terminate(None)?;
    Ok(())
}

#[test]
fn exit_notifications_retain_nonzero_and_signal_status() -> io::Result<()> {
    use std::os::unix::process::ExitStatusExt;
    for kind in ["37", "signal"] {
        let mut tree = child(kind)?;
        let status = tree.wait(Duration::from_secs(2))?;
        if kind == "37" {
            assert_eq!(status.code(), Some(37));
        } else {
            assert_eq!(status.signal(), Some(libc::SIGTERM));
        }
        assert_eq!(tree.try_wait(None)?, Some(status));
        tree.terminate(None)?;
    }
    Ok(())
}
