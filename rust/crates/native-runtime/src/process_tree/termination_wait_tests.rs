use super::super::*;
use std::process::Stdio;
use std::sync::atomic::Ordering;

#[test]
fn quiet_termination_wait_does_not_retry_observation_on_a_timer() -> io::Result<()> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::capture_events_tests::event_capture_child",
            "--nocapture",
        ])
        .env("ACYCLIC_TEST_EVENT_CAPTURE", "quiet")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut process = ProcessTree::spawn(&mut command)?;
    // This fixture creates no descendants. Let the leader exit naturally to
    // exercise the wait after containment has already stopped issuing kills.
    process.guard.active = false;
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    let started = Instant::now();
    process.terminate(Some(&meter))?;
    let reads = meter.read_attempts.load(Ordering::Acquire);
    eprintln!(
        "TERMINATION_WAIT reads={reads} elapsed_us={}",
        started.elapsed().as_micros()
    );
    assert!(process.is_reaped());
    assert!(
        reads <= 6,
        "termination must wait for child exit without periodic observation"
    );
    Ok(())
}
