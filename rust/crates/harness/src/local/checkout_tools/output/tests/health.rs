use super::*;
use acyclic_native_runtime::{ProcessInput, ProcessTree, ProcessWake};
use std::process::{Command, Stdio};

#[test]
fn health_child() {
    if crate::native_environment::variable("ACYCLIC_OUTPUT_HEALTH_CHILD").is_some() {
        use std::io::Write;
        println!("OUTPUT_HEALTH_READY");
        std::io::stdout().flush().expect("fixture stdout");
        std::thread::sleep(Duration::from_secs(2));
    }
}

#[test]
fn output_failure_wakes_a_silent_capture_and_reaps_it() -> std::io::Result<()> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "local::checkout_tools::output::tests::health::health_child",
            "--nocapture",
        ])
        .env("ACYCLIC_OUTPUT_HEALTH_CHILD", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command)?;
    let health = Health::default();
    let wake = ProcessWake::new()?;
    health.wake_on_failure(wake.clone())?;
    let failure = health.clone();
    let (ready, announced) = std::sync::mpsc::sync_channel(1);
    let producer = std::thread::spawn(move || {
        announced
            .recv_timeout(Duration::from_secs(1))
            .map_err(std::io::Error::other)?;
        std::thread::sleep(Duration::from_millis(50));
        failure.fail("output publication was refused");
        Ok::<_, std::io::Error>(())
    });
    let mut ready = Some(ready);
    let mut stdout = Vec::new();
    let started = std::time::Instant::now();
    let result = tree.wait_with_output(
        Duration::from_secs(1),
        16 * 1024,
        None,
        || health.check(),
        |chunk| {
            if let ProcessOutput::Stdout(bytes) = chunk {
                stdout.extend_from_slice(bytes);
                if stdout
                    .windows(b"OUTPUT_HEALTH_READY".len())
                    .any(|bytes| bytes == b"OUTPUT_HEALTH_READY")
                    && let Some(ready) = ready.take()
                {
                    ready.send(()).map_err(std::io::Error::other)?;
                }
            }
            Ok(ProcessInput::default())
        },
        None,
        None,
        Some(std::slice::from_ref(&wake)),
    );
    producer
        .join()
        .map_err(|_| std::io::Error::other("output failure producer panicked"))??;
    let failure = result.expect_err("failed output publication must stop capture");
    assert!(
        failure
            .error
            .to_string()
            .contains("output publication was refused")
    );
    assert!(failure.cleanup_completed && tree.is_reaped());
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "output failure did not wake the silent native worker"
    );
    Ok(())
}
