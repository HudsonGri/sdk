use super::*;
use proptest::prelude::*;

proptest! {
    // Every backend/control cause remains recoverable beneath an Interrupted
    // classification, so ownership loss follows the control-stop path.
    #[test]
    fn control_failure_preserves_its_concrete_cause(code in 1i32..4096) {
        let failure = control_error(std::io::Error::from_raw_os_error(code));
        prop_assert_eq!(failure.kind(), std::io::ErrorKind::Interrupted);
        prop_assert_eq!(failure.get_ref()
            .and_then(|cause| cause.downcast_ref::<std::io::Error>())
            .and_then(std::io::Error::raw_os_error), Some(code));
    }
}

#[cfg(all(feature = "test-support", unix))]
mod simulation {
    use super::*;
    use acyclic_native_runtime::testing::{Schedule, fill_seeded, unix_millis};
    use std::{future::Future, pin::Pin, sync::OnceLock};

    #[derive(Debug, PartialEq, Eq)]
    struct Observed {
        source: (bool, [u8; 16], Option<u64>),
        trace: Vec<String>,
        polls: Vec<(u64, &'static str)>,
        stdout: Vec<u8>,
    }

    fn capture_probe(entropy: bool) -> Result<Observed> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|cause| Error::Storage(cause.to_string()))?;
        runtime.block_on(async {
            tokio::time::pause();
            let (schedule, mut boundaries) = if entropy {
                Schedule::seeded(71)
            } else {
                Schedule::without_entropy(71)
            };
            let source = Arc::new(OnceLock::new());
            let worker_source = Arc::clone(&source);
            let invocation = Invocation {
                executable: "/bin/sh".into(),
                argv: vec!["-c".into(), "printf dst".into()],
                cwd: "/".into(),
                environment: Default::default(),
                inherit_environment: false,
                timeout_ms: 1000,
                control_timeout_ms: 1000,
                cancellation_poll_ms: 25,
                maximum_output_bytes: 3,
                mcp_stdio: None,
                standard_input: None,
            };
            let handle = tokio::runtime::Handle::current();
            let operation = capture_with_custody(
                invocation,
                |custody: &mut Vec<u8>, chunk| {
                    if let acyclic_native_runtime::ProcessOutput::Stdout(bytes) = chunk {
                        custody.extend_from_slice(bytes);
                    }
                    Ok(())
                },
                move || {
                    if worker_source.get().is_none() {
                        let mut bytes = [0; 16];
                        let seeded = fill_seeded(&mut bytes);
                        worker_source.set((seeded, bytes, unix_millis())).unwrap();
                        // Exercise a real Pending ownership check. A worker
                        // retaining its permit here deadlocks the seeded driver.
                        wait_control(&handle, tokio::time::sleep(Duration::from_millis(3)));
                    }
                    Ok(())
                },
                Vec::new(),
            );
            type Capture<'a> = Pin<
                Box<dyn Future<Output = Result<(Option<NativeProcessResult>, Vec<u8>)>> + 'a>,
            >;
            let operation: Capture<'_> = Box::pin(operation);
            let mut operation = Box::pin(schedule.scope(operation));
            let (output, custody) = loop {
                tokio::select! {
                    biased;
                    boundary = boundaries.recv() => boundary.expect("lost process boundary").release(),
                    output = &mut operation => break output?,
                }
            };
            schedule.drained().await;
            let output = output.expect("native capture was interrupted");
            assert!(output.success, "{:?}", output.stop);
            assert_eq!(output.exit_code, Some(0));
            assert!(output.stderr.is_empty());
            assert_eq!(output.stdout, b"dst");
            assert_eq!(custody, output.stdout, "observer lost worker custody");
            Ok(Observed {
                source: *source.get().expect("ownership was never checked"),
                trace: schedule.trace(),
                polls: schedule.polls(),
                stdout: output.stdout,
            })
        })
    }

    fn retained_sources(observed: &Observed) -> bool {
        observed.source.0
            && observed.source.2.is_some()
            && observed
                .trace
                .iter()
                .any(|event| event == "blocking:dispatch")
    }

    #[test]
    fn capture_retains_seeded_worker_sources_and_custody() -> Result<()> {
        let first = capture_probe(true)?;
        let second = capture_probe(true)?;
        assert!(retained_sources(&first));
        assert_eq!(first, second, "real process handoff did not replay");
        // Erase only the worker's entropy source. Keep its seeded executor,
        // clock and physical terminal, including the same Pending control check.
        assert!(
            !retained_sources(&capture_probe(false)?),
            "source-erased process handoff escaped the same oracle"
        );
        Ok(())
    }
}
