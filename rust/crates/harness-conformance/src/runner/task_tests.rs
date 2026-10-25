use super::*;
use acyclic_native_runtime::testing::{CapturedClock, Schedule};

async fn scheduled<F: Future>(seed: u64, future: F) -> F::Output {
    let (schedule, mut boundaries) = Schedule::seeded(seed);
    schedule.set_unix_millis(100);
    let operation = schedule.scope(async {
        let output = future.await;
        schedule.drained().await;
        output
    });
    tokio::pin!(operation);
    loop {
        tokio::select! {
            biased;
            boundary = boundaries.recv() => boundary.expect("lost case schedule").release(),
            output = &mut operation => return output,
        }
    }
}

#[tokio::test]
async fn initial_and_replacement_cases_keep_the_original_native_context() -> TestResult {
    for seed in 0..4 {
        scheduled(seed, async {
            for concurrency in 1usize..=3 {
                let clocks = run_cases(
                    (0..7).map(|_| async { CapturedClock::capture().is_some() }),
                    concurrency.try_into()?,
                )
                .await?;
                assert_eq!(clocks, vec![true; 7], "case escaped its native context");
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await?;
    }
    Ok(())
}

struct Completion {
    finished: bool,
    sent: tokio::sync::mpsc::UnboundedSender<bool>,
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.sent.send(self.finished).expect("lost task owner");
    }
}

async fn cancellation_keeps_admission_bounded() -> TestResult {
    for concurrency in 1usize..=4 {
        let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
        let (finished, mut finishes) = tokio::sync::mpsc::unbounded_channel();
        let (release, wait) = tokio::sync::watch::channel(false);
        let mut constructed = 0;
        let mut operation = Box::pin(run_cases(
            (0..8).map(|_| {
                constructed += 1;
                let started = started.clone();
                let mut wait = wait.clone();
                let sent = finished.clone();
                async move {
                    let mut completion = Completion {
                        finished: false,
                        sent,
                    };
                    started.send(()).expect("lost admission owner");
                    wait.changed().await.expect("lost release owner");
                    completion.finished = true;
                }
            }),
            concurrency.try_into()?,
        ));
        assert!(futures::poll!(&mut operation).is_pending());
        for _ in 0..concurrency {
            starts.recv().await.ok_or("admitted case did not start")?;
        }
        drop(operation);
        // Let a detached task complete as a deterministic negative control:
        // cancellation must already have won before this release is published.
        release.send(true)?;
        for _ in 0..concurrency {
            assert_eq!(
                finishes.recv().await,
                Some(false),
                "case detached on owner drop"
            );
        }
        assert_eq!(constructed, concurrency);
        assert!(
            starts.try_recv().is_err(),
            "queued case was admitted after cancellation"
        );
    }
    Ok(())
}

#[tokio::test]
async fn dropping_the_runner_cancels_admitted_cases_without_starting_queued_cases() -> TestResult {
    cancellation_keeps_admission_bounded().await?;
    for seed in 0..4 {
        scheduled(seed, cancellation_keeps_admission_bounded()).await?;
    }
    Ok(())
}
