use super::{BLOCKING_WORKERS, MAX_ASYNC_WORKERS, TestResult, scenario_builder, scenario_workers};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

#[test]
fn blocking_pool_queues_work_instead_of_creating_another_thread() -> TestResult {
    for broken in [false, true] {
        let created = Arc::new(AtomicUsize::new(0));
        let names = created.clone();
        let mut builder = scenario_builder(MAX_ASYNC_WORKERS);
        if broken {
            builder.max_blocking_threads(BLOCKING_WORKERS + 1);
        }
        // Tokio calls this synchronously when requesting a thread. No sampling
        // or scheduling delay can hide creation of a worker beyond the bound.
        let runtime = builder
            .thread_name_fn(move || {
                names.fetch_add(1, Ordering::SeqCst);
                "e2e-budget".into()
            })
            .build()?;
        let mut jobs = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..BLOCKING_WORKERS {
            let (entered, waiting) = mpsc::sync_channel(0);
            let (release, gate) = mpsc::channel();
            jobs.push(runtime.spawn_blocking(move || -> TestResult {
                entered.send(())?;
                gate.recv()?;
                Ok(())
            }));
            releases.push(release);
            waiting.recv()?;
        }
        let queued = runtime.spawn_blocking(|| 42);
        let threads = created.load(Ordering::SeqCst);
        // Release before asserting so a failure cannot strand the pool on drop.
        for release in releases {
            release.send(())?;
        }
        let answer = runtime.block_on(async {
            for job in jobs {
                job.await??;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(queued.await?)
        })?;
        assert_eq!(answer, 42, "queued work was discarded");
        let bound = MAX_ASYNC_WORKERS + BLOCKING_WORKERS;
        assert_eq!(threads, bound + usize::from(broken));
        assert_eq!(threads <= bound, !broken, "blocking-pool control escaped");
    }
    Ok(())
}

fn admission(count: usize, workers: usize, unbounded: bool) -> TestResult {
    use futures::{FutureExt as _, StreamExt as _};
    use std::{cell::Cell, rc::Rc};
    struct Running(Rc<Cell<usize>>);
    impl Drop for Running {
        fn drop(&mut self) {
            self.0.set(self.0.get() - 1);
        }
    }
    let live = Rc::new(Cell::new(0));
    let peak = Rc::new(Cell::new(0));
    let mut release = Vec::new();
    let mut work = Vec::new();
    for index in 0..count {
        let (send, waiting) = futures::channel::oneshot::channel();
        release.push(send);
        let (live, peak) = (live.clone(), peak.clone());
        work.push(async move {
            live.set(live.get() + 1);
            peak.set(peak.get().max(live.get()));
            let _running = Running(live);
            waiting.await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(index)
        });
    }
    let mut results: std::pin::Pin<Box<dyn futures::Stream<Item = TestResult<usize>>>> =
        if unbounded {
            // Former admission: every case acquires state and starts its guard before
            // any native I/O has finished. Keep the same jobs and expected bound.
            Box::pin(futures::stream::iter(work).buffer_unordered(count.max(1)))
        } else {
            Box::pin(super::parallel_cases(work, workers))
        };
    let first = results.next().now_or_never();
    assert_eq!(first.is_some(), count == 0);
    assert_eq!(
        live.get(),
        count.min(if unbounded { count } else { workers })
    );
    for send in release {
        send.send(()).map_err(|_| "case dropped before admission")?;
    }
    let completed = futures::executor::block_on(results.collect::<Vec<_>>());
    let mut ids: Vec<_> = completed.into_iter().collect::<TestResult<_>>()?;
    ids.sort_unstable();
    assert_eq!(ids, (0..count).collect::<Vec<_>>(), "coverage was lost");
    assert_eq!(live.get(), 0, "case state retained after completion");
    if peak.get() > workers {
        return Err("case admission exceeded the shared I/O bound".into());
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn bounded_admission_runs_every_case_once_without_retaining_queued_state(count in 0usize..97, available in proptest::num::usize::ANY) {
        let workers = scenario_workers(available);
        proptest::prop_assert!((2..=8).contains(&workers));
        if (2..=8).contains(&available) {
            proptest::prop_assert_eq!(workers, available);
        }
        if available > 8 {
            proptest::prop_assert_eq!(workers, 8);
        }
        admission(count, workers, false).map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    }
}

#[test]
fn unbounded_admission_fails_the_same_live_state_bound() {
    let workers = scenario_workers(4);
    assert_eq!(
        admission(workers + 1, workers, true)
            .unwrap_err()
            .to_string(),
        "case admission exceeded the shared I/O bound"
    );
}
