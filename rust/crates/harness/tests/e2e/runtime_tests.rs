use super::{BLOCKING_WORKERS, TestResult, scenario_builder};
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
        const ASYNC_WORKERS: usize = 2;
        let mut builder = scenario_builder();
        builder.worker_threads(ASYNC_WORKERS);
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
        let bound = ASYNC_WORKERS + BLOCKING_WORKERS;
        assert_eq!(threads, bound + usize::from(broken));
        assert_eq!(threads <= bound, !broken, "blocking-pool control escaped");
    }
    Ok(())
}

#[test]
fn async_pool_matches_the_bounded_workload_and_detects_one_extra_worker() -> TestResult {
    let available = std::thread::available_parallelism().map_or(1, usize::from);
    let expected = available.clamp(2, super::MAX_ASYNC_WORKERS);
    for broken in [false, true] {
        let created = Arc::new(AtomicUsize::new(0));
        let names = created.clone();
        let mut builder = scenario_builder();
        if broken {
            builder.worker_threads(expected + 1);
        }
        let runtime = builder
            .thread_name_fn(move || {
                names.fetch_add(1, Ordering::SeqCst);
                "e2e-async-budget".into()
            })
            .build()?;
        let actual = created.load(Ordering::SeqCst);
        drop(runtime);
        assert_eq!(actual, expected + usize::from(broken));
        assert_eq!(actual == expected, !broken);
    }
    Ok(())
}
