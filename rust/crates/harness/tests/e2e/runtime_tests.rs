use super::{TestResult, runtime_workers, scenario_builder};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

proptest::proptest! {
    #[test]
    fn runtime_pools_share_available_parallelism(available in proptest::num::usize::ANY) {
        let workers = runtime_workers(available);
        proptest::prop_assert!(workers >= 1);
        proptest::prop_assert!(workers <= available.max(1));
        if available >= 6 {
            proptest::prop_assert!(workers <= available / 6);
            proptest::prop_assert!(available - 6 * workers < 6);
        }
    }
}

#[test]
fn blocking_pool_queues_work_instead_of_creating_another_thread() -> TestResult {
    for broken in [false, true] {
        let created = Arc::new(AtomicUsize::new(0));
        let names = created.clone();
        let mut builder = scenario_builder(6); // One async and one blocking worker.
        if broken {
            builder.max_blocking_threads(2);
        }
        // Tokio calls the name factory synchronously when requesting a thread,
        // so this observes creation without sampling or waiting for scheduling.
        let runtime = builder
            .thread_name_fn(move || {
                names.fetch_add(1, Ordering::SeqCst);
                "e2e-budget".into()
            })
            .build()?;
        let (entered, waiting) = mpsc::sync_channel(0);
        let (release, gate) = mpsc::channel();
        let first = runtime.spawn_blocking(move || -> TestResult {
            entered.send(())?;
            gate.recv()?;
            Ok(())
        });
        waiting.recv()?;
        let second = runtime.spawn_blocking(|| 42);
        let threads = created.load(Ordering::SeqCst);
        // Release and join before asserting: even a failed bound cannot strand
        // the first job during Runtime::drop. Dropping release also opens it.
        release.send(())?;
        let answer = runtime.block_on(async {
            first.await??;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(second.await?)
        })?;
        assert_eq!(answer, 42, "queued work was discarded");
        assert_eq!(threads, if broken { 3 } else { 2 });
        assert_eq!(threads <= 2, !broken, "blocking-pool control escaped");
    }
    Ok(())
}
