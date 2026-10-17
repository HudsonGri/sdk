//! Actual owned tasks keep their driver binding after caller cancellation.
use super::*;
use std::sync::OnceLock;

fn retained_binding(broken: bool) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let driver = std::thread::current().id();
        let observed = Arc::new(OnceLock::new());
        let output = observed.clone();
        let (schedule, mut events) = Schedule::new();
        let mut caller = Box::pin(with_local_tasks(schedule.scope(async move {
            spawn(LOCAL_TASKS.scope(!broken, async move {
                let owner = std::thread::current().id();
                let child = spawn(async { std::thread::current().id() }).await.unwrap();
                let worker = crate::run_blocking_io(|| std::thread::current().id(), None)
                    .unwrap()
                    .await
                    .unwrap();
                output.set((owner, child, worker)).unwrap();
            }))
            .await
            .unwrap();
        })));
        let accepted = tokio::select! {
            _ = &mut caller => panic!("caller passed its submission gate"),
            event = events.recv() => event.unwrap(),
        };
        assert_eq!(accepted.name, "task:submitted");
        schedule.cancel();
        drop(caller);
        accepted.release();
        let mut boundaries = vec!["task:submitted"];
        loop {
            tokio::select! {
                _ = schedule.drained() => break,
                event = events.recv() => {
                    let event = event.unwrap();
                    boundaries.push(event.name);
                    event.release();
                },
            }
        }
        let &(owner, child, worker) = observed.get().expect("owned effect was lost");
        assert_eq!(owner, driver, "owner moved off its driver");
        assert_eq!(child, driver, "owned completion lost its local binding");
        assert_ne!(
            worker, driver,
            "controlled native I/O bypassed the real queue"
        );
        assert_eq!(
            boundaries
                .iter()
                .filter(|&&name| name == "native:submitted")
                .count(),
            1
        );
        assert_eq!(
            boundaries
                .iter()
                .filter(|&&name| name == "native:completed")
                .count(),
            1
        );
        assert!(
            !LOCAL_TASKS.try_with(|local| *local).unwrap_or(false),
            "local binding leaked outside its task scope"
        );
    }));
}

#[test]
fn local_completion_retains_binding_after_caller_cancellation() {
    retained_binding(false);
}

#[test]
#[should_panic(expected = "owned completion lost its local binding")]
fn broken_binding_fails_the_same_completion_check() {
    retained_binding(true);
}
