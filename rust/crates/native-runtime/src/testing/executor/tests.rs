use super::*;

async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

fn script(seed: u64) -> io::Result<Vec<(u64, &'static str)>> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(seed);
    runtime.block_on(executor.run(async {
        let tasks: Vec<_> = (0..4)
            .map(|id| {
                executor.spawn(async move {
                    yield_once().await;
                    id
                })
            })
            .collect();
        for (expected, task) in tasks.into_iter().enumerate() {
            assert_eq!(task.await.unwrap(), expected);
        }
    }));
    assert!(lock(&executor.0).live.is_empty());
    Ok(executor.trace())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn same_seed_replays_every_original_task_poll(seed in proptest::prelude::any::<u64>()) {
        proptest::prop_assert_eq!(script(seed).unwrap(), script(seed).unwrap());
    }

    #[test]
    fn every_continuously_ready_task_runs_in_each_round(seed in proptest::prelude::any::<u64>()) {
        fair_rounds(seed, false);
    }
}

fn fair_rounds(seed: u64, forget_round: bool) {
    let executor = Executor::new(seed);
    lock(&executor.0).forget_round = forget_round;
    let tasks: Vec<_> = (0..4).map(|_| executor.register(None)).collect();
    let driver = Waker::noop();
    let cx = PollContext::from_waker(driver);
    for _ in 0..8 {
        let mut seen = BTreeSet::new();
        for _ in 0..tasks.len() {
            let (id, work, cancelled) = executor.next(&cx).unwrap();
            assert!(work.is_none() && !cancelled);
            assert!(seen.insert(id), "ready task starved within a round");
            executor.ready(id);
        }
        assert_eq!(seen.len(), tasks.len());
    }
    for task in tasks {
        executor.retire(task.id);
    }
}

#[test]
#[should_panic(expected = "ready task starved within a round")]
fn negative_control_removing_rounds_allows_a_ready_task_to_starve() {
    fair_rounds(0, true);
}

#[test]
fn different_seeds_explore_different_ready_orders() -> io::Result<()> {
    let traces: BTreeSet<_> = (0..8).map(script).collect::<io::Result<_>>()?;
    assert!(traces.len() > 1, "seed never changed the polling order");
    Ok(())
}

#[test]
fn dropped_handles_detach_and_aborted_futures_retire() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(73);
    runtime.block_on(executor.run(async {
        let (finished, waiting) = oneshot::channel();
        drop(executor.spawn(async move {
            yield_once().await;
            finished.send(91).unwrap();
        }));
        assert_eq!(waiting.await.unwrap(), 91);
        let task = executor.spawn(std::future::pending::<()>());
        task.abort();
        assert_eq!(task.await.unwrap_err().to_string(), "owned task cancelled");
    }));
    let state = lock(&executor.0);
    assert!(state.tasks.is_empty() && state.ready.is_empty() && state.live.is_empty());
    Ok(())
}

#[test]
fn owned_panics_keep_their_cause_and_retire() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(11);
    runtime.block_on(executor.run(async {
        let task = executor.spawn(async { panic!("original task cause") });
        let error = task.await.unwrap_err().to_string();
        assert_eq!(error, "owned task panicked: original task cause");
    }));
    assert!(lock(&executor.0).live.is_empty());
    Ok(())
}

#[test]
fn cancellation_of_the_root_keeps_owned_work_drivable() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(11);
    runtime.block_on(async {
        let (finished, mut waiting) = oneshot::channel();
        let mut root = Box::pin(executor.run(async {
            drop(executor.spawn(async {
                yield_once().await;
                finished.send(57).unwrap();
            }));
            std::future::pending::<()>().await
        }));
        // Poll the root until its real owned submission has been accepted.
        std::future::poll_fn(|cx| {
            assert!(root.as_mut().poll(cx).is_pending());
            if lock(&executor.0).next > 1 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        drop(root);
        let result = std::future::poll_fn(|cx| {
            executor.poll_owned(cx);
            Pin::new(&mut waiting).poll(cx)
        })
        .await
        .unwrap();
        assert_eq!(result, 57);
    });
    assert!(lock(&executor.0).live.is_empty());
    Ok(())
}
