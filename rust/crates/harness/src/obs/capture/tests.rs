use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::instrument::WithSubscriber as _;

async fn body(owner: &'static str) {
    tracing::info!(owner, "test capture");
    let mut yielded = false;
    futures::future::poll_fn(|cx| {
        if std::mem::replace(&mut yielded, true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
    let _span = tracing::info_span!("test.capture", owner);
}

fn owns_exactly(seen: &Seen, owner: &str, count: usize) -> bool {
    let seen = seen.lock().unwrap();
    let owners: Vec<_> = seen
        .iter()
        .filter(|(_, field, _)| *field == "owner")
        .collect();
    owners.len() == count && owners.iter().all(|(_, _, value)| value == owner)
}

// Exhaust every ordering of two polls for each test future, including nested
// entry, interleaving, and sequential completion. No scheduler or timing bound.
fn isolated(mode: u8) -> bool {
    // The broken guard control deliberately leaves defaults out of order.
    // Restore the runner's original subscriber even for that control.
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || schedules(mode))
}

fn schedules(mode: u8) -> bool {
    for schedule in [
        [0, 0, 1, 1],
        [0, 1, 0, 1],
        [0, 1, 1, 0],
        [1, 0, 0, 1],
        [1, 0, 1, 0],
        [1, 1, 0, 0],
    ] {
        let (shared_seen, shared_dispatch, _registration) = subscriber();
        let first = Seen::default();
        let second = Seen::default();
        let mut tests: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
        for (owner, target) in [("first", first.clone()), ("second", second.clone())] {
            let shared_dispatch = shared_dispatch.clone();
            let shared_seen = shared_seen.clone();
            tests.push(Box::pin(async move {
                let seen = match mode {
                    0 => {
                        run(|seen| async move {
                            body(owner).await;
                            seen
                        })
                        .await
                    }
                    // Reverted helper: the thread guard spans an await.
                    1 => {
                        let (seen, dispatch, _registration) = subscriber();
                        let _guard = tracing::dispatcher::set_default(&dispatch);
                        body(owner).await;
                        seen
                    }
                    // A process subscriber routes both tests to the same sink.
                    _ => {
                        body(owner).with_subscriber(shared_dispatch).await;
                        shared_seen
                    }
                };
                *target.lock().unwrap() = seen.lock().unwrap().clone();
            }));
        }
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        let mut polls = [0; 2];
        for index in schedule {
            polls[index] += 1;
            assert_eq!(
                tests[index].as_mut().poll(&mut cx).is_ready(),
                polls[index] == 2
            );
        }
        if !owns_exactly(&first, "first", 2) || !owns_exactly(&second, "second", 2) {
            if mode == 2 {
                assert!(!owns_exactly(&shared_seen, "first", 4));
                assert_eq!(
                    shared_seen
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(_, f, _)| *f == "owner")
                        .count(),
                    4
                );
            }
            return false;
        }
    }
    true
}

#[test]
fn concurrent_test_captures_are_isolated_at_every_poll() {
    assert!(isolated(0));
    assert!(!isolated(1), "thread guard across await escaped the oracle");
    assert!(!isolated(2), "shared process subscriber escaped the oracle");
}

#[tokio::test]
async fn spawned_tasks_retain_their_test_subscriber() {
    async fn test(owner: &'static str, lose_dispatch: bool) -> bool {
        run(|seen| async move {
            let group = crate::live::TaskGroup::new(1, None).unwrap();
            let crate::Admission::Accepted(task) = group
                .spawn(crate::OperationId::new(), async move {
                    if lose_dispatch {
                        body(owner).with_subscriber(tracing::Dispatch::none()).await;
                    } else {
                        body(owner).await;
                    }
                })
                .await
            else {
                panic!("task was rejected")
            };
            assert!(matches!(task.result().await, crate::Outcome::Succeeded(())));
            owns_exactly(&seen, owner, 2)
        })
        .await
    }
    let (first, second) = futures::join!(test("first", false), test("second", false));
    assert!(first && second);
    assert!(
        !test("lost", true).await,
        "lost task dispatcher escaped the oracle"
    );
}
