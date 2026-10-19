use super::*;

// Saturation fixtures share the process-wide physical pool.
static SATURATION: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn unscoped_physical_workers_leave_the_driver_available_to_reply() {
    let _fixture = SATURATION.lock().unwrap_or_else(PoisonError::into_inner);
    let workers = crate::workers(&crate::BLOCKING_WORKERS, false).unwrap();
    let (started, receive) = std::sync::mpsc::channel();
    let mut occupants = Vec::new();
    let mut replies = Vec::new();
    let mut cx = std::task::Context::from_waker(Waker::noop());
    for index in 0..workers.threads {
        let (reply, waiting) = std::sync::mpsc::channel();
        replies.push(reply);
        let started = started.clone();
        let mut completion = Box::pin(
            crate::run_blocking_io(
                move || {
                    started.send(()).unwrap();
                    assert_eq!(waiting.recv().unwrap(), index);
                    index
                },
                None,
            )
            .unwrap(),
        );
        assert!(completion.as_mut().poll(&mut cx).is_pending());
        occupants.push(completion);
    }
    for _ in 0..workers.threads {
        receive.recv().unwrap();
    }
    let host = Host::for_workers(workers);
    let worker = host.clone();
    let mut completion = crate::run_blocking_io(move || worker.run(|| 42), None).unwrap();
    let mut job = completion.pending.take().unwrap();
    if let crate::NativeJob::Task { controlled, .. } = &mut job {
        *controlled = true;
    }
    let mut drive = Box::pin(host.drive(queued(job, workers)));
    // Every physical worker is waiting for this driver. Its first poll must
    // yield before any reply is sent, even though none belongs to this Host.
    assert!(drive.as_mut().poll(&mut cx).is_pending());
    for (index, reply) in replies.into_iter().enumerate() {
        reply.send(index).unwrap();
    }
    executor::complete_io(drive).unwrap();
    assert_eq!(executor::complete_io(completion).unwrap(), 42);
    for (index, completion) in occupants.into_iter().enumerate() {
        assert_eq!(executor::complete_io(completion).unwrap(), index);
    }
}

#[test]
fn queued_workers_leave_the_executor_available_to_reply() {
    let _fixture = SATURATION.lock().unwrap_or_else(PoisonError::into_inner);
    let count = crate::workers(&crate::BLOCKING_WORKERS, false)
        .unwrap()
        .threads;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(17);
    let mut completed = runtime.block_on(async {
        let mut root = Box::pin(schedule.scope(async {
            let (send, mut receive) = mpsc::unbounded_channel();
            let mut workers = Vec::new();
            for index in 0..=count {
                let send = send.clone();
                workers.push(spawn_blocking(move || {
                    let (reply, waiting) = oneshot::channel();
                    send.send((index, reply)).unwrap();
                    assert_eq!(blocking_on(waiting).unwrap(), index);
                    index
                }));
            }
            drop(send);
            // Occupy every physical worker before allowing any reply. Another
            // accepted job must queue while the root remains able to answer.
            let mut replies = Vec::new();
            for _ in 0..count {
                replies.push(receive.recv().await.unwrap());
            }
            for (index, reply) in replies {
                reply.send(index).unwrap();
            }
            let (index, reply) = receive.recv().await.unwrap();
            reply.send(index).unwrap();
            let mut completed = Vec::new();
            for worker in workers {
                completed.push(worker.await.unwrap());
            }
            completed
        }));
        let result = loop {
            tokio::select! {
                result = &mut root => break result,
                event = events.recv() => event.unwrap().release(),
            }
        };
        drop(root);
        schedule.drained().await;
        result
    });
    completed.sort_unstable();
    assert_eq!(completed, (0..=count).collect::<Vec<_>>());
}

fn exchanges(seed: u64) -> (Vec<u32>, Vec<(u64, &'static str)>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(seed);
    let values = runtime.block_on(async {
        let mut root = Box::pin(schedule.scope(async {
            let (send, mut receive) = mpsc::channel(1);
            let worker = spawn_blocking(move || {
                for value in 0..8 {
                    let (reply, waiting) = oneshot::channel();
                    blocking_on(send.send((value, reply))).unwrap();
                    assert_eq!(blocking_on(waiting).unwrap(), value * 2);
                }
                8
            });
            let mut values = Vec::new();
            while let Some((value, reply)) = receive.recv().await {
                values.push(value);
                reply.send(value * 2).unwrap();
            }
            assert_eq!(worker.await.unwrap(), 8);
            values
        }));
        let result = loop {
            tokio::select! {
                result = &mut root => break result,
                boundary = events.recv() => boundary.unwrap().release(),
            }
        };
        drop(root);
        schedule.drained().await;
        result
    });
    (values, schedule.polls())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn original_bounded_channels_and_replies_replay_without_blocking_the_driver(seed in proptest::prelude::any::<u64>()) {
        let first = exchanges(seed);
        proptest::prop_assert_eq!(&first.0, &(0..8).collect::<Vec<_>>());
        proptest::prop_assert_eq!(first, exchanges(seed));
    }
}

fn native_callback(
    seed: u64,
    erase_scope: bool,
) -> (Vec<u8>, Vec<String>, Vec<(u64, &'static str)>) {
    use crate::{NativeFile, OwnedRead};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"seed").unwrap();
    let file = NativeFile::from_file(std::fs::File::open(path).unwrap(), None)
        .map_err(|(error, _)| error)
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(seed);
    let bytes = runtime.block_on(async {
        tokio::time::pause();
        let mut root = Box::pin(schedule.scope(async {
            spawn_blocking(move || {
                let mut bytes = Vec::new();
                for offset in 0..4 {
                    let read = file.read_batch_async(vec![OwnedRead { offset, length: 1 }]);
                    let batch = if erase_scope {
                        // Erase only callback task ownership, retaining its
                        // seeded worker, clock, entropy and original device I/O.
                        blocking_on(CURRENT.scope(None, read))
                    } else {
                        blocking_on(read)
                    }
                    .unwrap();
                    bytes.extend_from_slice(&batch[0]);
                }
                bytes
            })
            .await
            .unwrap()
        }));
        let bytes = loop {
            tokio::select! {
                bytes = &mut root => break bytes,
                event = events.recv() => event.unwrap().release(),
            }
        };
        drop(root);
        schedule.drained().await;
        bytes
    });
    (bytes, schedule.trace(), schedule.polls())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn original_native_io_in_a_worker_callback_retains_seeded_task_ownership(
        seed in proptest::prelude::any::<u64>(),
    ) {
        let first = native_callback(seed, false);
        proptest::prop_assert_eq!(&first.0, b"seed");
        proptest::prop_assert_eq!(&first, &native_callback(seed, false));
        let owned = |trace: &[String]| trace.iter()
            .filter(|event| event.as_str() == "native:completion").count() == 4;
        proptest::prop_assert!(owned(&first.1));
        let erased = native_callback(seed, true);
        proptest::prop_assert_eq!(&erased.0, b"seed");
        proptest::prop_assert!(!owned(&erased.1), "scope-erased callback escaped the ownership oracle");
    }
}

#[test]
fn saturated_native_submission_leaves_the_simulated_deadline_available() {
    let _fixture = SATURATION.lock().unwrap_or_else(PoisonError::into_inner);
    let count = crate::workers(&crate::BLOCKING_WORKERS, false)
        .unwrap()
        .threads;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let (schedule, mut events) = Schedule::seeded(23);
    runtime.block_on(async {
        let mut root = Box::pin(schedule.scope(async {
            let (send, mut receive) = mpsc::unbounded_channel();
            let mut workers = Vec::new();
            for index in 0..count {
                let send = send.clone();
                workers.push(spawn_blocking(move || {
                    let (reply, waiting) = oneshot::channel();
                    send.send((index, reply)).unwrap();
                    assert_eq!(blocking_on(waiting).unwrap(), index);
                    index
                }));
            }
            drop(send);
            let mut replies = Vec::new();
            for _ in 0..count {
                replies.push(receive.recv().await.unwrap());
            }
            // Unlike spawn_blocking, this uses submit's direct native-job path.
            // All physical workers await this root; its timer must still run.
            let mut native = Box::pin(crate::run_blocking_io(|| 42, None).unwrap());
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(1), &mut native)
                    .await
                    .is_err()
            );
            for (index, reply) in replies {
                reply.send(index).unwrap();
            }
            assert_eq!(native.await.unwrap(), 42);
            for (index, worker) in workers.into_iter().enumerate() {
                assert_eq!(worker.await.unwrap(), index);
            }
        }));
        loop {
            tokio::select! {
                () = &mut root => break,
                event = events.recv() => event.unwrap().release(),
            }
        }
        drop(root);
        schedule.drained().await;
    });
}
