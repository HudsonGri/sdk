use super::*;

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
