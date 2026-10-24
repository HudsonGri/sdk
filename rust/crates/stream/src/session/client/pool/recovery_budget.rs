use super::*;

fn record(sequence: u64) -> wire::SessionData {
    wire::SessionData {
        after: Some(wire::SessionCursor {
            next: sequence + 1,
            opaque: Bytes::new(),
        }),
        accepted: sequence + 1,
        durable: sequence + 1,
        item: Some(Item::Record(wire::Record {
            sequence,
            commit_id: Bytes::from_static(&[1; 32]),
            ..Default::default()
        })),
    }
}

async fn replay_budget(rewind_progress: bool) -> Result<(), StreamError> {
    let endpoint = tonic::transport::Endpoint::from_static("http://unused.invalid").connect_lazy();
    let (mut pool, mut records) = fixture::fixture(&endpoint);
    pool.data("good", 3, 1, record(0), 0, 2)?;
    drop(records.recv().await.ok_or(StreamError::Unavailable)??);
    let attempts = crate::runtime::BackoffPolicy::CONNECTION.attempts;
    for attempt in 0..attempts {
        // A reconnect subscribes from the unconsumed application cursor.
        pool.subscribe(3)?;
        pool.links
            .get("good")
            .ok_or(StreamError::Unavailable)?
            .sender
            .next()
            .await?;
        if rewind_progress {
            pool.routes
                .get_mut(&3)
                .ok_or(StreamError::Unavailable)?
                .recovery_progress = 0;
        }
        pool.data("good", 3, 1, record(0), 0, 2)?;
        drop(records.recv().await.ok_or(StreamError::Unavailable)??);
        let route = pool.routes.get_mut(&3).ok_or(StreamError::Unavailable)?;
        route.buffered_data = 0;
        assert!(route.recovery.next(true, None, false).is_some());
        assert_eq!(
            route.recovery.attempts_used(),
            attempt + 1,
            "replayed data cannot renew the recovery budget"
        );
    }
    let route = pool.routes.get_mut(&3).ok_or(StreamError::Unavailable)?;
    assert!(route.recovery.next(true, None, false).is_none());
    // A record beyond the high-water cut really does advance recovery.
    pool.data("good", 3, 1, record(1), 0, 2)?;
    let route = pool.routes.get_mut(&3).ok_or(StreamError::Unavailable)?;
    assert_eq!(route.recovery_progress, 2);
    assert_eq!(route.recovery.attempts_used(), 0);
    Ok(())
}

#[tokio::test]
async fn buffered_replay_does_not_renew_recovery_but_new_data_does() -> Result<(), StreamError> {
    replay_budget(false).await
}

#[tokio::test]
#[should_panic(expected = "replayed data cannot renew the recovery budget")]
async fn negative_control_rewind_progress_with_subscription() {
    replay_budget(true).await.unwrap();
}
