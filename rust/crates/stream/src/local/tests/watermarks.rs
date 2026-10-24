use super::*;
use proptest::prelude::*;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn qualify(operations: &[u8], early_durable: bool) -> Result {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let paths = ["a", "b", "c"].map(|path| StreamPath::new(path).unwrap());
        let mut accepted = [0; 3];
        let mut durable = [0; 3];
        let mut frames = 0;
        for &operation in operations {
            match operation % 4 {
                3 => {
                    provider.flush()?;
                    durable = accepted;
                }
                selected => {
                    let selected = usize::from(selected);
                    let (Some(path), Some(count)) =
                        (paths.get(selected), accepted.get_mut(selected))
                    else {
                        return Err("operation names no path".into());
                    };
                    deferring_durability(provider.append(AppendRequest {
                        path: path.clone(),
                        records: vec![(Bytes::from_static(b"accepted")).into()],
                        if_tail: None,
                        idempotency_key: None,
                    }))
                    .await?;
                    *count += 1;
                    frames += 1;
                }
            }
            assert_eq!(
                *provider.inner.revision.borrow(),
                frames,
                "publication count differs from accepted journal frames"
            );
            assert_eq!(
                provider
                    .inner
                    .journal
                    .lock()
                    .map_err(|_| "journal")?
                    .appended,
                frames
            );
            if early_durable {
                provider.inner.synced.send_replace(frames);
            }
            for ((path, accepted), durable) in paths.iter().zip(accepted).zip(&mut durable) {
                if accepted == 0 {
                    assert_eq!(
                        provider.watermarks(path.clone()).await,
                        Err(StreamError::NotFound)
                    );
                    continue;
                }
                let actual = provider.watermarks(path.clone()).await?;
                assert_eq!(actual.accepted, accepted);
                assert_eq!(
                    actual.durable, *durable,
                    "durable watermark must reflect only a completed device cut"
                );
                assert!(actual.durable <= actual.accepted);
            }
        }
        Ok(())
    })
}

async fn independent_paths(global_cut_only: bool) -> Result {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let a = StreamPath::new("durable-actor")?;
    let b = StreamPath::new("other-actor")?;
    for (path, deferred) in [(a.clone(), false), (b, true), (a.clone(), true)] {
        let request = AppendRequest {
            path,
            records: vec![Bytes::from_static(b"event").into()],
            if_tail: None,
            idempotency_key: None,
        };
        if deferred {
            deferring_durability(provider.append(request)).await?;
        } else {
            provider.append(request).await?;
        }
    }
    // No earlier watermark read primed a cache. A second pending write on A
    // must retain its first, flushed prefix despite B's pending publication.
    let mut actual = provider.watermarks(a.clone()).await?;
    if global_cut_only && *provider.inner.synced.borrow() < *provider.inner.revision.borrow() {
        actual.durable = 0;
    }
    assert_eq!(
        actual,
        crate::StreamWatermarks {
            accepted: 2,
            durable: 1
        },
        "unrelated pending work must not hide the durable actor prefix"
    );
    provider.flush()?;
    assert_eq!(provider.watermarks(a).await?.durable, 2);
    Ok(())
}

#[tokio::test]
async fn unrelated_pending_frames_preserve_each_paths_exact_durable_prefix() -> Result {
    independent_paths(false).await
}

#[tokio::test]
async fn fork_commit_native_and_reopen_preserve_exact_path_cuts() -> Result {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    let source = StreamPath::new("source")?;
    let child = StreamPath::new("child")?;
    provider.append(keyed_append("source", 0)?).await?;
    deferring_durability(provider.fork(ForkRequest {
        source: source.clone(),
        destination: child.clone(),
        at_tail: Some(1),
        idempotency_key: None,
    }))
    .await?;
    assert_eq!(provider.watermarks(source.clone()).await?.durable, 1);
    assert_eq!(
        provider.watermarks(child.clone()).await?,
        crate::StreamWatermarks {
            accepted: 1,
            durable: 0
        }
    );
    provider.flush()?;
    deferring_durability(provider.commit(keyed_commit("child", 1)?)).await?;
    assert_eq!(
        provider.watermarks(child.clone()).await?,
        crate::StreamWatermarks {
            accepted: 2,
            durable: 1
        }
    );
    let meter = Arc::new(Meter::local());
    deferring_durability(provider.native_append(keyed_append("source", 1)?, meter.original()))
        .await??;
    assert_eq!(
        provider.watermarks(source.clone()).await?,
        crate::StreamWatermarks {
            accepted: 2,
            durable: 1
        }
    );
    deferring_durability(provider.native_commit(keyed_commit("child", 2)?, None, meter.original()))
        .await??;
    // The native commit uses the prepared writer, whose acknowledgement
    // includes its device flush. That flush covers both paths' pending cuts.
    assert_eq!(
        provider.watermarks(child.clone()).await?,
        crate::StreamWatermarks {
            accepted: 3,
            durable: 3
        }
    );
    provider.flush()?;
    drop(provider);
    meter.assert_refunded();
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.watermarks(source).await?.durable, 2);
    assert_eq!(reopened.watermarks(child).await?.durable, 3);
    Ok(())
}

#[tokio::test]
async fn rejected_paths_leave_no_durability_index_entries() -> Result {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    for id in 0..128 {
        let mut request = keyed_append(&format!("rejected/{id}"), 1)?;
        request.idempotency_key = None;
        assert!(matches!(
            provider.append(request).await?,
            AppendOutcome::TailConflict { .. }
        ));
    }
    assert!(
        provider
            .inner
            .durable_tails
            .lock()
            .map_err(|_| "index poisoned")?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "unrelated pending work must not hide the durable actor prefix")]
async fn negative_control_global_synced_revision_hides_a_durable_actor() {
    independent_paths(true).await.unwrap();
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // The independent reference tracks append and explicit-flush events, never
    // the production watch comparison or the watermark result as its oracle.
    #[test]
    fn accepted_and_durable_watch_cuts_match_the_journal_and_reference(
        operations in prop::collection::vec(any::<u8>(), 0..24),
    ) {
        qualify(&operations, false).unwrap();
    }

    #[test]
    fn prefunded_pending_cuts_preserve_fifo_across_stack_transfers(
        operations in prop::collection::vec(any::<bool>(), 0..128),
    ) {
        let mut actual = PendingCuts::default();
        let mut expected = std::collections::VecDeque::new();
        let mut sequence = 0;
        let mut synced = 0;
        for append in operations {
            if append {
                sequence += 1;
                let mut node = PendingCut::prepare(None).unwrap();
                node.sequence = sequence;
                node.tail = sequence * 3;
                actual.push(node);
                expected.push_back((sequence, sequence * 3));
            } else {
                synced = (synced + 1).min(sequence);
                let wanted = expected.front().filter(|(cut, _)| *cut <= synced).map(|(_, tail)| *tail);
                if wanted.is_some() { expected.pop_front(); }
                prop_assert_eq!(actual.pop_through(synced), wanted);
            }
        }
        while let Some((_, wanted)) = expected.pop_front() {
            prop_assert_eq!(actual.pop_through(sequence), Some(wanted));
        }
        prop_assert_eq!(actual.pop_through(sequence), None);
    }
}
#[test]
#[should_panic(expected = "durable watermark must reflect only a completed device cut")]
fn negative_control_publishes_a_durable_cut_before_the_device() {
    qualify(&[0], true).unwrap();
}
