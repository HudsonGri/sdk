use super::super::*;
use proptest::prelude::*;
use std::sync::atomic::AtomicU64;

fn baseline(cursor: &mut HistoryCursor, root: Option<&Arc<History>>, tail: u64) {
    if match (&cursor.root, root) {
        (Some(old), Some(root)) => Arc::ptr_eq(old, root),
        (None, None) => true,
        _ => false,
    } {
        return;
    }
    let mut added = Vec::new();
    let mut node = root;
    while let Some(current) = node {
        HISTORY_VISITS.with(|count| count.set(count.get() + 1));
        if cursor
            .root
            .as_ref()
            .is_some_and(|old| Arc::ptr_eq(old, current))
        {
            added.reverse();
            cursor.windows.extend(added);
            cursor.root = root.cloned();
            return;
        }
        match current.as_ref() {
            History::Batch {
                parent, records, ..
            } => {
                if let Some(window) = history_window(Arc::clone(records), tail, cursor.next) {
                    added.push(window);
                }
                if records
                    .records
                    .first()
                    .is_some_and(|first| first.sequence <= cursor.next)
                {
                    break;
                }
                node = parent.as_ref();
            }
            History::Prefix { .. } => break,
        }
    }
    added.clear();
    for (node, ceiling) in HistoryWalk::new(root, tail, cursor.next) {
        if let History::Batch { records, .. } = node
            && let Some(window) = history_window(Arc::clone(records), ceiling, cursor.next)
        {
            added.push(window);
        }
    }
    added.reverse();
    cursor.windows = added.into();
    cursor.root = root.cloned();
}

async fn seed(
    batches: usize,
    fork: bool,
) -> (
    MemoryStream,
    StreamPath,
    Vec<Result<AppendOutcome, StreamError>>,
    Vec<Result<ForkReceipt, StreamError>>,
) {
    let provider =
        MemoryStream::new_with_clock(MemoryLimits::default(), Arc::new(AtomicU64::new(7)));
    let source = StreamPath::new("history/source").unwrap();
    let mut appends = Vec::new();
    for i in 0..batches {
        appends.push(
            provider
                .append(AppendRequest {
                    path: source.clone(),
                    records: vec![Bytes::from(vec![i as u8, 0, 255])],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await,
        );
    }
    let mut forks = Vec::new();
    let path = if fork {
        let target = StreamPath::new("history/fork").unwrap();
        forks.push(
            provider
                .fork(ForkRequest {
                    source,
                    destination: target.clone(),
                    at_tail: Some((batches / 2) as u64),
                    idempotency_key: None,
                })
                .await,
        );
        forks.push(
            provider
                .fork(ForkRequest {
                    source: StreamPath::new("history/source").unwrap(),
                    destination: target.clone(),
                    at_tail: None,
                    idempotency_key: None,
                })
                .await,
        );
        target
    } else {
        source
    };
    (provider, path, appends, forks)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn cursor_windows_preserve_full_records_errors_and_observation_cuts(batches in 1usize..25, fork in any::<bool>(), from in 0usize..25, cut in 0usize..25) {
        let run = |old| futures::executor::block_on(async {
                let (provider, path, appends, forks) = seed(batches, fork).await;
                let state = provider.state.read().await;
                let stream = state.paths.get(&path).unwrap();
                let mut cursor = HistoryCursor::at((from as u64).min(stream.tail));
                if old { baseline(&mut cursor, stream.history.as_ref(), stream.tail); }
                else { cursor.sync(stream.history.as_ref(), stream.tail); }
                let first: Vec<_> = std::iter::from_fn(|| cursor.next_record()).map(record_bytes).take(cut).collect();
                drop(state);
                let appended = provider.append(AppendRequest {
                    path: path.clone(), records: vec![Bytes::from_static(b"new\0bytes")],
                    if_tail: None, idempotency_key: None,
                }).await;
                let state = provider.state.read().await;
                let stream = state.paths.get(&path).unwrap();
                if old { baseline(&mut cursor, stream.history.as_ref(), stream.tail); }
                else { cursor.sync(stream.history.as_ref(), stream.tail); }
                let rest: Vec<_> = std::iter::from_fn(|| cursor.next_record()).map(record_bytes).collect();
                (appends, forks, appended, first, rest, cursor.next)
        });
        let _ = acyclic_test_support::differential(|| run(true), || run(false));
    }
}

#[test]
fn initial_cursor_visits_each_batch_once_and_rescan_control_fails() {
    for batches in [4, 32] {
        let mut counts = Vec::new();
        let mut allocations = Vec::new();
        let mut observations = Vec::new();
        for old in [true, false] {
            let (provider, path, _, _) = futures::executor::block_on(seed(batches, false));
            let state = futures::executor::block_on(provider.state.read());
            let stream = state.paths.get(&path).unwrap();
            let mut cursor = HistoryCursor::at(0);
            HISTORY_VISITS.with(|count| count.set(0));
            allocations.push(acyclic_test_support::measure(|| {
                if old {
                    baseline(&mut cursor, stream.history.as_ref(), stream.tail);
                } else {
                    cursor.sync(stream.history.as_ref(), stream.tail);
                }
            }));
            counts.push(HISTORY_VISITS.with(std::cell::Cell::get));
            observations.push(
                std::iter::from_fn(|| cursor.next_record())
                    .map(record_bytes)
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(observations[0], observations[1]);
        assert_eq!(counts, [2 * batches, batches]);
        assert_ne!(counts[0], batches, "rescan control escaped the exact bound");
        assert_eq!(allocations[0].count_total, allocations[1].count_total);
        assert_eq!(allocations[1].count_total, if batches == 4 { 1 } else { 4 });
        println!("cursor batches={batches} visits={counts:?} allocations={allocations:?}");
    }
}

#[test]
fn appended_follow_reuses_the_cursor_queue_without_scratch_allocations() {
    let mut observations = Vec::new();
    let mut measured = Vec::new();
    for old in [true, false] {
        let (provider, path, _, _) = futures::executor::block_on(seed(4, false));
        let mut cursor = HistoryCursor::at(0);
        {
            let state = futures::executor::block_on(provider.state.read());
            let stream = state.paths.get(&path).unwrap();
            if old {
                baseline(&mut cursor, stream.history.as_ref(), stream.tail);
            } else {
                cursor.sync(stream.history.as_ref(), stream.tail);
            }
            while cursor.next_record().is_some() {}
        }
        futures::executor::block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"next")],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
        let state = futures::executor::block_on(provider.state.read());
        let stream = state.paths.get(&path).unwrap();
        HISTORY_VISITS.with(|count| count.set(0));
        let allocations = acyclic_test_support::measure(|| {
            if old {
                baseline(&mut cursor, stream.history.as_ref(), stream.tail);
            } else {
                cursor.sync(stream.history.as_ref(), stream.tail);
            }
        });
        measured.push((
            HISTORY_VISITS.with(std::cell::Cell::get),
            allocations.count_total,
            allocations.bytes_total,
        ));
        observations.push(
            std::iter::from_fn(|| cursor.next_record())
                .map(record_bytes)
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(observations[0], observations[1]);
    assert_eq!(
        measured[0],
        (2, 1, (4 * std::mem::size_of::<HistoryWindow>()) as u64)
    );
    assert_eq!(measured[1], (1, 0, 0));
    assert_ne!(
        measured[0], measured[1],
        "old scratch allocation escaped exact work assertions"
    );
    println!(
        "appended_follow before={:?} after={:?}",
        measured[0], measured[1]
    );
}

fn record_bytes(record: Record) -> (u64, Vec<u8>, CommitId, u64) {
    (
        record.sequence,
        record.value.to_vec(),
        record.commit_id,
        record.committed_at_micros,
    )
}
