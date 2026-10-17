//! Whole per-delta publication cost: exact bytes, work and allocations at N
//! and 2N against the former private-file path and a whole-file rewrite.
use super::*;

/// Every durable and in-memory cost of publishing `count` deltas.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Cost {
    stream_writes: u64,
    stream_records: u64,
    stream_bytes: u64,
    /// Ledgered authority (stream) appends and bytes.
    ledger_records: u64,
    ledger_bytes: u64,
    /// Private-file object writes and bytes.
    object_writes: u64,
    object_bytes: u64,
    items: u64,
    allocations: u64,
    allocated_bytes: u64,
    native: [u64; 4],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Path {
    /// The production live-chunk path.
    Chunk,
    /// The former path: a private JSON file per delta, then its observation.
    Private,
    /// Negative control: the chunk path plus a whole-transcript rewrite.
    Rewrite,
}

async fn prepared(path: Path) -> Result<State> {
    let (f, mut journal, files, model) = fixture().await?;
    publish_peers(f.stream.as_ref(), &files, &task_of(&journal), 10).await;
    if path == Path::Private {
        journal.task_files = None;
    }
    Ok((f, journal, files, model))
}

async fn record(
    journal: &Journal,
    path: Path,
    step: u32,
    index: u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let key = format!("event:{index}");
    if path != Path::Private {
        return journal.append_delta(OP, &key, step, &working(), io).await;
    }
    let reference = crate::executor::stage_json(journal, OP, &key, &working(), io).await?;
    journal
        .append(
            OP,
            &key,
            &ExecutionEvent::Model {
                step,
                purpose: crate::executor::ModelPurpose::Response,
                event: reference,
            },
            io,
        )?
        .await
}

/// Publishes `count` deltas, then reads every one back outside the measure.
fn publish(
    runtime: &tokio::runtime::Runtime,
    path: Path,
    count: u64,
    native: bool,
) -> (Vec<Vec<u8>>, String, Cost) {
    let state = runtime.block_on(prepared(path)).unwrap();
    accounted::with_delta_rewrite(path == Path::Rewrite, || {
        accounted::accounted(native, 1 << 30, false, |io| {
            let (f, journal, files, _) = state;
            let journal = if native {
                runtime
                    .block_on(f.native_execution(1, OP, io))
                    .unwrap()
                    .with_task_files((path != Path::Private).then(|| files.clone()))
            } else {
                journal
            };
            let io = native.then_some(io);
            f.stream.reset_io();
            let before = io.map(PreparationIo::receipt).unwrap_or_default();
            let mut outcome: Result<()> = Ok(());
            let mut items = 0;
            let allocations = acyclic_test_support::measure(|| {
                items = crate::test_support::measure_task_volume_work(|| {
                    for index in 100..100 + count {
                        if outcome.is_ok() {
                            outcome = runtime.block_on(record(&journal, path, 0, index, io));
                        }
                    }
                })
                .1;
            });
            outcome.unwrap();
            let after = io.map(PreparationIo::receipt).unwrap_or_default();
            let stream = f.stream.io_snapshot();
            let error = runtime
                .block_on(record(&journal, path, 1, 999, io))
                .unwrap_err()
                .to_string();
            let records = runtime
                .block_on(journal.replay(OP, 2, 64, None).unwrap())
                .unwrap();
            let mut bytes = Vec::new();
            for record in records {
                let ExecutionEvent::Model { event, .. } = record.event else {
                    unreachable!()
                };
                bytes.push(
                    runtime
                        .block_on(journal.load(&event, None).unwrap())
                        .unwrap()
                        .as_ref()
                        .to_vec(),
                );
            }
            let cost = Cost {
                stream_writes: stream.writes,
                stream_records: stream.records,
                stream_bytes: stream.write_bytes,
                ledger_records: after.authority_records_appended
                    - before.authority_records_appended,
                ledger_bytes: after.authority_bytes_written - before.authority_bytes_written,
                object_writes: after.backend_write_operations - before.backend_write_operations,
                object_bytes: after.object_bytes_written - before.object_bytes_written,
                items,
                allocations: allocations.count_total,
                allocated_bytes: allocations.bytes_total,
                native: [
                    after.allocation_operations - before.allocation_operations,
                    after.bytes_copied - before.bytes_copied,
                    after.bytes_encoded - before.bytes_encoded,
                    after.bytes_hashed - before.bytes_hashed,
                ],
            };
            (bytes, error, cost)
        })
        .unwrap()
        .0
    })
}

/// Exactly linear: every durable byte doubles with the deltas, and 2N
/// examines exactly two index items more than twice N.
fn linear(small: Cost, large: Cost) -> bool {
    let fields = |cost: Cost| {
        [
            cost.stream_writes,
            cost.stream_records,
            cost.stream_bytes,
            cost.ledger_records,
            cost.ledger_bytes,
            cost.object_writes,
            cost.object_bytes,
        ]
    };
    fields(small).map(|value| 2 * value) == fields(large) && large.items == 2 * small.items + 2
}

#[test]
fn delta_publication_is_exactly_linear_in_bytes_work_and_allocations() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let measured = [false, true].map(|native| {
        let [small, large] = [8, 16].map(|count| publish(&runtime, Path::Chunk, count, native));
        let former = publish(&runtime, Path::Private, 8, native);
        eprintln!("delta native={native} N=8 {:?}", small.2);
        eprintln!("delta native={native} N=16 {:?}", large.2);
        eprintln!("private native={native} N=8 {:?}", former.2);
        (small, large, former)
    });
    for (native, (small, large, former)) in [false, true].into_iter().zip(measured) {
        // The independent former path reads back identical JSON and errors.
        assert_eq!(small.0, former.0, "native={native}");
        assert_eq!(small.1, former.1, "native={native}");
        assert_eq!(small.0.len(), 8);
        let n = 8;
        let (cost, large) = (small.2, large.2);
        // One fenced commit of an observation and its chunk per delta, the
        // same 995 bytes for every delta, and no private object write.
        assert_eq!((cost.stream_writes, cost.stream_records), (n, 2 * n));
        assert_eq!(cost.stream_bytes, n * 995, "native={native}");
        // One backend write per delta on the native ledger (its fenced
        // append), and no private object bytes on either provider.
        let writes = if native { n } else { 0 };
        assert_eq!((cost.object_writes, cost.object_bytes), (writes, 0));
        if native {
            assert_eq!(cost.ledger_bytes, cost.stream_bytes);
            // The ledger saw the private object writes the former path made.
            assert!(former.2.object_writes > 0 && former.2.object_bytes > 0);
        }
        assert!(
            former.2.allocated_bytes > 3 * cost.allocated_bytes,
            "native={native}"
        );
        // Exact whole-path work and allocations, including the in-memory
        // provider. Both journals advance one retained summary per append.
        let pinned = match (native, cfg!(target_os = "macos")) {
            (true, _) => [
                (118, 3_204, 1_926_145, [2_486, 60_899, 71_185, 81_099]),
                (238, 6_170, 3_805_041, [4_761, 126_307, 138_161, 156_859]),
            ],
            (false, false) => [
                (118, 2_439, 605_799, [0; 4]),
                (238, 4_645, 1_168_095, [0; 4]),
            ],
            // Darwin's runtime setup allocates a constant 7 more (600 B).
            (false, true) => [
                (118, 2_446, 606_399, [0; 4]),
                (238, 4_652, 1_168_695, [0; 4]),
            ],
        };
        let exact = |cost: Cost| {
            (
                cost.items,
                cost.allocations,
                cost.allocated_bytes,
                cost.native,
            )
        };
        assert_eq!([exact(cost), exact(large)], pinned, "native={native}");
        // Every durable byte doubles on both paths.
        assert!(
            linear(
                cost,
                Cost {
                    items: 2 * cost.items + 2,
                    ..large
                }
            ),
            "native={native}"
        );
        assert!(linear(cost, large), "native={native}");
    }
}

#[test]
fn whole_transcript_rewrite_negative_breaks_the_linear_pins() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let [small, large] = [8, 16].map(|count| publish(&runtime, Path::Rewrite, count, false));
    let [chunk, _] = [8, 16].map(|count| publish(&runtime, Path::Chunk, count, false));
    eprintln!("rewrite N=8 {:?} N=16 {:?}", small.2, large.2);
    assert_eq!(small.0, chunk.0, "the control changes cost, not content");
    assert!(
        !linear(small.2, large.2),
        "a growing rewrite passed the N/2N pin"
    );
    assert!(large.2.allocated_bytes > 2 * small.2.allocated_bytes);
}

/// Native work of `count` deltas on a real local stream and filesystem, after
/// nine peer chunks and one warm-up delta: (items, allocations, allocated bytes, native counters,
/// ledger records and bytes).
#[cfg(feature = "filesystem-local")]
fn on_disk(runtime: &tokio::runtime::Runtime, count: u64) -> (u64, u64, u64, [u64; 4], [u64; 2]) {
    let directory = tempfile::tempdir().unwrap();
    let (f, journal, files, _) = runtime
        .block_on(async {
            let stream = acyclic_stream::LocalStream::open(
                directory.path().join("stream"),
                Default::default(),
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
            let backend =
                acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.path().join("fs")))
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
            super::fixture_on(Arc::new(stream), backend).await
        })
        .unwrap();
    runtime.block_on(publish_peers(
        f.stream.as_ref(),
        &files,
        &task_of(&journal),
        9,
    ));
    accounted::accounted(true, 1 << 30, false, |io| {
        let journal = runtime
            .block_on(f.native_execution(1, OP, io))
            .unwrap()
            .with_task_files(Some(files.clone()));
        let append = |index: u64| {
            runtime.block_on(journal.append_delta(
                OP,
                &format!("event:{index}"),
                0,
                &working(),
                Some(io),
            ))
        };
        append(99).unwrap();
        let before = io.receipt();
        let mut outcome = Ok(());
        let mut items = 0;
        let allocations = acyclic_test_support::measure(|| {
            items = crate::test_support::measure_task_volume_work(|| {
                for index in 100..100 + count {
                    if outcome.is_ok() {
                        outcome = append(index);
                    }
                }
            })
            .1;
        });
        outcome.unwrap();
        let after = io.receipt();
        (
            items,
            allocations.count_total,
            allocations.bytes_total,
            [
                after.allocation_operations - before.allocation_operations,
                after.bytes_copied - before.bytes_copied,
                after.bytes_encoded - before.bytes_encoded,
                after.bytes_hashed - before.bytes_hashed,
            ],
            [
                after.authority_records_appended - before.authority_records_appended,
                after.authority_bytes_written - before.authority_bytes_written,
            ],
        )
    })
    .unwrap()
    .0
}

type DiskCost = (u64, u64, u64, [u64; 4], [u64; 2]);

/// What 2N deltas cost beyond twice N: allocated bytes, then native
/// allocations, copy, encoding and hash bytes.
fn excess(small: DiskCost, large: DiskCost) -> [i128; 5] {
    let twice = |small: u64, large: u64| i128::from(large) - 2 * i128::from(small);
    [
        twice(small.2, large.2),
        twice(small.3[0], large.3[0]),
        twice(small.3[1], large.3[1]),
        twice(small.3[2], large.3[2]),
        twice(small.3[3], large.3[3]),
    ]
}

/// Linear work leaves 2N within a constant of twice N. The byte slack is one
/// geometric reallocation of the stream provider's retained-commit index
/// (16 B per retained commit, at most 2,048 moved here); the counter slack
/// covers varint widths of offsets that grow with the stream.
fn linear_on_disk(excess: [i128; 5]) -> bool {
    let [bytes, allocations, copy, encoding, hash] = excess;
    bytes <= 1 << 15 && copy <= 1 << 15 && allocations <= 16 && encoding <= 256 && hash <= 256
}

#[test]
#[cfg(feature = "filesystem-local")]
fn native_delta_publication_on_local_disk_is_linear_with_exact_counts() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let [small, large] = [8, 16].map(|count| on_disk(&runtime, count));
    // Negative control: the same deltas with every append replaying the whole
    // journal instead of advancing the retained summary.
    let [replay_small, replay_large] =
        [8, 16].map(|count| accounted::with_append_scan(true, || on_disk(&runtime, count)));
    eprintln!("disk N=8 {small:?} N=16 {large:?}");
    eprintln!("disk replay N=8 {replay_small:?} N=16 {replay_large:?}");
    // Exact counts at N and 2N on the real local stream and filesystem, the
    // same on Linux and Windows. Debug builds of the local stream make one
    // more 104 B SDK allocation per delta; every native counter and ledger
    // byte is identical.
    let (allocations, bytes) = if cfg!(debug_assertions) {
        ([2_942, 5_884], [1_994_390, 4_010_878])
    } else {
        ([2_934, 5_868], [1_993_558, 4_009_214])
    };
    assert_eq!(
        [small, large],
        [
            (
                120,
                allocations[0],
                bytes[0],
                [2_225, 64_472, 78_392, 87_608],
                [16, 7_960]
            ),
            (
                240,
                allocations[1],
                bytes[1],
                [4_452, 141_520, 156_912, 175_248],
                [32, 15_920]
            ),
        ]
    );
    // Index items, SDK allocations and every ledger record and byte double.
    assert_eq!(large.0, 2 * small.0, "items");
    assert_eq!(large.1, 2 * small.1, "allocations");
    assert_eq!(large.4, small.4.map(|value| 2 * value), "ledger");
    assert_eq!(small.4[0], 2 * 8, "one observation and one chunk per delta");
    assert_eq!(small.0, 8 * 15, "index items per delta");
    assert!(
        linear_on_disk(excess(small, large)),
        "{:?}",
        excess(small, large)
    );
    let replayed = excess(replay_small, replay_large);
    assert!(
        !linear_on_disk(replayed),
        "whole-journal replay passed: {replayed:?}"
    );
}
