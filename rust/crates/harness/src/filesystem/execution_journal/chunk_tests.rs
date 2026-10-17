//! Real task-owned journal crash cuts and bounded chunk projection work.
use super::*;
use crate::runtime::DurableTaskHost;
use crate::test_stream::{ExecutionFaultMode, LostSessionAck};
use crate::test_support::accounted;
use crate::{
    durable_host::tests::fixture::Fixture,
    task_volume::{Query, TaskFiles},
};
use acyclic_stream::MemoryStream;
use acyclic_test_support::differential;
use std::sync::atomic::Ordering::SeqCst;

type Provider = LostSessionAck<MemoryStream>;
type Journal = FilesystemExecutionJournal<
    Provider,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;
const OP: OperationId = OperationId::from_bytes([2; 16]);
struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1234
    }
}

type State = (
    Fixture<Provider>,
    Journal,
    Arc<TaskFiles<Provider>>,
    ModelEvent,
);

fn working() -> ModelEvent {
    ModelEvent::Content {
        delta: "working".into(),
    }
}

/// The observation a recorded delta commits, for retries and stale appends.
fn observed(journal: &Journal, step: u32, sequence: u64) -> Result<ExecutionEvent> {
    Ok(ExecutionEvent::Model {
        step,
        purpose: crate::executor::ModelPurpose::Response,
        event: journal
            .delta_reference(OP, step, sequence, &working(), None)?
            .0,
    })
}

/// The task that owns `journal`'s chunks.
fn task_of<P, A, O>(journal: &FilesystemExecutionJournal<P, A, O>) -> crate::task_volume::Component
where
    P: StreamProvider,
{
    crate::task_volume::Component::new(journal.owner.as_ref().unwrap().0.task_id().to_string())
        .unwrap()
}

/// Commits `count` chunks from a peer execution whose UUID sorts after `OP`.
/// Measurements start at task sequence 10, so every measured chunk path at
/// N and 2N spells its sequence with the same two digits.
async fn publish_peers<P: StreamProvider>(
    stream: &P,
    files: &TaskFiles<P>,
    task: &crate::task_volume::Component,
    count: u64,
) {
    use acyclic_stream::{CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey};
    for _ in 0..count {
        let key = files
            .inner
            .next_chunk_key(task.clone(), OperationId::from_bytes([255; 16]), 0, None)
            .await
            .unwrap();
        let peer = files
            .inner
            .prepare_named_chunk(
                key.clone(),
                crate::task_volume::Channel::Text,
                b"peer",
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let request = CommitRequest {
            conditions: vec![peer.condition],
            mutations: vec![CommitMutation::Append {
                path: peer.path,
                records: vec![peer.record],
            }],
            idempotency_key: IdempotencyKey::new(format!("peer:{key:?}").into_bytes()).unwrap(),
        };
        assert!(matches!(
            stream.commit(request).await.unwrap(),
            CommitOutcome::Committed(_)
        ));
    }
}

/// The former private-file observation of the same delta.
async fn staged(journal: &Journal, key: &str) -> Result<ExecutionEvent> {
    Ok(ExecutionEvent::Model {
        step: 0,
        purpose: crate::executor::ModelPurpose::Response,
        event: crate::executor::stage_json(journal, OP, key, &working(), None).await?,
    })
}

async fn fixture() -> Result<State> {
    let stream = Provider {
        inner: MemoryStream::new_with_clock(Default::default(), Arc::new(Clock)),
        ..Default::default()
    };
    fixture_on(Arc::new(stream), acyclic_fs::Fs::memory()).await
}

/// The task-owned journal fixture on any stream and filesystem backend.
async fn fixture_on<P, A, O>(
    stream: Arc<P>,
    backend: acyclic_fs::Fs<A, O>,
) -> Result<(
    Fixture<P, A, O>,
    FilesystemExecutionJournal<P, A, O>,
    Arc<TaskFiles<P>>,
    ModelEvent,
)>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let f = Fixture::new(
        stream,
        backend,
        serde_json::json!({"type":"integer"}),
        Some(crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 2,
        }),
        &[],
    )
    .await?;
    let owner = f.running(1).await?;
    let (task, fence) = owner.task_binding();
    let request = crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            model: crate::model::Model::new(
                "synthetic".into(),
                "journal".into(),
                "1".into(),
                serde_json::json!({}),
            )?,
            messages: vec![crate::model::ModelMessage {
                role: crate::model::ModelRole::User,
                content: ModelContent::Text("hello".into()),
            }],
            tools: vec![],
            max_output_tokens: Some(32),
        },
        Limits::default(),
        None,
    )?;
    f.host
        .claim_model_dispatch(task, OP, 0, request.request_digest(), fence.clone())
        .await?;
    // Fix initial entropy independently on each side: native B-tree lookup
    // examinations depend on authenticated commit identities, including headers.
    f.stream
        .append(acyclic_stream::AppendRequest {
            path: acyclic_stream::StreamPath::new(format!(
                "harness/task-volumes/{}",
                f.storage.storage_name(&mut None)?
            ))?,
            records: vec![
                serde_json::to_vec(&serde_json::json!({
                    "volume":"06060606-0606-0606-0606-060606060606",
                    "epoch":"07070707-0707-0707-0707-070707070707",
                    "schema":crate::task_volume::SCHEMA,
                }))
                .unwrap()
                .into(),
            ],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let files = f.task_files().await?;
    let journal = f
        .execution(f.owner(1).await?, OP)?
        .with_task_files(Some(files.clone()));
    let reference = journal
        .stage(OP, "request", request.bytes(), "application/json", None)?
        .await?;
    journal
        .append(
            OP,
            "started",
            &ExecutionEvent::Started {
                request_digest: [9; 32],
            },
            None,
        )?
        .await?;
    journal
        .append(
            OP,
            "model",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: crate::executor::ModelPurpose::Response,
                request_digest: request.request_digest(),
                request: reference,
            },
            None,
        )?
        .await?;
    Ok((f, journal, files, working()))
}

// The journal call is the exact crash cut: no caller continuation runs.
async fn record(
    journal: &Journal,
    former: Option<&ExecutionEvent>,
    model: &ModelEvent,
    io: Option<&PreparationIo>,
) -> Result<()> {
    match former {
        Some(event) => journal
            .append_validated(OP, None, "event:100", event, None, io)
            .await
            .map(|_| ()),
        None => journal.append_delta(OP, "event:100", 0, model, io).await,
    }
}

async fn crash_cut(cut: u8, native: bool, broken: bool) -> Result<bool> {
    let (f, mut journal, files, model) = fixture().await?;
    // Former two-commit path: a privately staged delta, killed before any
    // separate chunk publication.
    let former = if broken {
        journal.task_files = None;
        Some(staged(&journal, "former").await?)
    } else {
        None
    };
    f.stream.arm_execution(
        2,
        match cut {
            0 => ExecutionFaultMode::Before,
            1 => ExecutionFaultMode::AfterVisible,
            _ => ExecutionFaultMode::AfterHidden,
        },
    );
    let result = if native {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, 1 << 28),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let journal = f
            .native_execution(1, OP, &io)
            .await?
            .with_task_files(if broken { None } else { Some(files.clone()) });
        f.stream.native_only.store(true, SeqCst);
        let result = record(&journal, former.as_ref(), &model, Some(&io)).await;
        f.stream.native_only.store(false, SeqCst);
        result
    } else {
        record(&journal, former.as_ref(), &model, None).await
    };
    if let Err(error) = result {
        assert!(matches!(error, Error::Indeterminate(_)), "{error:?}");
    }
    drop(journal);
    f.stream.hide_receipt.store(false, SeqCst);
    let mut reopened = f.execution(f.owner(1).await?, OP)?;
    // The former observation addresses its private file.
    reopened.task_files = (!broken).then(|| files.clone());
    let before = files.inner.snapshot(&Query::Root, 16).await?;
    let records = reopened.replay(OP, 0, 8, None)?.await?;
    let committed = records.len() == 3;
    let complete = committed == (before.files.len() == 1);
    if !broken {
        assert_eq!(committed, cut != 0);
        if committed {
            assert_eq!(before.files[0].bytes.as_ref(), b"working");
            let path = reopened.path(OP)?;
            let mut raw = path.read(2, 1).await?;
            use futures::TryStreamExt as _;
            let canonical = raw.try_next().await?.unwrap();
            let envelope = f.stream.inner.read_commit(canonical.commit_id).await?;
            assert_eq!(
                envelope.mutations.len(),
                2,
                "event and chunk must share the commit"
            );
        }
        reopened
            .append_delta(OP, "event:100", 0, &model, None)
            .await?;
        let after = files.inner.snapshot(&Query::Root, 16).await?;
        assert_eq!(after.files.len(), 1);
        let tail = after.generation;
        reopened
            .append_delta(OP, "event:100", 0, &model, None)
            .await?;
        assert_eq!(
            files.inner.head().await?,
            tail,
            "exact retry republished its chunk"
        );
        let event = observed(&reopened, 0, 0)?;
        assert!(
            !reopened
                .append_if_tail(OP, 0, "rejected:101", &event, None)?
                .await?
        );
        assert!(
            reopened
                .append_delta(OP, "invalid:102", 1, &model, None)
                .await
                .is_err()
        );
        // A committed chunk names exactly one observation.
        assert!(
            reopened
                .append(OP, "again:103", &event, None)?
                .await
                .is_err()
        );
        assert_eq!(
            files.inner.head().await?,
            tail,
            "rejected append published a chunk"
        );
        assert_eq!(reopened.replay(OP, 0, 8, None)?.await?.len(), 3);
    }
    Ok(complete)
}

#[test]
fn chunk_transaction_survives_each_crash_cut() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for cut in 0..3 {
        for native in [false, true] {
            assert!(runtime.block_on(crash_cut(cut, native, false)).unwrap());
        }
    }
}

#[tokio::test]
async fn negative_control_split_commit_loses_the_chunk() {
    assert!(!crash_cut(2, false, true).await.unwrap());
}

#[tokio::test]
async fn task_response_delta_has_no_private_file_path() {
    let (f, journal, files, _) = fixture().await.unwrap();
    let head = files.inner.head().await.unwrap();
    let mut former = f.execution(f.owner(1).await.unwrap(), OP).unwrap();
    former.task_files = None;
    let event = staged(&former, "former").await.unwrap();
    let error = journal.append(OP, "former", &event, None).unwrap().await;
    assert!(
        format!("{error:?}").contains("must be recorded as its live chunk"),
        "{error:?}"
    );
    // A reference to a chunk nobody committed is not content.
    let forged = observed(&journal, 0, 7).unwrap();
    let error = journal.append(OP, "forged", &forged, None).unwrap().await;
    assert!(format!("{error:?}").contains("not published"), "{error:?}");
    assert_eq!(files.inner.head().await.unwrap(), head);
    assert_eq!(
        journal.replay(OP, 0, 8, None).unwrap().await.unwrap().len(),
        2
    );
    // Negative control: without task files the same private event is accepted.
    former
        .append(OP, "former", &event, None)
        .unwrap()
        .await
        .unwrap();
}

async fn warm() -> Result<State> {
    let (f, mut journal, files, model) = fixture().await?;
    journal.task_files = None;
    for ordinal in 0..8 {
        let event = staged(&journal, &format!("warm:{ordinal}")).await?;
        journal
            .append(OP, &format!("warm:{ordinal}"), &event, None)?
            .await?;
    }
    journal.task_files = Some(files.clone());
    publish_peers(f.stream.as_ref(), &files, &task_of(&journal), 8).await;
    // Both bounded prefix slots belong to setup, so every measured anchor
    // uses the same production suffix cursor (including its exact allocation).
    journal
        .append_delta(OP, "event:99", 0, &model, None)
        .await?;
    journal
        .append_delta(OP, "event:100", 0, &model, None)
        .await?;
    Ok((f, journal, files, model))
}

async fn deltas(
    state: State,
    count: usize,
    io: Option<&PreparationIo>,
) -> Result<(Vec<Vec<u8>>, Vec<crate::task_volume::FileValue>, String)> {
    let (f, journal, files, model) = state;
    let journal = match io {
        Some(io) => f
            .native_execution(1, OP, io)
            .await?
            .with_task_files(Some(files.clone())),
        None => journal,
    };
    f.stream.reset_io();
    // The deliberately old snapshot scan uses ordinary I/O. The candidate
    // and payload-copy control must use only original native provider calls.
    f.stream
        .native_only
        .store(io.is_some() && !accounted::chunk_scan_enabled(), SeqCst);
    for index in 101..101 + count as u64 {
        let key = format!("event:{index}");
        accounted::observe_append(journal.append_delta(OP, &key, 0, &model, io), io).await?;
        differential::record(("commit.accept", f.stream.io_snapshot().writes));
        // Exact retries do no projection work.
        accounted::observe_append(journal.append_delta(OP, &key, 0, &model, io), io).await?;
        differential::record(("commit.retry", f.stream.io_snapshot().writes));
        // Setup published task-wide chunk sequences 0..=9 (eight peer
        // chunks, then events 99 and 100).
        let event = observed(&journal, 0, index - 91)?;
        assert!(
            !accounted::observe_append(
                async {
                    journal
                        .append_if_tail(OP, 0, "rejected:999", &event, io)?
                        .await
                },
                io
            )
            .await?
        );
        differential::record(("commit.reject", f.stream.io_snapshot().writes));
    }
    let error =
        accounted::observe_append(journal.append_delta(OP, "invalid:999", 1, &model, io), io)
            .await
            .unwrap_err();
    differential::record(("commit.invalid", f.stream.io_snapshot().writes));
    f.stream.native_only.store(false, SeqCst);
    let writes = f.stream.io_snapshot();
    assert_eq!(writes.writes, count as u64);
    assert_eq!(
        writes.records,
        2 * count as u64,
        "each delta needs exactly one atomic pair"
    );
    let records = journal.replay(OP, 12, 64, None)?.await?;
    let mut bytes = Vec::new();
    for record in records {
        let ExecutionEvent::Model { event, .. } = record.event else {
            unreachable!()
        };
        bytes.push(journal.load(&event, None)?.await?.as_ref().to_vec());
    }
    use futures::TryStreamExt as _;
    let mut canonical = journal.path(OP)?.read(0, 64).await?;
    while let Some(record) = canonical.try_next().await? {
        let mut authenticated = record.commit_id.as_bytes().to_vec();
        authenticated.extend_from_slice(&record.value);
        bytes.push(authenticated);
    }
    let snapshot = files.inner.snapshot(&Query::Root, 64).await?;
    // Eight peer chunks, then the two setup deltas and every measured one.
    assert_eq!(snapshot.files.len(), count + 10);
    for (index, file) in snapshot.files.iter().enumerate() {
        let expected: &[u8] = if index < 8 { b"peer" } else { b"working" };
        assert_eq!(file.bytes.as_ref(), expected);
    }
    Ok((bytes, snapshot.files, error.to_string()))
}

fn semantic_trace(trace: &[(&'static str, u64)]) -> Vec<(&'static str, u64)> {
    trace
        .iter()
        .copied()
        .filter(|(operation, _)| matches!(*operation, "read" | "verify" | "retain"))
        .collect()
}

fn totals(trace: &[(&'static str, u64)]) -> [u64; 7] {
    [
        "chunk.items",
        "chunk.allocations",
        "chunk.allocated-bytes",
        "chunk.native-allocations",
        "chunk.native-copy",
        "chunk.native-encoding",
        "chunk.native-hash",
    ]
    .map(|name| {
        trace
            .iter()
            .filter(|(operation, _)| *operation == name)
            .map(|(_, value)| value)
            .sum()
    })
}

#[test]
fn chunk_projection_has_exact_linear_work_and_allocations() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Measure both modes before asserting, so a failure reports every count.
    let modes = [false, true].map(|native| {
        let mut measured = Vec::new();
        for count in [8, 16] {
            let [baseline, candidate] = differential::pair(
                || runtime.block_on(warm()).unwrap(),
                |state, old| {
                    accounted::with_chunk_scan(old, || {
                        if native {
                            accounted::accounted(true, 1 << 28, false, |io| {
                                runtime.block_on(deltas(state, count, Some(io))).unwrap()
                            })
                            .unwrap()
                            .0
                        } else {
                            runtime.block_on(deltas(state, count, None)).unwrap()
                        }
                    })
                },
            );
            assert_eq!(
                baseline.0, candidate.0,
                "prefix scan changed bytes or exact errors"
            );
            assert_eq!(semantic_trace(&baseline.1), semantic_trace(&candidate.1));
            let metric = totals(&candidate.1);
            let broken = totals(&baseline.1);
            eprintln!("chunk native={native} N={count} exact={metric:?}");
            measured.push((metric, broken));
        }
        measured
    });
    for (native, measured) in [false, true].into_iter().zip(modes) {
        for (count, &(metric, broken)) in [8, 16].into_iter().zip(&measured) {
            assert_eq!(
                metric[0],
                6 * count as u64,
                "one record read, one sequence lookup and four index operations per chunk"
            );
            let n = count as u64;
            // Per delta: SDK allocations and bytes (the native side retains
            // its read allocation and ledger key with each chunk record).
            let (allocations, bytes) = if native { NATIVE_CHUNK } else { (23, 2_812) };
            assert_eq!(metric[1], n * allocations, "native={native}");
            assert_eq!(metric[2], n * bytes, "native={native}");
            if native {
                assert_eq!(metric[3], n * 17);
                assert_eq!(metric[4], n * 547);
                assert_eq!(metric[5], n * 348);
                assert_eq!(metric[6], n * 1_592);
                assert!(metric[6] > 0);
            } else {
                assert_eq!(&metric[3..], &[0, 0, 0, 0]);
            }
            assert!(metric[1] > 0 && metric[2] > 0);
            if native {
                assert!(metric[3] > 0 && metric[4] > 0 && metric[5] > 0);
            }
            assert_ne!(
                broken, metric,
                "quadratic publication passed the work/allocation guarantee"
            );
        }
        for index in 0..7 {
            assert_eq!(
                measured[1].0[index],
                2 * measured[0].0[index],
                "native={native}, metric={index}"
            );
        }
        assert_ne!(
            measured[1].1[0],
            2 * measured[0].1[0],
            "quadratic work escaped N/2N assertion"
        );
        assert_ne!(
            measured[1].1[1],
            2 * measured[0].1[1],
            "quadratic allocations escaped N/2N assertion"
        );
    }
}

#[test]
fn projection_payload_copy_negative_control_has_exact_extra_allocations() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for native in [false, true] {
        let [baseline, candidate] = differential::pair(
            || runtime.block_on(warm()).unwrap(),
            |state, copied| {
                accounted::with_chunk_copy(copied, || {
                    if native {
                        accounted::accounted(true, 1 << 28, false, |io| {
                            runtime.block_on(deltas(state, 8, Some(io))).unwrap()
                        })
                        .unwrap()
                        .0
                    } else {
                        runtime.block_on(deltas(state, 8, None)).unwrap()
                    }
                })
            },
        );
        assert_eq!(baseline.0, candidate.0);
        assert_eq!(semantic_trace(&baseline.1), semantic_trace(&candidate.1));
        let old = totals(&baseline.1);
        let new = totals(&candidate.1);
        eprintln!("copy negative native={native} old={old:?} candidate={new:?}");
        assert_eq!(old[0], new[0]);
        assert_eq!(old[1] - new[1], if native { 16 } else { 8 });
        assert_eq!(new[1], 8 * if native { NATIVE_CHUNK.0 } else { 23 });
        if native {
            assert_eq!(old[3] - new[3], 16);
            assert_eq!(old[4] - new[4], 8 * b"working".len() as u64);
            assert_eq!(old[5], new[5]);
            assert_eq!(old[6], new[6]);
        }
        assert!(old[2] > new[2]);
    }
}

#[test]
fn chunk_projection_preserves_the_bound_on_its_authorized_task_files() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for maximum in [6, 7] {
        for native in [false, true] {
            let (f, mut journal, files, model) = runtime.block_on(fixture()).unwrap();
            let limited = runtime
                .block_on(TaskFiles::open(
                    f.stream.clone(),
                    files.volume(),
                    &journal.verifier,
                    &journal.scope,
                    maximum,
                ))
                .unwrap();
            let limited = Arc::new(limited);
            journal.task_files = Some(limited.clone());
            let before = runtime.block_on(files.inner.head()).unwrap();
            let appended = if native {
                accounted::accounted(true, 1 << 28, false, |io| {
                    runtime.block_on(async {
                        let journal = f
                            .native_execution(1, OP, io)
                            .await
                            .unwrap()
                            .with_task_files(Some(limited.clone()));
                        journal
                            .append_delta(OP, "bounded:100", 0, &model, Some(io))
                            .await
                    })
                })
                .unwrap()
                .0
            } else {
                runtime.block_on(journal.append_delta(OP, "bounded:100", 0, &model, None))
            };
            assert_eq!(appended.is_ok(), maximum == 7, "{appended:?}");
            let after = runtime
                .block_on(files.inner.snapshot(&Query::Root, 4))
                .unwrap();
            assert_eq!(after.files.len(), usize::from(maximum == 7));
            let records = runtime
                .block_on(journal.replay(OP, 0, 8, None).unwrap())
                .unwrap();
            assert_eq!(records.len(), if maximum == 7 { 3 } else { 2 });
            if maximum == 6 {
                assert_eq!(after.generation, before);
            }
        }
    }
}

#[test]
fn entire_append_has_exact_linear_work_with_prefix_replay_negative() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let names = [
        "append.calls",
        "append.items",
        "append.allocations",
        "append.allocated-bytes",
        "append.native-allocations",
        "append.native-copy",
        "append.native-encoding",
        "append.native-hash",
        "append.native-items",
    ];
    // Whole-append physical and original-ledger receipts. N/2N includes
    // fixed initial replay and the real index/map splits; no projection gap.
    let expected = [
        [
            [25, 161, 3986, 1006314, 0, 0, 0, 0, 0],
            [49, 313, 7761, 1954978, 0, 0, 0, 0, 0],
        ],
        [
            [25, 163, 8338, 4249318, 6897, 188645, 188833, 164739, 6393],
            [
                49, 315, 13305, 7651650, 11051, 283977, 318001, 281359, 12209,
            ],
        ],
    ];
    let mut modes = Vec::new();
    for native in [false, true] {
        let mut measured = Vec::new();
        let mut negatives = Vec::new();
        for count in [8, 16] {
            let [baseline, candidate] = differential::pair(
                || runtime.block_on(warm()).unwrap(),
                |state, old| {
                    accounted::with_append_scan(old, || {
                        if native {
                            accounted::accounted(true, 1 << 28, false, |io| {
                                runtime.block_on(deltas(state, count, Some(io))).unwrap()
                            })
                            .unwrap()
                            .0
                        } else {
                            runtime.block_on(deltas(state, count, None)).unwrap()
                        }
                    })
                },
            );
            assert_eq!(
                baseline.0, candidate.0,
                "full bytes, task files and exact errors changed"
            );
            let commits = |trace: &Vec<(&'static str, u64)>| {
                trace
                    .iter()
                    .copied()
                    .filter(|(name, _)| name.starts_with("commit."))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                commits(&baseline.1),
                commits(&candidate.1),
                "committed operation trace changed"
            );
            let sum = |trace: &Vec<(&'static str, u64)>| {
                names.map(|name| {
                    trace
                        .iter()
                        .filter(|(key, _)| *key == name)
                        .map(|(_, n)| *n)
                        .sum::<u64>()
                })
            };
            let actual = sum(&candidate.1);
            let broken = sum(&baseline.1);
            eprintln!("append native={native} N={count} actual={actual:?} negative={broken:?}");
            assert_eq!(actual[0], 3 * count as u64 + 1);
            assert_ne!(
                actual, broken,
                "full prefix replay passed whole-append work detector"
            );
            negatives.push(broken);
            measured.push(actual);
        }
        modes.push((measured, negatives));
    }
    for (mode, (measured, negatives)) in modes.into_iter().enumerate() {
        let native = mode == 1;
        let expected = expected[mode];
        assert_eq!(measured.as_slice(), &expected);
        for metric in 0..9 {
            assert_eq!(
                i128::from(measured[1][metric]) - 2 * i128::from(measured[0][metric]),
                i128::from(expected[1][metric]) - 2 * i128::from(expected[0][metric]),
            );
        }
        // The same N/2N allocation assertion rejects the separately initialized
        // full-prefix production-path control, even with identical outputs.
        assert_ne!(
            i128::from(negatives[1][2]) - 2 * i128::from(negatives[0][2]),
            i128::from(expected[1][2]) - 2 * i128::from(expected[0][2]),
        );
        if native {
            assert_ne!(
                i128::from(negatives[1][8]) - 2 * i128::from(negatives[0][8]),
                i128::from(expected[1][8]) - 2 * i128::from(expected[0][8]),
            );
        }
    }
}

/// Native allocations and bytes retained per chunk: Darwin's native read
/// holds one more 64-byte allocation per record than Linux and Windows.
const NATIVE_CHUNK: (u64, u64) = if cfg!(target_os = "macos") {
    (25, 3_078)
} else {
    (24, 3_014)
};

mod ordering;
mod publication;
