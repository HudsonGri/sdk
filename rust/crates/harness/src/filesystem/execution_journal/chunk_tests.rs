//! Real task-owned journal crash cuts and bounded chunk projection work.
use super::*;
use crate::runtime::DurableTaskHost;
use crate::test_stream::{ExecutionFaultMode, LostSessionAck};
use crate::test_support::differential;
use crate::{
    durable_host::tests::fixture::Fixture,
    task_volume::{Query, TaskFiles},
};
use acyclic_stream::MemoryStream;

type Provider = LostSessionAck<MemoryStream>;
type Journal = FilesystemExecutionJournal<
    Provider,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;
const OP: OperationId = OperationId::from_bytes([2; 16]);

async fn fixture() -> Result<(
    Fixture<Provider>,
    Journal,
    Arc<TaskFiles<Provider>>,
    ExecutionEvent,
)> {
    let f = Fixture::new(
        Arc::new(Provider::default()),
        acyclic_fs::Fs::memory(),
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
    let reference = crate::executor::stage_json(
        &journal,
        OP,
        "delta",
        &ModelEvent::Content {
            delta: "working".into(),
        },
        None,
    )
    .await?;
    let event = ExecutionEvent::Model {
        step: 0,
        purpose: crate::executor::ModelPurpose::Response,
        event: reference,
    };
    Ok((f, journal, files, event))
}

async fn crash_cut(cut: u8, native: bool, broken: bool) -> Result<bool> {
    use std::sync::atomic::Ordering::SeqCst;
    let (f, mut journal, files, event) = fixture().await?;
    if broken {
        journal.task_files = None;
    } // Former two-commit path, killed before publication.
    f.stream.arm_execution(
        2,
        match cut {
            0 => ExecutionFaultMode::Before,
            1 => ExecutionFaultMode::AfterVisible,
            _ => ExecutionFaultMode::AfterHidden,
        },
    );
    // append_validated is the exact old crash cut: no caller continuation runs.
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
        let result = journal
            .append_validated(OP, None, "event:100", &event, Some(&io))
            .await;
        f.stream.native_only.store(false, SeqCst);
        result
    } else {
        journal
            .append_validated(OP, None, "event:100", &event, None)
            .await
    };
    if let Err(error) = result {
        assert!(matches!(error, Error::Indeterminate(_)), "{error:?}");
    }
    drop(journal);
    f.stream.hide_receipt.store(false, SeqCst);
    let reopened = f
        .execution(f.owner(1).await?, OP)?
        .with_task_files(Some(files.clone()));
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
        reopened.append(OP, "event:100", &event, None)?.await?;
        let after = files.inner.snapshot(&Query::Root, 16).await?;
        assert_eq!(after.files.len(), 1);
        let tail = after.generation;
        reopened.append(OP, "event:100", &event, None)?.await?;
        assert_eq!(
            files.inner.head().await?,
            tail,
            "exact retry republished its chunk"
        );
        assert!(
            !reopened
                .append_if_tail(OP, 0, "rejected:101", &event, None)?
                .await?
        );
        let mut invalid = event.clone();
        if let ExecutionEvent::Model { step, .. } = &mut invalid {
            *step = 1;
        }
        assert!(
            reopened
                .append(OP, "invalid:102", &invalid, None)?
                .await
                .is_err()
        );
        assert_eq!(
            files.inner.head().await?,
            tail,
            "rejected append published a chunk"
        );
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
async fn warm() -> Result<(
    Fixture<Provider>,
    Journal,
    Arc<TaskFiles<Provider>>,
    ExecutionEvent,
)> {
    let (f, mut journal, files, event) = fixture().await?;
    journal.task_files = None;
    for ordinal in 0..8 {
        journal
            .append(OP, &format!("warm:{ordinal}"), &event, None)?
            .await?;
    }
    journal.task_files = Some(files.clone());
    // Both bounded prefix slots belong to setup, so every measured anchor
    // uses the same production suffix cursor (including its exact allocation).
    journal.append(OP, "event:99", &event, None)?.await?;
    journal.append(OP, "event:100", &event, None)?.await?;
    Ok((f, journal, files, event))
}

async fn deltas(
    state: (
        Fixture<Provider>,
        Journal,
        Arc<TaskFiles<Provider>>,
        ExecutionEvent,
    ),
    count: usize,
    io: Option<&PreparationIo>,
) -> Result<(Vec<Vec<u8>>, Vec<crate::task_volume::FileValue>, String)> {
    let (f, journal, files, event) = state;
    let journal = match io {
        Some(io) => f
            .native_execution(1, OP, io)
            .await?
            .with_task_files(Some(files.clone())),
        None => journal,
    };
    f.stream.reset_io();
    for index in 101..101 + count {
        let key = format!("event:{index}");
        journal.append(OP, &key, &event, io)?.await?;
        journal.append(OP, &key, &event, io)?.await?; // Exact retries do no projection work.
        assert!(
            !journal
                .append_if_tail(OP, 0, "rejected:999", &event, io)?
                .await?
        );
    }
    let mut invalid = event.clone();
    if let ExecutionEvent::Model { step, .. } = &mut invalid {
        *step = 1;
    }
    let error = journal
        .append(OP, "invalid:999", &invalid, io)?
        .await
        .unwrap_err();
    let writes = f.stream.io_snapshot();
    assert_eq!(writes.writes, count as u64);
    assert_eq!(
        writes.records,
        2 * count as u64,
        "each delta needs exactly one atomic pair"
    );
    let records = journal.replay(OP, 2, 64, None)?.await?;
    let mut bytes = Vec::new();
    for record in records {
        let ExecutionEvent::Model { event, .. } = record.event else {
            unreachable!()
        };
        bytes.push(journal.load(&event, None)?.await?.as_ref().to_vec());
    }
    let snapshot = files.inner.snapshot(&Query::Root, 64).await?;
    assert_eq!(snapshot.files.len(), count + 2);
    for file in &snapshot.files {
        assert_eq!(file.bytes.as_ref(), b"working");
    }
    Ok((bytes, snapshot.files, error.to_string()))
}

fn totals(trace: &[(&'static str, u64)]) -> [u64; 6] {
    [
        "chunk.items",
        "chunk.allocations",
        "chunk.allocated-bytes",
        "chunk.native-allocations",
        "chunk.native-copy",
        "chunk.native-encoding",
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
    for native in [false, true] {
        let mut measured = Vec::new();
        for count in [8, 16] {
            let [baseline, candidate] = differential::pair(
                || runtime.block_on(warm()).unwrap(),
                |state, old| {
                    differential::with_chunk_scan(old, || {
                        if native {
                            differential::accounted(true, 1 << 28, false, |io| {
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
            let metric = totals(&candidate.1);
            let broken = totals(&baseline.1);
            assert_eq!(metric[0], count as u64, "one anchor record per new chunk");
            assert!(metric[1] > 0 && metric[2] > 0);
            if native {
                assert!(metric[3] > 0 && metric[4] > 0 && metric[5] > 0);
            }
            assert_ne!(
                broken, metric,
                "quadratic publication passed the work/allocation guarantee"
            );
            measured.push((metric, broken));
        }
        for index in 0..6 {
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
