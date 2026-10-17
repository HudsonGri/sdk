use super::*;
use crate::{
    durable_host::tests::fixture::Fixture,
    filesystem::{FilesystemExecutionJournal, FilesystemJournalBinding},
    task_volume::{Component, FileKey, Query, TaskFiles, Write},
};
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_stream::MemoryStream;
use proptest::prelude::*;
use serde_json::json;
use std::future::Future as _;
use std::{cell::RefCell, rc::Rc, task::Poll};

#[derive(Default)]
struct Input {
    next: Option<ModelEvent>,
    parked: u64,
    done: bool,
}

async fn run(deltas: &[String], rewrite: bool) -> Result<bool> {
    let stream = Arc::new(MemoryStream::default());
    let fixture = Fixture::new(
        stream.clone(),
        acyclic_fs::Fs::memory(),
        json!({"type":"integer"}),
        Some(crate::scheduler::SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        }),
        &[],
    )
    .await?;
    fixture.running(1).await?;
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let task = crate::TaskId::from_bytes([1; 16]);
    let fence = fixture.owner(1).await?.task_binding().1.clone();
    let operation = OperationId::from_bytes([2; 16]);
    let files = Arc::new(
        TaskFiles::open(
            stream,
            &fixture.storage,
            &fixture.issuer.verifier(&mut None)?,
            fixture.scope(),
            65_536,
        )
        .await?,
    );
    let journal = FilesystemExecutionJournal::for_task(
        fixture.host.journal_owner(task, &fence, Some(&io)).await?,
        operation,
        operation,
        fixture.files.clone(),
        FilesystemJournalBinding {
            volume: &fixture.storage,
            verifier: &fixture.issuer.verifier(&mut None)?,
            scope: fixture.scope(),
            maximum_bytes: 65_536,
            io: Some(&io),
        },
    )?
    .with_task_files(files.clone());
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            Some(&io),
        )?
        .await?;
    let model = Model::new(
        "scripted".into(),
        "delta-work".into(),
        "1".into(),
        Value::Null,
    )?;
    let request = crate::model::PreparedModelRequest::prepare(
        crate::model::ModelRequest {
            model: model.clone(),
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("p".into()),
            }],
            tools: vec![],
            max_output_tokens: None,
        },
        Limits::default(),
        Some(&io),
    )?;
    fixture
        .host
        .claim_model_dispatch(task, operation, 0, request.request_digest(), fence)
        .await?;
    let reference = stage_bytes(
        &journal,
        operation,
        "request",
        request.bytes(),
        true,
        Some(&io),
    )
    .await?;
    journal
        .append(
            operation,
            "claim",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Response,
                request_digest: request.request_digest(),
                request: reference,
            },
            Some(&io),
        )?
        .await?;
    let stock = StockExecutor::new(
        model,
        Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let mut execution = StockExecution::ordinary(&stock);
    execution.io = Some(&io);
    let input = Rc::new(RefCell::new(Input::default()));
    let source = input.clone();
    let incoming = futures::stream::poll_fn(move |_| {
        let mut source = source.borrow_mut();
        if let Some(event) = source.next.take() {
            Poll::Ready(Some(Ok(event)))
        } else if source.done {
            Poll::Ready(None)
        } else {
            source.parked += 1;
            Poll::Pending
        }
    });
    let mut admission = ModelEventAdmission::default();
    let recording = execution.record_model_events(
        &journal,
        operation,
        (0, ModelPurpose::Response, "model"),
        &mut admission,
        incoming,
        false,
    );
    let _frame = work.claim(
        std::mem::size_of_val(&recording) as u64,
        1,
        acyclic_fs::WorkCounters::default(),
    )?;
    let mut recording = std::pin::pin!(recording);
    let mut follow = files
        .observe(Query::Task(Component::new(task.to_string())?), None, 16)
        .await?;
    let mut prefix = String::new();
    for (index, delta) in deltas.iter().enumerate() {
        let encoded = serde_json::to_vec(&if index % 2 == 0 {
            ModelEvent::Content {
                delta: delta.clone(),
            }
        } else {
            ModelEvent::Reasoning {
                delta: delta.clone(),
            }
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let (event, custody) = ModelEvent::decode_with_native_work(&encoded, &work)?;
        io.absorb_native_custody(custody)?;
        input.borrow_mut().next = Some(event);
        let parked = input.borrow().parked;
        let before = work.receipt();
        let mut result = None;
        let allocations = acyclic_test_support::measure(|| {
            result =
                Some(futures::executor::block_on(async {
                    futures::future::poll_fn(|cx| {
                        let polled =
                            std::pin::Pin::new(&mut tokio::task::unconstrained(recording.as_mut()))
                                .poll(cx);
                        if let Poll::Ready(result) = polled {
                            return Poll::Ready(result.and_then(|_| {
                                Err(Error::Invalid("model stream ended early".into()))
                            }));
                        }
                        if input.borrow().parked > parked {
                            Poll::Ready(Ok(()))
                        } else {
                            Poll::Pending
                        }
                    })
                    .await?;
                    if rewrite {
                        prefix.push_str(delta);
                        let generation = files.inner.head().await?;
                        files
                            .inner
                            .append_if(
                                generation,
                                &Write::new(FileKey::Goal, prefix.as_bytes().to_vec()),
                                vec![],
                                None,
                                Some(&work),
                            )
                            .await?;
                    }
                    let file = loop {
                        let change = follow
                            .changes
                            .next()
                            .await
                            .ok_or_else(|| Error::Storage("delta follow ended".into()))??;
                        if let Some(file) = change.file {
                            break file;
                        }
                    };
                    if file.bytes.as_ref() != delta.as_bytes() {
                        return Err(Error::Invalid("delta file changed".into()));
                    }
                    Ok::<_, Error>(())
                }));
        });
        result.expect("measured the actual stock loop")?;
        let after = work.receipt();
        let copied = after.bytes_copied - before.bytes_copied;
        let operations = (after.backend_read_operations - before.backend_read_operations)
            + (after.backend_write_operations - before.backend_write_operations)
            + (after.durability_operations - before.durability_operations);
        let bounded = copied <= 4 * delta.len() as u64 + 16_384
            && allocations.bytes_total <= 16 * delta.len() as u64 + 262_144
            && allocations.count_total <= 2048
            && operations <= 128;
        if !bounded {
            if rewrite {
                return Ok(false);
            }
            return Err(Error::Invalid(format!(
                "delta {index}: copied={copied}, allocations={allocations:?}, operations={operations}"
            )));
        }
    }
    input.borrow_mut().done = true;
    futures::executor::block_on(recording.as_mut())?;
    Ok(true)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn stock_delta_publication_work_ignores_the_retained_prefix(deltas in prop::collection::vec("[abλ雪]{8,128}", 2..32)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        prop_assert!(runtime.block_on(run(&deltas, false)).map_err(|error| TestCaseError::fail(error.to_string()))?);
    }

    #[test]
    fn rewriting_the_whole_file_fails_the_stock_delta_work_oracle(count in 16usize..24, length in 2048usize..4096) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let deltas = vec!["x".repeat(length); count];
        prop_assert!(runtime.block_on(run(&deltas, false)).map_err(|error| TestCaseError::fail(error.to_string()))?);
        prop_assert!(!runtime.block_on(run(&deltas, true)).map_err(|error| TestCaseError::fail(error.to_string()))?);
    }
}
