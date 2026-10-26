use super::*;
use proptest::prelude::*;

// This fixture defines one token per encoded byte, including expanded files.
// The admission path therefore has a directly observable request-size bound.
fn byte_count(
    request: &crate::model::PreparedModelRequest,
) -> Result<crate::context::ModelTokenCount> {
    let mut count = fixture_token_count(request)?;
    for message in &request.request().messages {
        for (file, policy) in message.content.file_parts() {
            if !matches!(policy, crate::model::FileProjectionPolicy::Reference) {
                count.input_tokens += file.descriptor().byte_length();
            }
        }
    }
    count.message_tokens = request
        .request()
        .messages
        .iter()
        .map(|message| {
            u32::try_from(crate::contract::canonical_json_bytes(&message.content)?.len())
                .map_err(|_| Error::Invalid("fixture message exceeds u32".into()))
        })
        .collect::<Result<_>>()?;
    Ok(count)
}

async fn check(
    history: &[String],
    current: &str,
    copies: usize,
    capacity: u32,
    automatic: bool,
) -> Result<usize> {
    let mut dispatched = 0;
    for entry in 0..3 {
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Text("summary or answer".into()),
                ScriptedTurn::Text("answer".into()),
            ])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: capacity,
                output_tokens: Some(256),
            })
            .with_token_counter(byte_count),
        );
        let context = crate::context::Context {
            messages: history
                .iter()
                .enumerate()
                .map(|(index, text)| ModelMessage {
                    role: if index % 2 == 0 {
                        ModelRole::User
                    } else {
                        ModelRole::Assistant
                    },
                    content: ModelContent::Text(text.repeat(copies)),
                })
                .collect(),
            ..Default::default()
        };
        let executor = StockExecutor::new(
            Model::new(
                "fixture".into(),
                "byte-bound".into(),
                "1".into(),
                Value::Null,
            )?,
            provider.clone(),
            ContextPipeline::new([Arc::new(crate::context::SourceStage::new(
                "history",
                "1",
                Arc::new(context),
                crate::context::ContextPlacement::Prepend,
            )) as Arc<dyn crate::context::ContextStage>]),
            ToolRegistry::default(),
        )
        .with_max_output_tokens(128)?
        .with_compaction_policy(if automatic {
            crate::context::CompactionPolicy::Threshold(
                crate::context::ThresholdCompaction::for_capacity(
                    crate::context::ModelContextCapacity {
                        context_tokens: capacity,
                        output_tokens: Some(256),
                    },
                    0,
                )?,
            )
        } else {
            crate::context::CompactionPolicy::Disabled
        });
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text(current.repeat(copies)),
            selected_context: None,
            max_steps: 1,
        };
        let journal = Journal::default();
        // Oversized mandatory input can be rejected; every actual dispatch,
        // including the summary dispatch, must satisfy the same bound.
        match entry {
            0 => {
                let _ = executor.execute(input.clone(), &journal).await;
            }
            1 => {
                let _ = executor.execute_progress(input.clone(), &journal).await;
            }
            _ => {
                let _ = executor.model_step(&journal, &input, 0, &[]).await;
            }
        }
        for request in provider.requests() {
            dispatched += 1;
            let output = request.max_output_tokens.unwrap();
            let prepared =
                crate::model::PreparedModelRequest::prepare(request, Limits::default(), None)?;
            assert!(
                byte_count(&prepared)?.input_tokens + u64::from(output) <= u64::from(capacity),
                "entry {entry} dispatched a request larger than its catalog capacity"
            );
        }
    }
    Ok(dispatched)
}

#[tokio::test]
async fn small_input_dispatches_and_oversized_mandatory_input_does_not() -> Result<()> {
    assert_eq!(check(&[], "hello", 1, 4096, true).await?, 3);
    assert_eq!(check(&[], &"x".repeat(16_384), 1, 4096, true).await?, 0);
    Ok(())
}

#[tokio::test]
async fn disabling_compaction_preserves_capacity_admission() -> Result<()> {
    assert_eq!(check(&[], "hello", 1, 4096, false).await?, 3);
    assert_eq!(check(&[], &"x".repeat(16_384), 1, 4096, false).await?, 0);
    Ok(())
}

#[tokio::test]
async fn disabling_compaction_rejects_missing_accounting_before_effects() -> Result<()> {
    for entry in 0..3 {
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("must not dispatch".into())])
                .without_accounting(),
        );
        let executor = uncompacted_executor(
            Model::new(
                "fixture".into(),
                "unknown-capacity".into(),
                "1".into(),
                Value::Null,
            )?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        let journal = Journal::default();
        let input = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text("hello".into()),
            selected_context: None,
            max_steps: 1,
        };
        let result = match entry {
            0 => executor.execute(input.clone(), &journal).await.map(|_| ()),
            1 => {
                let progress = executor.execute_progress(input.clone(), &journal).await?;
                assert!(matches!(&progress, ExecutionProgress::PreDispatchFailed(_)));
                progress.into_output().map(|_| ())
            }
            _ => executor
                .model_step(&journal, &input, 0, &[])
                .await
                .map(|_| ()),
        };
        assert!(
            matches!(result, Err(Error::Unsupported(_))),
            "entry {entry}: {result:?}"
        );
        assert!(provider.requests().is_empty(), "entry {entry}");
        assert!(journal.0.lock().unwrap().is_empty(), "entry {entry}");
        assert!(journal.1.lock().unwrap().is_empty(), "entry {entry}");
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn arbitrary_history_never_dispatches_past_the_selected_capacity(
        history in prop::collection::vec(".{0,64}", 0..20), current in ".{0,64}",
        copies in 1usize..64, capacity in 4_096u32..32_768, automatic in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().build().unwrap()
            .block_on(check(&history, &current, copies, capacity, automatic)).unwrap();
    }
}
