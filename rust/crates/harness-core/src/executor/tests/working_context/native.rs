use super::*;

struct PublishedCut {
    storage: crate::filesystem::MemoryHarnessStorage,
    executor: StockExecutor,
    input: TurnInput,
    retained: crate::context::Context,
}

async fn published_cut() -> Result<PublishedCut> {
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 20_000).await?;
    let model = Model::new(
        "synthetic".into(),
        "counter".into(),
        "1".into(),
        Value::Null,
    )?;
    let (provider, _) = counted_model(
        20_000,
        None,
        [
            ScriptedTurn::Text("reply".repeat(600)),
            ScriptedTurn::Text("bounded summary".into()),
            ScriptedTurn::Text("done".into()),
        ],
        [],
    );
    let bundle = storage
        .builder()
        .model(model.clone(), provider.clone())
        .grant("model:generate")
        .without_default_tools()
        .compaction(crate::context::CompactionPolicy::Threshold(
            crate::context::ThresholdCompaction {
                response_reserve_tokens: 4_096,
                recent_tokens: 1,
                ..Default::default()
            },
        ))
        .build()?;
    let first = OperationId::new();
    let old = storage
        .stage(
            first,
            "turns/old.txt",
            &vec![b'q'; 8_000],
            "text/plain",
            "old.txt",
        )
        .await?;
    storage
        .run_conversation(&bundle, first, old, vec![], 1)
        .await?;
    let operation = OperationId::new();
    let current = storage
        .stage(
            operation,
            "turns/current.txt",
            &vec![b'x'; 8_000],
            "text/plain",
            "current.txt",
        )
        .await?;
    storage
        .run_conversation(&bundle, operation, current, vec![], 1)
        .await?;
    let journal = storage.journal();
    let reference =
        canonical_checkpoint_for_operation(journal.as_ref(), operation, Limits::default(), None)
            .await?
            .ok_or_else(|| Error::Invalid("fixture did not compact".into()))?;
    let (envelope, retained) =
        load_canonical_checkpoint(journal.as_ref(), &reference, Limits::default(), None, None)
            .await?;
    let source = retain_preparation(
        load_json::<crate::context::Context>(journal.as_ref(), &envelope.source, None).await?,
    )?;
    let current_input = source
        .current_input_index
        .and_then(|index| source.messages.get(index as usize))
        .ok_or_else(|| Error::Invalid("fixture has no current input".into()))?
        .content
        .clone();
    let input = TurnInput {
        operation_id: operation,
        input: current_input,
        selected_context: Some(SelectedModelContext {
            selection: envelope.selection,
            messages: source.messages,
        }),
        max_steps: 2,
    };
    let read = storage
        .volume()
        .capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let executor = StockExecutor::new(
        model,
        provider,
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_tool_authority(
        RuntimeScope::new(Capabilities::new([read]), Limits::default())?,
        None,
    )?;
    Ok(PublishedCut {
        storage,
        executor,
        input,
        retained,
    })
}

#[tokio::test]
async fn finite_and_native_working_cuts_keep_their_original_custody_until_drop() -> Result<()> {
    let fixture = published_cut().await?;
    let journal = fixture.storage.journal();
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    for native in [false, true] {
        println!("working custody native={native}");
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let mut execution = StockExecution {
            io: Some(&io),
            ..StockExecution::ordinary(&fixture.executor)
        };
        let mut input = ContextInput::copy_for_turn(&fixture.input, 1, &[], None)?;
        let context = execution
            .canonical_base_for_turn(journal.as_ref(), fixture.input.operation_id, &mut input)
            .await?;
        assert_eq!(context, fixture.retained);
        // Native guards transfer custody out of `io`, but remain charged to `work`.
        let funded = if native {
            work.live_allocation_bytes()
        } else {
            io.owned_bytes()?
        };
        let cache = execution.working_context.take();
        assert!(
            matches!(&cache, Some(Some(_))),
            "checkpoint was not retained"
        );
        drop(cache);
        let remaining = if native {
            work.live_allocation_bytes()
        } else {
            io.owned_bytes()?
        };
        let result_bytes = io.owned_bytes()?;
        assert!(
            funded > remaining,
            "negative control omitted retained-cache custody"
        );
        assert!(result_bytes > 0, "returned context has no original funding");
        let output = io.guard_owned(result_bytes)?;
        drop(context);
        drop(output);
        assert_eq!(io.owned_bytes()?, 0, "checkpoint decoding retained scratch");
        drop(execution);
        drop(io);
    }
    drop(journal);
    drop(fixture);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn denied_or_cancelled_native_working_cuts_leave_no_cached_authority() -> Result<()> {
    let fixture = published_cut().await?;
    let journal = fixture.storage.journal();
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = PreparationIo::from_native_work(&work)?;
        let mut executor = fixture.executor.clone();
        if cancelled {
            cancellation.cancel();
        } else {
            executor.tool_scope = RuntimeScope::default();
        }
        let execution = StockExecution {
            io: Some(&io),
            ..StockExecution::ordinary(&executor)
        };
        let mut input = ContextInput::copy_for_turn(&fixture.input, 1, &[], None)?;
        let outcome = execution
            .canonical_base_for_turn(journal.as_ref(), fixture.input.operation_id, &mut input)
            .await;
        assert!(outcome.is_err());
        assert!(execution.working_context.get().is_none());
        drop(outcome);
        drop(execution);
        assert_eq!(io.owned_bytes()?, 0);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

// A native counter funds the returned token DTO on the caller's original work.
// Generation is deliberately ordinary-only: the cancellation must stop before it.
struct NativeCompactionCounter(ScriptedModelProvider);

impl crate::model::ModelProvider for NativeCompactionCounter {
    fn context_capacity(&self, model: &Model) -> Result<crate::context::ModelContextCapacity> {
        self.0.context_capacity(model)
    }
    fn count_tokens<'a>(
        &'a self,
        request: &'a crate::model::PreparedModelRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<crate::context::ModelTokenCount>> {
        self.0.count_tokens(request)
    }
    fn generate<'a>(
        &'a self,
        request: &'a crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        self.0.generate(request, dispatch)
    }
    fn supports_native_model(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }
    fn context_capacity_with_native_work(
        &self,
        model: &Model,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::context::ModelContextCapacity> {
        work.remaining()?;
        self.0.context_capacity(model)
    }
    fn count_tokens_with_native_work<'a>(
        &'a self,
        request: &'a crate::model::PreparedModelRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                crate::context::ModelTokenCount,
                acyclic_fs::NativeAllocation,
            ),
        >,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            let bytes = std::mem::size_of::<crate::context::ModelTokenCount>() as u64
                + request.request().messages.len() as u64 * 4;
            let custody = work.claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)?;
            Ok((self.0.count_tokens(request).await?, custody))
        })
    }
}

#[tokio::test]
async fn fresh_compaction_keeps_cancellation_on_the_original_owner() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let trigger = cancellation.clone();
    let counts = Arc::new(AtomicUsize::new(0));
    let observed = counts.clone();
    let capacity = crate::context::ModelContextCapacity {
        context_tokens: 20_000,
        output_tokens: Some(4_096),
    };
    let provider = Arc::new(NativeCompactionCounter(
        ScriptedModelProvider::new([ScriptedTurn::Text("bounded summary".into())])
            .with_context_capacity(capacity)
            .with_token_counter(move |request| {
                // Cancel inside fresh compaction, after its caller selects the owner.
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    trigger.cancel();
                }
                let tokens = request
                    .request()
                    .messages
                    .iter()
                    .map(|message| match &message.content {
                        ModelContent::Text(text) => u32::try_from(text.len()).unwrap(),
                        _ => 0,
                    })
                    .collect();
                Ok(crate::context::ModelTokenCount::from_upper_bounds(
                    request.request_digest(),
                    100,
                    tokens,
                ))
            }),
    ));
    let executor = StockExecutor::new(
        Model::new(
            "synthetic".into(),
            "counter".into(),
            "1".into(),
            Value::Null,
        )?,
        provider,
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(crate::context::CompactionPolicy::Threshold(
        crate::context::ThresholdCompaction {
            response_reserve_tokens: 4_096,
            recent_tokens: 1,
            ..Default::default()
        },
    ));
    let messages = vec![
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("q".repeat(8_000)),
        },
        ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("a".repeat(3_000)),
        },
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("x".repeat(8_000)),
        },
    ];
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: messages.last().unwrap().content.clone(),
        selected_context: Some(SelectedModelContext {
            selection: crate::conversation::ModelContextSelection {
                conversation_revision: 3,
                message_ids: (1..=3).map(uuid::Uuid::from_u128).collect(),
                checkpoint: None,
            },
            messages,
        }),
        max_steps: 2,
    };
    let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let execution = StockExecution {
        io: Some(&io),
        ..StockExecution::ordinary(&executor)
    };
    // Enter the fresh-compaction decision with an already captured projection.
    // Journal writes and native task leases are separate from this decision.
    let context = crate::context::Context {
        messages: input.selected_context.as_ref().unwrap().messages.clone(),
        current_input_index: Some(2),
        ..Default::default()
    };
    let prepared = crate::model::PreparedModelRequest::prepare(
        execution.request_from_context(&context)?,
        executor.limits,
        Some(&io),
    )?;
    let count = crate::context::ModelTokenCount::from_upper_bounds(
        prepared.request_digest(),
        100,
        vec![8_000, 3_000, 8_000],
    );
    let projection = ResponseProjection {
        model: executor.model.clone(),
        context,
        canonical: None,
        checkpoint: None,
        working: None,
    };
    let journal = Journal::default();
    let outcome = execution
        .resolve_response_accounting(
            &journal,
            (&input, 0, &[]),
            (projection, Some(ContextAccounting { capacity, count })),
            prepared,
            &[],
            &mut DispatchPhase::new(),
        )
        .await;
    assert_eq!(
        counts.load(Ordering::SeqCst),
        1,
        "fresh compaction continued beyond its original owner's cancellation: {:?}",
        outcome.as_ref().err()
    );
    assert!(
        outcome.is_err(),
        "fresh compaction ignored its original owner"
    );
    Ok(())
}
