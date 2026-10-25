//! Group commit of model observations: a crash at every journal mutation
//! leaves a recoverable record, and no observation the executor received is
//! still unrecorded when it waits on the provider again.
use super::*;
use std::task::Poll;

/// The shared test journal, crashing at one mutation ordinal: that mutation
/// and every later one fail without effect, as after a process crash.
struct CrashingJournal {
    inner: Journal,
    mutations: AtomicUsize,
    crash_at: usize,
    groups: Mutex<Vec<usize>>,
}

impl CrashingJournal {
    fn new(crash_at: usize) -> Self {
        Self {
            inner: Journal::default(),
            mutations: AtomicUsize::new(0),
            crash_at,
            groups: Mutex::new(Vec::new()),
        }
    }

    fn mutate(&self) -> Result<()> {
        if self.mutations.fetch_add(1, Ordering::SeqCst) >= self.crash_at {
            return Err(Error::Storage("crashed".into()));
        }
        Ok(())
    }

    /// Durable observations of the response step, in journal order.
    fn observations(&self, operation: OperationId) -> Vec<(String, FileRef)> {
        self.inner
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record.operation_id == operation)
            .filter_map(|record| match &record.event {
                ExecutionEvent::Model { step: 0, event, .. } => {
                    Some((record.idempotency_key.clone(), event.clone()))
                }
                _ => None,
            })
            .collect()
    }
}

impl ExecutionJournal for CrashingJournal {
    fn claim_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a PreparationIo>,
    ) -> Result<ExecutionClaim<'a>> {
        self.inner.claim_execution(operation, io)
    }

    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
        context: Option<&'a crate::runtime::TaskContext>,
        summary_files: &'a [&'a FileRef],
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.inner
            .verify_model_content(content, io, context, summary_files)
    }

    fn verify_input_file<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.verify_input_file(reference, io)
    }

    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        self.inner.replay(operation_id, after, maximum, io)
    }

    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.mutate()?;
        self.inner.append(operation_id, idempotency_key, event, io)
    }

    fn append_if_tail<'a>(
        &'a self,
        operation_id: OperationId,
        expected_tail: u64,
        claim_id: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        self.mutate()?;
        self.inner
            .append_if_tail(operation_id, expected_tail, claim_id, event, io)
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        self.mutate()?;
        self.inner
            .stage(operation_id, idempotency_key, bytes, media_type, io)
    }

    /// One mutation publishes the whole group, as one generation does.
    fn stage_all<'a>(
        &'a self,
        operation_id: OperationId,
        payloads: &'a [crate::executor::StagedPayload<'a>],
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<FileRef>>> {
        self.mutate()?;
        self.groups.lock().unwrap().push(payloads.len());
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let mut files = Vec::new();
            for payload in payloads {
                files.push(
                    self.inner
                        .stage(
                            operation_id,
                            payload.idempotency_key,
                            payload.bytes,
                            payload.media_type,
                            io,
                        )?
                        .await?,
                );
            }
            Ok(files)
        })
    }

    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.inner.load(reference, io)
    }
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        request: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.open_interaction(id, request, io)
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        self.inner.interaction_outcome(id, io)
    }
}

/// Emits bursts of observations, pending once between bursts; on resuming
/// after a wait it records how many of the observations it emitted are
/// durable at that moment.
struct BurstProvider {
    bursts: Vec<Vec<ModelEvent>>,
    journal: Arc<CrashingJournal>,
    operation: OperationId,
    waits: Mutex<Vec<(usize, usize)>>,
}

impl ModelProvider for BurstProvider {
    fn generate<'a>(
        &'a self,
        _: crate::model::PreparedModelRequest,
        _: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        let (mut burst, mut position, mut waited, mut emitted) = (0, 0, false, 0);
        Box::pin(futures::stream::poll_fn(move |context| {
            let Some(events) = self.bursts.get(burst) else {
                return Poll::Ready(None);
            };
            if position == 0 && burst > 0 {
                if !waited {
                    waited = true;
                    context.waker().wake_by_ref();
                    return Poll::Pending;
                }
                let durable = self.journal.observations(self.operation).len();
                self.waits.lock().unwrap().push((emitted, durable));
            }
            let event = events[position].clone();
            position += 1;
            emitted += 1;
            if position == events.len() {
                (burst, position, waited) = (burst + 1, 0, false);
            }
            Poll::Ready(Some(Ok(event)))
        }))
    }
}

fn bursts() -> Vec<Vec<ModelEvent>> {
    let text = |delta: &str| ModelEvent::Content {
        delta: delta.into(),
    };
    vec![
        vec![text("a"), text("b"), text("c")],
        vec![text("d")],
        vec![
            text("e"),
            ModelEvent::Completed {
                metadata: json!({}),
            },
        ],
    ]
}

/// One turn's outcome: whether it completed, the provider's wait
/// observations, the durable observation count and the group sizes staged.
type Run = (bool, Vec<(usize, usize)>, usize, Vec<usize>);

/// Runs one turn crashing at `crash_at`, checking the durable record.
async fn run(crash_at: usize, defer: bool) -> Result<Run> {
    let operation = OperationId::new();
    let journal = Arc::new(CrashingJournal::new(crash_at));
    let provider = Arc::new(BurstProvider {
        bursts: bursts(),
        journal: journal.clone(),
        operation,
        waits: Mutex::new(Vec::new()),
    });
    let executor = uncompacted_executor(
        Model::new("test".into(), "model".into(), "1".into(), json!({}))?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: operation,
        input: ModelContent::Text("burst".into()),
        selected_context: None,
        max_steps: 1,
    };
    let completed = crate::executor::dispatch::model_group_deferred(defer, || {
        executor.execute(input, journal.as_ref())
    })
    .await
    .is_ok();
    let scripted: Vec<ModelEvent> = bursts().into_iter().flatten().collect();
    let observations = journal.observations(operation);
    // The durable observations are exactly a prefix, each readable as the
    // observation the provider emitted at its index.
    for (index, (key, reference)) in observations.iter().enumerate() {
        assert!(key.ends_with(&format!(":0:{index}")), "out of order: {key}");
        let bytes = journal.load(reference, None)?.await?;
        let event: ModelEvent =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(event, scripted[index]);
    }
    let waits = provider.waits.lock().unwrap().clone();
    let groups = journal.groups.lock().unwrap().clone();
    Ok((completed, waits, observations.len(), groups))
}

#[tokio::test]
async fn a_crash_at_every_journal_mutation_keeps_every_observation_received_before_a_wait()
-> Result<()> {
    let mut crash_at = 0;
    loop {
        let (completed, waits, durable, _) = run(crash_at, false).await?;
        for (emitted, recorded) in &waits {
            assert_eq!(
                emitted, recorded,
                "crash at {crash_at}: waited with an unrecorded observation"
            );
        }
        // Every observation received before the provider's last wait was
        // durable before that wait, so a later crash cannot lose it.
        if let Some((emitted, _)) = waits.last() {
            assert!(
                durable >= *emitted,
                "crash at {crash_at} lost {durable}<{emitted}"
            );
        }
        if completed {
            assert_eq!(durable, 6);
            break;
        }
        crash_at += 1;
        assert!(crash_at < 64, "the turn never completed");
    }
    // Each burst is one group commit, recorded before the next wait.
    let (_, waits, _, groups) = run(usize::MAX, false).await?;
    assert_eq!(waits, [(3, 3), (4, 4)]);
    assert_eq!(groups, [3, 1, 2]);
    Ok(())
}

#[tokio::test]
async fn deferring_the_group_past_a_wait_is_detected() -> Result<()> {
    // Negative control: recording only at stream end waits with every
    // observation unrecorded, and a crash then loses all of them.
    let (completed, waits, _, groups) = run(usize::MAX, true).await?;
    assert!(completed);
    assert_eq!(waits, [(3, 0), (4, 0)]);
    assert_eq!(groups, [6]);
    Ok(())
}
