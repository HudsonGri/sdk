use super::*;

pub(super) struct LostCanonicalAck {
    pub(super) inner: Arc<dyn acyclic_harness::executor::ExecutionJournal>,
    pub(super) fail: std::sync::atomic::AtomicBool,
    pub(super) deny_reads: std::sync::atomic::AtomicBool,
    pub(super) tamper: std::sync::atomic::AtomicUsize,
}

impl acyclic_harness::executor::ExecutionJournal for LostCanonicalAck {
    fn claim_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::executor::ExecutionClaim<'a>> {
        self.inner.claim_execution(operation, io)
    }

    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            Vec<acyclic_harness::executor::ExecutionRecord>,
        >,
    > {
        self.inner.replay(operation, after, maximum, io)
    }
    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a acyclic_harness::executor::ExecutionEvent,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.append(operation, key, event, io)
    }
    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        tail: u64,
        key: &'a str,
        event: &'a acyclic_harness::executor::ExecutionEvent,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, bool>> {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            let event = event.clone();
            let compacted = matches!(
                event,
                acyclic_harness::executor::ExecutionEvent::ContextCompacted { step: 0, .. }
            );
            let mode = if compacted {
                self.tamper.swap(0, Ordering::SeqCst)
            } else {
                0
            };
            let event = if mode == 0 {
                event
            } else {
                fabricate_checkpoint(self.inner.as_ref(), operation, event, mode).await?
            };
            let committed = self
                .inner
                .append_if_tail(operation, tail, key, &event, io)?
                .await?;
            if committed && compacted && self.fail.swap(false, Ordering::SeqCst) {
                Err(Error::Storage(
                    "lost canonical compaction acknowledgement".into(),
                ))
            } else {
                Ok(committed)
            }
        })
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, FileRef>> {
        self.inner.stage(operation, key, bytes, media_type, io)
    }
    fn load<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a acyclic_harness::conversation::ContentIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
    {
        self.inner.load(file, io)
    }
    fn verify_input_file<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a acyclic_harness::filesystem::PreparationIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        if self.deny_reads.load(Ordering::SeqCst) {
            acyclic_harness::conversation::BoundedContentFuture::new(io, || async {
                Err(Error::Unauthorized("retained payload read denied".into()))
            })
        } else {
            self.inner.verify_input_file(file, io)
        }
    }
    fn verify_selected_context<'a>(
        &'a self,
        operation: OperationId,
        selected: &'a acyclic_harness::projection::SelectedModelContext,
        io: Option<&'a acyclic_harness::filesystem::PreparationIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.verify_selected_context(operation, selected, io)
    }
    fn open_interaction<'a>(
        &'a self,
        id: acyclic_harness::InteractionId,
        interaction: &'a acyclic_harness::interaction::Interaction,
        io: Option<&'a acyclic_harness::filesystem::PreparationIo>,
    ) -> Result<acyclic_harness::conversation::BoundedContentFuture<'a, ()>> {
        self.inner.open_interaction(id, interaction, io)
    }
    fn interaction_outcome<'a>(
        &'a self,
        id: acyclic_harness::InteractionId,
        io: Option<&'a acyclic_harness::filesystem::PreparationIo>,
    ) -> Result<
        acyclic_harness::conversation::BoundedContentFuture<
            'a,
            Option<acyclic_harness::interaction::InteractionOutcome>,
        >,
    > {
        self.inner.interaction_outcome(id, io)
    }
}

#[tokio::test]
pub(super) async fn default_canonical_checkpoint_recovers_lost_commit_ack_without_recount_or_stage_replay()
-> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    let (provider, counts) = summary_model(4);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let journal = Arc::new(LostCanonicalAck {
        inner: storage.journal(),
        fail: std::sync::atomic::AtomicBool::new(true),
        deny_reads: std::sync::atomic::AtomicBool::new(false),
        tamper: std::sync::atomic::AtomicUsize::new(0),
    });
    let bundle = storage
        .builder()
        .journal(journal.clone())
        .grant("model:generate")
        .model(
            Model::new(
                "synthetic".into(),
                "byte-counter".into(),
                "1".into(),
                serde_json::json!({}),
            )?,
            provider.clone(),
        )
        .context(ContextPipeline::new([Arc::new(InputDependentStage {
            revision: 1,
            calls: calls.clone(),
        })
            as Arc<dyn acyclic_harness::context::ContextStage>]))
        .build()?;
    storage
        .run_prompt(&bundle, &"q".repeat(60_000), Vec::new())
        .await?;
    let operation = OperationId::new();
    let file = storage
        .stage(
            operation,
            "fault/current.txt",
            "q".repeat(60_000).as_bytes(),
            "text/plain",
            "current.txt",
        )
        .await?;
    let error = storage
        .run_conversation(&bundle, operation, file.clone(), Vec::new(), 1)
        .await;
    assert!(matches!(error, Err(Error::Storage(message)) if message.contains("lost canonical")));
    assert!(
        acyclic_harness::executor::canonical_checkpoint_for_operation(
            storage.journal().as_ref(),
            operation,
            Limits::default(),
            None
        )
        .await?
        .is_some()
    );
    let generated = provider.requests().len();
    let counted = counts.load(Ordering::SeqCst);
    let transformed = calls.load(Ordering::SeqCst);
    assert!(
        storage
            .run_prompt(&bundle, "another turn", Vec::new())
            .await
            .is_err()
    );
    let output = storage
        .run_conversation(&bundle, operation, file, Vec::new(), 1)
        .await?;
    assert_eq!(output.text, "retained summary");
    assert_eq!(provider.requests().len(), generated + 1);
    assert_eq!(counts.load(Ordering::SeqCst), counted);
    assert_eq!(calls.load(Ordering::SeqCst), transformed);
    let records = storage.journal().replay(operation, 0, 64, None)?.await?;
    assert_single_compaction_and_summary(&records);
    storage
        .run_prompt(&bundle, "after recovery", Vec::new())
        .await?;
    assert_checkpoint_read_admission(&storage, &journal, operation).await?;
    assert_eq!(provider.remaining(), 0);
    Ok(())
}

pub(super) async fn assert_checkpoint_read_admission(
    storage: &MemoryHarnessStorage,
    journal: &LostCanonicalAck,
    operation: OperationId,
) -> Result<()> {
    let limits = Limits::default();
    let reference = acyclic_harness::executor::canonical_checkpoint_for_operation(
        journal, operation, limits, None,
    )
    .await?
    .ok_or_else(|| Error::Invalid("recovered checkpoint missing".into()))?;
    let (mut envelope, _) = acyclic_harness::executor::load_canonical_checkpoint(
        journal, &reference, limits, None, None,
    )
    .await?;
    journal.deny_reads.store(true, Ordering::SeqCst);
    assert!(
        matches!(acyclic_harness::executor::load_canonical_checkpoint(journal, &reference, limits, None, None).await,
        Err(Error::Unauthorized(message)) if message.contains("retained payload"))
    );
    journal.deny_reads.store(false, Ordering::SeqCst);
    envelope.operation_id = OperationId::new();
    let unpublished = storage
        .stage(
            envelope.operation_id,
            "fault/unpublished-checkpoint.json",
            &envelope.encode(limits)?,
            "application/json",
            "checkpoint.json",
        )
        .await?;
    assert!(
        matches!(acyclic_harness::executor::load_canonical_checkpoint(journal, &unpublished, limits, None, None).await,
        Err(Error::Conflict(message)) if message.contains("not published"))
    );
    Ok(())
}
