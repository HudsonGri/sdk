//! Count actual device operations on both journal settlement entry points.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing_subscriber::layer::SubscriberExt as _;

#[derive(Clone, Default)]
struct Flushes(Arc<AtomicUsize>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Flushes {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Count<'a>(&'a AtomicUsize);
        impl tracing::field::Visit for Count<'_> {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "fsyncs" {
                    self.0.fetch_add(value as usize, Ordering::Relaxed);
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        event.record(&mut Count(&self.0));
    }
}

#[test]
fn approval_settlement_groups_staging_but_keeps_both_durable_proofs() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    runtime.block_on(async {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let stream = Arc::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let backend = acyclic_fs::Fs::new(
            acyclic_fs::StreamAuthorityStore::new(stream.clone()),
            acyclic_fs::LogicalObjectStore::new(Arc::new(objects), bucket),
            acyclic_fs::EmbeddedCapabilities::MEMORY,
        );
        let fixture = Fixture::new(
            stream,
            backend,
            serde_json::json!({"type":"integer"}),
            None,
            &[],
        )
        .await?;
        fixture.running(TASK).await?;
        let conversation = conversation();
        bind(&fixture, &conversation).await?;
        let operation = OperationId::from_bytes([TASK; 16]);
        let journal = fixture.execution(fixture.owner(TASK).await?, operation)?;
        let files = Arc::new(
            TaskFiles::open(
                fixture.stream.clone(),
                &fixture.storage,
                &fixture.issuer.verifier(&mut None)?,
                &journal.scope,
                65_536,
            )
            .await?,
        );
        let scope = conversation.issue(
            "interaction-owner",
            &Capabilities::new([
                crate::contract::capability::INTERACTION_OPEN.to_owned(),
                fixture
                    .storage
                    .capability(VolumeOperation::Read, &mut None)?,
                fixture
                    .storage
                    .capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let journal = journal
            .with_task_files(files.clone())?
            .with_interaction_owner(
                &conversation.verifier(&mut None)?,
                &SchemaRegistry::new(),
                &scope,
                &fixture.storage,
                None,
            )?;
        for approved in [false, true] {
            let mut measured = Vec::new();
            for entry in 0..3 {
                let id = InteractionId::from_bytes([100 + entry + 3 * u8::from(approved); 16]);
                let request = Interaction::approval(
                    "Grouped settlement".into(),
                    operation,
                    [5; 32],
                    ApprovalSubject::Opaque {},
                )?;
                journal.open_interaction(id, &request, None)?.await?;
                let host = journal.interactions()?;
                let ticket = host.read(id, None).await?.unwrap().0;
                let human = responder(&conversation, "human", &[id])?;
                let decision = decision(
                    id,
                    "human",
                    &ApprovalDecisionFile::new(
                        "human",
                        ticket.request.descriptor().sha256(),
                        1,
                        approved,
                        None,
                    )?,
                );
                let flushes = Flushes::default();
                let subscriber = tracing::subscriber::set_default(
                    tracing_subscriber::registry().with(flushes.clone()),
                );
                match entry {
                    0 => journal.write_approval_decision(&decision, &human).await?,
                    1 => {
                        journal
                            .resolve_interaction(
                                id,
                                InteractionResponse::Approval {
                                    approved,
                                    reason: None,
                                },
                                &human,
                            )
                            .await?
                    }
                    _ => {
                        // Execute the former eager filesystem staging and
                        // canonical append, followed by the identical proof.
                        host.resolve_latest(
                            id,
                            InteractionResponse::Approval {
                                approved,
                                reason: None,
                            },
                            &human,
                            journal.owner.as_ref().map(|(owner, _)| owner),
                        )
                        .await?;
                        journal.published_decision(id, None).await?;
                    }
                }
                drop(subscriber);
                let count = flushes.0.load(Ordering::Relaxed);
                assert!(count > 0, "the native flush trace was not observed");
                measured.push(count);
                let observed = files
                    .snapshot(
                        &Query::Approval {
                            task: task(),
                            request: request_id(id),
                        },
                        8,
                    )
                    .await?;
                assert_eq!(observed.files.len(), 2);
                for file in &observed.files {
                    assert!(
                        files.inner.file_is_durable(file).await?,
                        "settlement returned an unflushed file"
                    );
                }
                let proof = journal.published_decision(id, None).await?;
                assert_eq!(proof.approved(), approved);
                assert_eq!(
                    journal.interaction_outcome_with_io(id, None).await?,
                    Some(if approved {
                        InteractionOutcome::Approved {}
                    } else {
                        InteractionOutcome::Declined {}
                    })
                );
            }
            assert!(
                measured[0] < measured[2],
                "file settlement did not group staging: {measured:?}"
            );
            assert!(
                measured[1] < measured[2],
                "direct settlement did not group staging: {measured:?}"
            );
            eprintln!(
                "APPROVALS_SETTLEMENT_FLUSHES approved={approved} file={} direct={} former={}",
                measured[0], measured[1], measured[2]
            );
        }
        Ok(())
    })
}
