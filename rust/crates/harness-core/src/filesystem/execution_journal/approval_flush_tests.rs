//! Count actual device operations on both journal settlement entry points.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing_subscriber::layer::SubscriberExt as _;

#[derive(Clone, Default)]
struct Flushes(Arc<AtomicU64>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Flushes {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Count<'a>(&'a AtomicU64);
        impl tracing::field::Visit for Count<'_> {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "fsyncs" {
                    self.0.fetch_add(value, Ordering::Relaxed);
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        event.record(&mut Count(&self.0));
    }
}

#[test]
fn approval_settlement_groups_staging_but_keeps_both_durable_proofs() -> Result<()> {
    settlement_flushes(true)
}

#[test]
fn approval_settlement_keeps_a_foreign_filesystem_authority_durable() -> Result<()> {
    settlement_flushes(false)
}

fn settlement_flushes(shared_provider: bool) -> Result<()> {
    // The callsite registry belongs to the process. Give this observer one
    // process and install it before any provider/span is constructed.
    let test = std::thread::current()
        .name()
        .ok_or_else(|| Error::Storage("flush proof has no libtest identity".into()))?
        .to_owned();
    const CHILD: &str = "ACYCLIC_APPROVAL_FLUSH_PROOF";
    if crate::native_environment::variable(CHILD).as_deref() != Some(std::ffi::OsStr::new(&test)) {
        let mut command = std::process::Command::new(
            std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?,
        );
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            command.creation_flags(0x08000000);
        }
        let output = command
            .args([&test, "--exact", "--test-threads=1", "--nocapture"])
            .env(CHILD, &test)
            .output()
            .map_err(|error| Error::Storage(error.to_string()))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains("1 passed; 0 failed"), "{stdout}\n{stderr}");
        assert_eq!(stderr.matches("APPROVALS_SETTLEMENT_FLUSHES").count(), 2);
        eprint!("{stderr}");
        return Ok(());
    }
    let flushes = Flushes::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(flushes.clone()))
        .map_err(|error| Error::Storage(error.to_string()))?;
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
        let authority_directory =
            tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let authority = if shared_provider {
            stream.clone()
        } else {
            Arc::new(
                acyclic_stream::LocalStream::open(authority_directory.path(), Default::default())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
            )
        };
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let backend = acyclic_fs::Fs::new(
            acyclic_fs::StreamAuthorityStore::new(authority),
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
                flushes.0.store(0, Ordering::Relaxed);
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
                            .await?;
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
            if shared_provider {
                assert!(
                    measured[0] < measured[2],
                    "file settlement did not group staging: {measured:?}"
                );
                assert!(
                    measured[1] < measured[2],
                    "direct settlement did not group staging: {measured:?}"
                );
            } else {
                assert_eq!(measured[0], measured[2], "file settlement deferred a foreign authority");
                assert_eq!(measured[1], measured[2], "direct settlement deferred a foreign authority");
            }
            eprintln!(
                "APPROVALS_SETTLEMENT_FLUSHES shared={shared_provider} approved={approved} file={} direct={} former={}",
                measured[0], measured[1], measured[2]
            );
        }
        Ok(())
    })
}
