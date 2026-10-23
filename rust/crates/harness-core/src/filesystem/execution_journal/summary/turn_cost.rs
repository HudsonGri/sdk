use super::*;
use crate::executor::{ToolErrorClass, ToolFailureCause, ToolFailureKind};

#[tokio::test]
async fn authenticated_replay_defers_payload_loading_until_consumption() -> Result<()> {
    let journal = journal().await?;
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let payload = vec![b'x'; 16_384];
    let reference = journal
        .stage_with_io(operation, "invocation", &payload, "text/plain", None)
        .await?;
    journal
        .append(
            operation,
            "tool",
            &ExecutionEvent::ToolStarted {
                step: 0,
                call_id: "call".into(),
                invocation: reference.clone(),
            },
            None,
        )?
        .await?;
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )?;
    for cold in [false, true] {
        if cold {
            *journal.verified.lock().await = SummaryCache::default();
        }
        let before = io.receipt();
        let owned = io.owned_bytes()?;
        let records = journal.replay(operation, 1, 1, Some(&io))?.await?;
        let guard = io.guard_owned(io.owned_since(owned)?)?;
        assert_eq!(records.len(), 1);
        let after = io.receipt();
        eprintln!(
            "replay cold={cold} source_bytes={} authority_bytes={} object_bytes={} hashed_bytes={}",
            after.source_bytes_read - before.source_bytes_read,
            after.authority_bytes_read - before.authority_bytes_read,
            after.object_bytes_read - before.object_bytes_read,
            after.bytes_hashed - before.bytes_hashed,
        );
        let loaded = after.object_bytes_read - before.object_bytes_read;
        if cold {
            assert!(
                loaded >= payload.len() as u64,
                "clearing the proof must detect the eager replay negative control"
            );
        } else {
            assert_eq!(loaded, 0, "authenticated replay reloaded a payload");
        }
        drop(records);
        drop(guard);
    }
    let bytes = journal.load(&reference, Some(&io))?.await?;
    let guard = io.guard_bytes(&bytes)?;
    assert_eq!(bytes.as_ref(), payload);
    drop(bytes);
    drop(guard);
    Ok(())
}

#[tokio::test]
async fn append_cost_measurements() -> Result<()> {
    for native in [false, true] {
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 65_536).await?;
        let journal = storage.journal();
        let operation = OperationId::new();
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 16 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
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
        let mut elapsed = Vec::new();
        for step in 0..1_000 {
            let event = ExecutionEvent::ToolFailed {
                step,
                call_id: "call".into(),
                reason: ToolFailureKind::Unavailable,
                cause: ToolFailureCause {
                    class: ToolErrorClass::Invalid,
                    message: "unavailable".into(),
                },
            };
            let before = io.receipt();
            let started = std::time::Instant::now();
            assert!(
                journal
                    .append_if_tail(
                        operation,
                        u64::from(step) + 1,
                        &format!("failed:{step}"),
                        &event,
                        Some(&io)
                    )?
                    .await?
            );
            elapsed.push(started.elapsed().as_micros());
            let after = io.receipt();
            assert_eq!(
                after.authority_records_read - before.authority_records_read,
                0,
                "append replayed an already authenticated observation at step {step}"
            );
            assert_eq!(
                after.authority_bytes_read - before.authority_bytes_read,
                0,
                "append reloaded an authenticated journal prefix at step {step}"
            );
            assert!(
                after.bytes_encoded - before.bytes_encoded < 4_096,
                "append encoded historical events at step {step}"
            );
            if [0, 9, 99, 999].contains(&step) {
                eprintln!(
                    "append native={native} step={} elapsed_us={} records_read={} bytes_read={} bytes_encoded={}",
                    step + 1,
                    elapsed[step as usize],
                    after.authority_records_read - before.authority_records_read,
                    after.authority_bytes_read - before.authority_bytes_read,
                    after.bytes_encoded - before.bytes_encoded
                );
            }
        }
        for (start, end) in [(0, 10), (90, 100), (990, 1_000)] {
            let mean = elapsed[start..end].iter().sum::<u128>() / (end - start) as u128;
            eprintln!(
                "append native={native} turns={}..={end} mean_us={mean}",
                start + 1
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn resetting_the_summary_is_detected_by_the_flat_append_read_bound() -> Result<()> {
    let journal = journal().await?;
    let operation = OperationId::new();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )?;
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
    for step in 0..10 {
        // The old measured append path constructed this empty summary each time.
        *journal.verified.lock().await = SummaryCache::default();
        let before = io.receipt();
        journal
            .append(
                operation,
                &format!("failed:{step}"),
                &ExecutionEvent::ToolFailed {
                    step,
                    call_id: "call".into(),
                    reason: ToolFailureKind::Unavailable,
                    cause: ToolFailureCause {
                        class: ToolErrorClass::Invalid,
                        message: "unavailable".into(),
                    },
                },
                Some(&io),
            )?
            .await?;
        let replayed = io.receipt().authority_records_read - before.authority_records_read;
        assert_eq!(replayed, u64::from(step) + 1);
        assert!(
            replayed > 0,
            "flat read oracle failed to detect prefix replay"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_second_writer_refreshes_only_the_tail_and_preserves_retry_and_compare_semantics()
-> Result<()> {
    let first = journal().await?;
    let second = FilesystemExecutionJournal::new(
        first.stream.clone(),
        first.host.clone(),
        &SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &first.volume,
            verifier: &first.verifier,
            scope: &first.scope,
            maximum_bytes: first.maximum_payload_bytes,
            io: None,
        },
    )?;
    let operation = OperationId::new();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )?;
    let failure = |step, message: &str| ExecutionEvent::ToolFailed {
        step,
        call_id: "call".into(),
        reason: ToolFailureKind::Unavailable,
        cause: ToolFailureCause {
            class: ToolErrorClass::Invalid,
            message: message.into(),
        },
    };
    first
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            Some(&io),
        )?
        .await?;
    assert!(
        first
            .append_if_tail(operation, 1, "first", &failure(0, "unavailable"), Some(&io))?
            .await?
    );
    assert!(
        second
            .append_if_tail(
                operation,
                2,
                "second",
                &failure(1, "unavailable"),
                Some(&io)
            )?
            .await?
    );
    let before = io.receipt();
    assert!(
        !first
            .append_if_tail(operation, 2, "third", &failure(2, "unavailable"), Some(&io))?
            .await?
    );
    assert_eq!(
        io.receipt().authority_records_read - before.authority_records_read,
        1
    );
    assert!(
        first
            .append_if_tail(operation, 1, "first", &failure(0, "unavailable"), Some(&io))?
            .await?
    );
    assert!(
        !first
            .append_if_tail(operation, 2, "first", &failure(0, "unavailable"), Some(&io))?
            .await?
    );
    let Err(error) = first
        .append(operation, "first", &failure(0, "different"), Some(&io))?
        .await
    else {
        return Err(Error::Invalid("changed retry was accepted".into()));
    };
    assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
    Ok(())
}

#[test]
fn quiescence_work_is_flat_for_a_thousand_settled_dispatches() -> Result<()> {
    let mut summary = ExecutionSummary::default();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(16 * 1024 * 1024, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )?;
    super::accept(
        &mut summary,
        ExecutionEvent::Started {
            request_digest: [1; 32],
        },
    )?;
    for step in 0..1_000 {
        super::accept(
            &mut summary,
            ExecutionEvent::ToolFailed {
                step,
                call_id: "call".into(),
                reason: ToolFailureKind::Unavailable,
                cause: ToolFailureCause {
                    class: ToolErrorClass::Invalid,
                    message: "unavailable".into(),
                },
            },
        )?;
        let before = io.receipt().items_examined;
        assert!(summary.quiescent(Some(&io))?);
        assert_eq!(io.receipt().items_examined - before, 1);
    }
    Ok(())
}

async fn journal() -> Result<
    FilesystemExecutionJournal<
        acyclic_stream::MemoryStream,
        acyclic_fs::MemoryAuthorityBackend,
        acyclic_fs::MemoryObjectBackend,
    >,
> {
    let provider = crate::resources::ProviderRef::new(
        "append-cost".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = crate::AgentId::new();
    let volume = VolumeRef::new(
        provider,
        "append-cost".into(),
        VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityIssuer::new(
        "append-cost",
        [81; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::new().to_string(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        &SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            maximum_bytes: 65_536,
            io: None,
        },
    )
}
