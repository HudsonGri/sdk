//! Recovery baseline before introducing a durable execution index.
use super::*;
use crate::executor::{ToolErrorClass, ToolFailureCause, ToolFailureKind};

fn refusal(step: u32) -> ExecutionEvent {
    ExecutionEvent::ToolFailed {
        step,
        call_id: "call".into(),
        reason: ToolFailureKind::Unavailable,
        cause: ToolFailureCause {
            class: ToolErrorClass::Invalid,
            message: "unavailable".into(),
        },
    }
}

#[tokio::test]
async fn cold_append_recovery_measurements() -> Result<()> {
    for count in [1, 100, 1_000] {
        let original = super::turn_cost::journal().await?;
        let operation = OperationId::new();
        original
            .append(
                operation,
                "start",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        for step in 0..count {
            original
                .append(operation, &format!("failed:{step}"), &refusal(step), None)?
                .await?;
        }
        let reopened = FilesystemExecutionJournal::new(
            original.stream.clone(),
            original.host.clone(),
            &SchemaRegistry::new(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &original.volume,
                verifier: &original.verifier,
                scope: &original.scope,
                maximum_bytes: original.maximum_payload_bytes,
                io: None,
            },
        )?;
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )?;
        let before = io.receipt();
        let started = std::time::Instant::now();
        reopened
            .append(operation, "after-reopen", &refusal(count), Some(&io))?
            .await?;
        let after = io.receipt();
        eprintln!(
            "cold append steps={count} elapsed_us={} records_read={} authority_bytes={} hashed_bytes={} owned_bytes={}",
            started.elapsed().as_micros(),
            after.authority_records_read - before.authority_records_read,
            after.authority_bytes_read - before.authority_bytes_read,
            after.bytes_hashed - before.bytes_hashed,
            io.owned_bytes()?,
        );
        // Explicitly exercise the old full-fold path as the recovery control.
        let before = io.receipt();
        let owned = io.owned_bytes()?;
        let mut summary = ExecutionSummary {
            operation: Some(operation),
            ..Default::default()
        };
        reopened
            .refresh_summary(operation, &mut summary, Some(&io))
            .await?;
        let hold = io.guard_owned(io.owned_since(owned)?)?;
        assert_eq!(summary.tail, u64::from(count) + 2);
        assert_eq!(
            io.receipt().authority_records_read - before.authority_records_read,
            u64::from(count) + 2,
            "full-fold negative control did not read the entire history"
        );
        drop(summary);
        drop(hold);
    }
    Ok(())
}
