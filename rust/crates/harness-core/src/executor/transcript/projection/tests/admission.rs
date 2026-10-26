//! Real journal checkpoint admission through the sole projection codec.
use super::*;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one real-journal admission, retry and operation-isolation timeline"
)]
async fn local_journal_admits_compaction_only_after_one_settled_summary() -> Result<()> {
    use crate::executor::{ExecutionEvent, ModelPurpose};
    use crate::{AgentId, filesystem::MemoryHarnessStorage, model::PreparedModelRequest};
    let storage = MemoryHarnessStorage::new(AgentId::new(), 131_072).await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    let request = PreparedModelRequest::prepare(
        ModelRequest {
            conversation_id: None,
            model: Model::new(
                "synthetic".into(),
                "journal-test".into(),
                "1".into(),
                serde_json::json!({}),
            )?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("source".into()),
            }],
            tools: Vec::new(),
            max_output_tokens: Some(32),
        },
        Limits::default(),
        None,
    )?;
    let request_ref = journal
        .stage(
            operation,
            "summary-request",
            request.bytes(),
            "application/json",
            None,
        )?
        .await?;
    let projection = ResponseProjection {
        model: request.request().model.clone(),
        context: crate::context::Context {
            messages: request.request().messages.clone(),
            current_input_index: Some(0),
            ..Default::default()
        },
        canonical: None,
        checkpoint: None,
        working: None,
    };
    let context = stage_projection(
        journal.as_ref(),
        operation,
        "temporal-context",
        &projection,
        &request,
        None,
    )
    .await?;
    let compacted = ExecutionEvent::ContextCompacted {
        step: 0,
        projection: context.clone(),
        compaction: context.clone(),
        accounting: context.clone(),
    };
    assert!(
        journal
            .append_if_tail(operation, 0, "premature", &compacted, None)?
            .await
            .is_err()
    );
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
    journal
        .append(
            operation,
            "prepared",
            &ExecutionEvent::ContextPrepared {
                step: 0,
                projection: context.clone(),
                accounting: None,
            },
            None,
        )?
        .await?;
    assert!(
        journal
            .append(operation, "no-summary", &compacted, None)?
            .await
            .is_err()
    );
    journal
        .append(
            operation,
            "summary-started",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Summary,
                request_digest: request.request_digest(),
                request: request_ref,
            },
            None,
        )?
        .await?;
    assert!(
        journal
            .append(operation, "unsettled-summary", &compacted, None)?
            .await
            .is_err()
    );
    let completed = stage_json(
        journal.as_ref(),
        operation,
        "summary-completed",
        &ModelEvent::Completed {
            metadata: serde_json::json!({}),
        },
        None,
    )
    .await?;
    journal
        .append(
            operation,
            "summary-completed",
            &ExecutionEvent::Model {
                step: 0,
                purpose: ModelPurpose::Summary,
                event: completed,
            },
            None,
        )?
        .await?;
    assert!(
        !journal
            .append_if_tail(operation, 3, "stale-tail", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append_if_tail(operation, 4, "compacted", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append_if_tail(operation, 4, "compacted", &compacted, None)?
            .await?
    );
    assert!(
        journal
            .append(operation, "duplicate-compacted", &compacted, None)?
            .await
            .is_err()
    );
    let other = OperationId::new();
    journal
        .append(
            other,
            "started",
            &ExecutionEvent::Started {
                request_digest: [2; 32],
            },
            None,
        )?
        .await?;
    assert_eq!(journal.replay(operation, 0, 64, None)?.await?.len(), 5);
    assert_eq!(journal.replay(other, 0, 64, None)?.await?.len(), 1);
    assert!(
        journal
            .append_if_tail(
                operation,
                4,
                "compacted",
                &journal
                    .replay(operation, 4, 1, None)?
                    .await?
                    .first()
                    .ok_or_else(|| Error::Invalid("compaction record missing".into()))?
                    .event,
                None
            )?
            .await?
    );
    Ok(())
}
