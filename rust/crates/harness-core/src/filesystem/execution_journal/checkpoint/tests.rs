use super::*;
use crate::executor::{ToolErrorClass, ToolFailureCause, ToolFailureKind};

#[test]
fn checkpoint_frame_has_fixed_size_and_one_canonical_root_encoding() -> Result<()> {
    let mut checkpoint = Checkpoint {
        state: IndexState {
            operation: OperationId::from_bytes([1; 16]),
            turn: OperationId::from_bytes([2; 16]),
            volume: [3; 32],
            tail: u64::MAX,
            nodes_tail: u64::MAX,
            roots: [None; 5],
            pending_dispatches: u64::MAX,
            anchor: [4; 32],
        },
        proof: [5; 32],
    };
    for mask in 0..32_u8 {
        for (index, root) in checkpoint.state.roots.iter_mut().enumerate() {
            *root = (mask & (1 << index) != 0).then_some(NodeRef {
                sequence: u64::MAX - 1,
                digest: [u8::try_from(index).map_err(|_| missing(None))?; 32],
            });
        }
        let bytes = codec::encode(&checkpoint, None)?;
        assert_eq!(
            bytes.len(),
            361,
            "checkpoint size depends on history or root count"
        );
        let decoded = codec::decode(&bytes, None)?;
        assert_eq!(
            codec::state_bytes(&decoded.state, None)?,
            codec::state_bytes(&checkpoint.state, None)?
        );
        assert_eq!(decoded.proof, checkpoint.proof);
        for length in 0..bytes.len() {
            assert!(codec::decode(&bytes[..length], None).is_err());
        }
    }
    checkpoint.state.roots = [None; 5];
    let original = codec::encode(&checkpoint, None)?;
    for index in 0..5 {
        let mut changed = original.to_vec();
        let root = 84 + index * 41;
        changed[root + 1] = 1;
        assert!(
            codec::decode(&changed, None).is_err(),
            "absent root carried a nonzero sequence"
        );
        changed[root + 1] = 0;
        changed[root + 9] = 1;
        assert!(
            codec::decode(&changed, None).is_err(),
            "absent root carried a digest"
        );
        changed[root] = 2;
        assert!(
            codec::decode(&changed, None).is_err(),
            "unknown root tag was accepted"
        );
    }
    let mut trailing = original.to_vec();
    trailing.push(0);
    assert!(codec::decode(&trailing, None).is_err());
    Ok(())
}

fn event(step: u32) -> ExecutionEvent {
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

fn bytes(
    operation: OperationId,
    retry: &str,
    event: &ExecutionEvent,
) -> Result<acyclic_stream::OwnedBytes> {
    serde_json::to_vec(&Observation {
        version: OBSERVATION_VERSION,
        operation_id: operation,
        turn_operation: operation,
        retry_digest: retry.into(),
        event: event.clone(),
    })
    .map(Into::into)
    .map_err(|_| missing(None))
}

fn accept(summary: &mut ExecutionSummary, retry: &str, event: ExecutionEvent) -> Result<[u8; 32]> {
    summary.accept(
        &ExecutionRecord {
            operation_id: summary.operation.ok_or_else(|| missing(None))?,
            sequence: summary.tail + 1,
            idempotency_key: retry.into(),
            event,
        },
        None,
        Limits::default(),
        None,
    )?;
    Ok(*blake3::hash(retry.as_bytes()).as_bytes())
}

#[tokio::test]
async fn checkpoint_and_observation_publish_at_one_authenticated_cut() -> Result<()> {
    for native in [None, Some(false), Some(true)] {
        check_cut(native).await?;
    }
    Ok(())
}

async fn check_cut(native: Option<bool>) -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = native
        .map(|native| {
            if native {
                PreparationIo::from_native_work(&work)
            } else {
                PreparationIo::from_budget(budget, &cancellation)
            }
        })
        .transpose()?;
    let journal = super::super::summary::tests::turn_cost::journal().await?;
    let operation = OperationId::new();
    let mut summary = ExecutionSummary {
        operation: Some(operation),
        ..Default::default()
    };
    for (retry, event) in [
        (
            "start",
            ExecutionEvent::Started {
                request_digest: [1; 32],
            },
        ),
        ("tool", event(0)),
    ] {
        let retry_key = accept(&mut summary, retry, event.clone())?;
        let observation = bytes(operation, retry, &event)?;
        let recovery = journal
            .publish_indexed_observation(&mut summary, retry_key, &event, &observation, io.as_ref())
            .await?
            .ok_or_else(|| missing(io.as_ref()))?;
        let loaded = journal
            .load_index_checkpoint(operation, io.as_ref())
            .await?
            .ok_or_else(|| missing(io.as_ref()))?;
        assert_eq!(loaded.0.tail, summary.tail);
        assert_eq!(loaded.0.roots, recovery.state.roots);
        assert_eq!(loaded.1, recovery.head_tail);
        assert!(
            journal
                .index_authenticates(loaded.0, summary.tail, retry, &event, io.as_ref())
                .await?
        );
        assert!(
            journal
                .index_authenticates(
                    loaded.0,
                    summary.tail,
                    "different-retry",
                    &event,
                    io.as_ref()
                )
                .await
                .is_err()
        );
        summary.recovery = Some(recovery);
    }
    assert_eq!(journal.replay(operation, 0, 4, None)?.await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn detached_and_forged_checkpoint_heads_cannot_authorize_recovery() -> Result<()> {
    let journal = super::super::summary::tests::turn_cost::journal().await?;
    let operation = OperationId::new();
    let mut summary = ExecutionSummary {
        operation: Some(operation),
        ..Default::default()
    };
    let first = ExecutionEvent::Started {
        request_digest: [1; 32],
    };
    let retry = accept(&mut summary, "start", first.clone())?;
    let recovery = journal
        .publish_indexed_observation(
            &mut summary,
            retry,
            &first,
            &bytes(operation, "start", &first)?,
            None,
        )
        .await?
        .expect("initial commit");
    let original = journal.encode_index_checkpoint(recovery.state, None)?;
    let head = journal.index_path(operation, "head", None)?;
    journal
        .stream
        .append(acyclic_stream::AppendRequest {
            path: (*head).clone(),
            records: vec![original.clone()],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    assert!(
        journal
            .load_index_checkpoint(operation, None)
            .await
            .is_err(),
        "detached valid root was accepted without its anchor commit"
    );
    let observation = bytes(operation, "forged", &event(0))?;
    let mut forged = codec::decode(&original, None)?;
    forged.state.tail = 2;
    forged.state.anchor = *blake3::hash(&observation).as_bytes();
    let execution = crate::distributed::execution_path(operation, &mut None)?;
    let request = acyclic_stream::CommitRequest {
        conditions: vec![
            acyclic_stream::CommitCondition::Tail {
                path: (*head).clone(),
                expected: 2,
            },
            acyclic_stream::CommitCondition::Tail {
                path: execution.clone(),
                expected: 1,
            },
        ],
        mutations: vec![
            acyclic_stream::CommitMutation::Append {
                path: (*head).clone(),
                records: vec![codec::encode(&forged, None)?],
            },
            acyclic_stream::CommitMutation::Append {
                path: execution,
                records: vec![observation],
            },
        ],
        idempotency_key: StreamKey::new(acyclic_stream::OwnedBytes::from_static(b"forged-head"))?,
    };
    assert!(matches!(
        journal.stream.commit(request).await?,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    assert!(
        journal
            .load_index_checkpoint(operation, None)
            .await
            .is_err(),
        "forged root was accepted merely because its anchor was atomic"
    );
    Ok(())
}
