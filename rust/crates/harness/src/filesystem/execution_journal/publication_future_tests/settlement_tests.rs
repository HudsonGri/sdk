//! Accepted model output settles after its original invocation stops accepting work.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

struct ClockCut(AtomicU64);
impl acyclic_stream::UnixMillisClock for ClockCut {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[tokio::test]
async fn accepted_text_repairs_after_cancellation_and_deadline_at_both_projection_cuts()
-> Result<()> {
    use crate::task_volume::{Component, Query, TaskFile};
    for deadline in [false, true] {
        for binding_exists in [false, true] {
            let operation = OperationId::from_bytes([61; 16]);
            let clock = Arc::new(ClockCut(AtomicU64::new(1000)));
            let cancellation = acyclic_fs::CancellationToken::new();
            let (journal, host) = projected_fixture_at(
                operation,
                clock.clone(),
                deadline.then_some(2000),
                Some(&cancellation),
            )
            .await?;
            turn(&journal, false, "accepted", 1, false, Some(host), true).await?;
            let record = journal
                .records(operation, 0, 64, None)
                .await?
                .into_iter()
                .find(|record| matches!(record.event, ExecutionEvent::Model { .. }))
                .expect("canonical content");
            let owner = &journal.owner.as_ref().unwrap().0;
            let task = Component::new(owner.task_id().to_string())?;
            if binding_exists {
                journal.model_turn(operation, 0, &task, None).await?;
            }
            if deadline {
                clock.0.store(3000, Ordering::SeqCst);
            } else {
                cancellation.cancel();
            }
            // The original owner refuses fresh work, while its already accepted
            // canonical output must still repair both disposable file writes.
            assert!(
                owner
                    .condition(crate::distributed::JournalWrite::Fresh, &mut None)
                    .await
                    .is_err()
            );
            for _ in 0..2 {
                journal
                    .publish_chunk(operation, "model:0:0", &record.event, None)
                    .await?;
            }
            let snapshot = journal
                .task_files
                .as_ref()
                .unwrap()
                .snapshot(&Query::Root, 16)
                .await?;
            let chunks: Vec<_> = snapshot
                .files
                .iter()
                .filter(|file| {
                    matches!(
                        file.key,
                        crate::task_volume::FileKey::Task {
                            file: TaskFile::Chunk { .. },
                            ..
                        }
                    )
                })
                .collect();
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].bytes.as_ref(), b"0:accepted");
        }
    }
    Ok(())
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn accepted_hook_outcomes_settle_after_the_cutoff_without_restarting_effects() -> Result<()> {
    for deadline in [false, true] {
        let clock = Arc::new(ClockCut(AtomicU64::new(1000)));
        let cancellation = acyclic_fs::CancellationToken::new();
        let operation = OperationId::from_bytes([62; 16]);
        let (journal, _host) = projected_fixture_at(
            operation,
            clock.clone(),
            deadline.then_some(2000),
            Some(&cancellation),
        )
        .await?;
        let owner = &journal.owner.as_ref().unwrap().0;
        let files = journal.task_files.as_ref().unwrap();
        let mut authorize = |write| async move {
            let (condition, deadline) = owner.file_condition(write, &mut None).await?;
            Ok((vec![condition], deadline))
        };
        let identity = (owner.task_id(), operation, "SessionStart");
        let output = crate::hooks::effects::run_once::<_, _, _, _, _, true>(
            &files.inner,
            identity,
            4096,
            true,
            &mut authorize,
            || async {
                // The external effect completed under its accepted claim. Its
                // result publication crosses the original admission cutoff.
                if deadline {
                    clock.0.store(3000, Ordering::SeqCst);
                } else {
                    cancellation.cancel();
                }
                Ok(crate::hooks::HookOutput {
                    additional_context: vec!["accepted hook output".into()],
                    ..Default::default()
                })
            },
        )
        .await?;
        assert_eq!(output.additional_context, ["accepted hook output"]);
        let replay = crate::hooks::effects::run_once::<_, _, _, _, _, true>(
            &files.inner,
            identity,
            4096,
            false,
            &mut authorize,
            || async { panic!("settlement restarted the completed external effect") },
        )
        .await?;
        assert_eq!(replay, output);
        let refused = crate::hooks::effects::run_once::<_, _, _, _, _, true>(
            &files.inner,
            (owner.task_id(), OperationId::from_bytes([63; 16]), "Stop"),
            4096,
            true,
            authorize,
            || async { panic!("settlement authority started a fresh external effect") },
        )
        .await;
        assert!(matches!(refused, Err(Error::Indeterminate(_))));
    }
    Ok(())
}
