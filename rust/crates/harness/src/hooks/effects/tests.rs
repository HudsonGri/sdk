use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

fn authorization() -> std::future::Ready<Result<(Vec<CommitCondition>, Option<u64>)>> {
    std::future::ready(Ok((Vec::new(), None)))
}

proptest! {
    // Property: replay uses the exact retained output without executing again;
    // a failed/cancelled dispatch leaves a claim that prevents every retry.
    #[test]
    fn retained_outcomes_replay_and_uncertain_effects_are_never_resent(
        text in ".{0,96}",
        operation in any::<[u8;16]>(),
        interrupted in any::<bool>(),
    ) {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(
                Arc::new(MemoryStream::default()), StreamPath::new("hook-effects")?,
            ).await?;
            let task = TaskId::from_bytes([1;16]);
            let operation = OperationId::from_bytes(operation);
            let first = run_once::<_,_,_,_,_,true>(
                &volume, (task, operation, "session-start"), 4096, true, authorization,
                || async {
                    if interrupted { return Err(Error::Indeterminate(operation)); }
                    Ok(HookOutput { additional_context: vec![text.clone()], ..Default::default() })
                },
            ).await;
            if interrupted {
                prop_assert!(matches!(first, Err(Error::Indeterminate(id)) if id == operation));
            } else {
                prop_assert_eq!(first?.additional_context, vec![text.clone()]);
            }
            let replay = run_once::<_,_,_,_,_,true>(
                &volume, (task, operation, "session-start"), 4096, false, authorization,
                || async { panic!("recovery resent a hook") },
            ).await;
            if interrupted {
                prop_assert!(matches!(replay, Err(Error::Indeterminate(id)) if id == operation));
            } else {
                prop_assert_eq!(replay?.additional_context, vec![text]);
            }
            Ok(())
        })?;
    }
}

#[test]
fn reconciliation_of_an_unstarted_phase_publishes_nothing() -> Result<()> {
    futures::executor::block_on(async {
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("unstarted-hooks")?,
        )
        .await?;
        let head = volume.head().await?;
        let output = run_once::<_, _, _, _, _, true>(
            &volume,
            (
                TaskId::from_bytes([1; 16]),
                OperationId::from_bytes([2; 16]),
                "SessionStart",
            ),
            4096,
            false,
            authorization,
            || async { panic!("reconciliation launched a hook") },
        )
        .await?;
        assert_eq!(output, HookOutput::default());
        assert_eq!(volume.head().await?, head);
        Ok(())
    })
}
