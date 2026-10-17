use super::*;
use crate::{
    context::ModelContextCapacity,
    distributed::{WorkPull, Worker},
    model::Model,
    runtime::RuntimeTask,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};

#[tokio::test]
async fn cancelled_child_reopen_repairs_a_lost_settled_ack_without_a_new_lease() -> Result<()> {
    for control in [Control::Cancel, Control::Archive] {
        let directory = tempfile::tempdir().map_err(storage)?;
        let model = Model::new(
            "scripted".into(),
            "recovery".into(),
            "1".into(),
            serde_json::Value::Null,
        )?;
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("retained answer".into())])
                .with_context_capacity(ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: 4096,
                }),
        );
        let config = || {
            crate::test_support::local_harness_config(
                directory.path(),
                (model.clone(), provider.clone()),
            )
        };
        let harness = LocalHarness::open(config()?).await?;
        let operation = OperationId::new();
        let Admission::Accepted(RuntimeTask::Durable {
            task_id: parent, ..
        }) = harness.submit(operation, "parent").await?
        else {
            panic!("parent admission")
        };
        let host = harness.runtime.task_host();
        let worker = Worker {
            id: "cancelled-reopen-test".into(),
            available: Default::default(),
            labels: Default::default(),
        };
        let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
        else {
            panic!("parent lease")
        };
        host.start_task(&lease, &mut None).await?;
        let context = harness
            .runtime
            .task_context(parent, LeaseFence::from(&lease.reservation))
            .await?;
        let files = Arc::clone(&harness.task_files);
        let reference = Component::new("worker").map_err(storage)?;
        let mut request = file(&reference, ChildFile::Request, b"answer".to_vec());
        let generation = files.inner.head().await.map_err(storage)?;
        let (condition, deadline) = context
            .owned_mailbox
            .as_ref()
            .unwrap()
            .file_publication_condition()
            .await?;
        files
            .write_if(generation, &request, parent, condition, deadline)
            .await
            .map_err(storage)?;
        request.requester = Some(parent);
        harness
            .service_child(files.clone(), reference.clone(), request.clone(), None)
            .await?;
        let snapshot = files
            .observe(Query::Child(reference.clone()), None, 8)
            .await
            .map_err(storage)?
            .snapshot
            .unwrap();
        let values = snapshot
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect();
        let before = execution(&reference, &values)?.unwrap();
        let result_key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Result(1),
        };
        let retained_result = values[&result_key].clone();
        assert!(before.settled);
        assert_eq!(before.state, ChildState::Running);
        files
            .write(
                snapshot.generation,
                &file(&reference, ChildFile::Control, control.bytes().to_vec()),
            )
            .await
            .map_err(storage)?;
        // This is the actual durable crash cut: effects and results have settled,
        // cancellation is accepted, but the terminal status has not been written.
        host.cancel(before.task).await?;
        assert!(matches!(
            host.outcome(before.task).await?,
            Some(crate::Outcome::Cancelled)
        ));
        let checkpoint = host.cancelled_workflow_suspension(before.task, &mut None).await?;
        assert!(checkpoint.is_some(), "cancellation must retain its receive cut");
        drop(context);
        drop(files);
        harness.close().await?;

        let reopened = LocalHarness::open(config()?).await?;
        let files = Arc::clone(&reopened.task_files);
        reopened
            .service_child(files.clone(), reference.clone(), request, None)
            .await?;
        let snapshot = files
            .observe(Query::Child(reference.clone()), None, 8)
            .await
            .map_err(storage)?
            .snapshot
            .unwrap();
        let values = snapshot
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect();
        let after = execution(&reference, &values)?.unwrap();
        assert_eq!(after.task, before.task);
        assert_eq!(after.inputs, before.inputs);
        assert_eq!(after.state, ChildState::Running.apply(control));
        assert!(after.settled);
        assert_eq!(values[&result_key], retained_result);
        assert_eq!(
            reopened
                .runtime
                .task_host()
                .cancelled_workflow_suspension(before.task, &mut None)
                .await?,
            checkpoint
        );
        assert_eq!(provider.requests().len(), 1);
        reopened.close().await?;
    }
    Ok(())
}
