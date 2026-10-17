//! Crash cuts at accepted production boundaries; absent from release builds.
use super::*;
use crate::{
    context::ModelContextCapacity,
    distributed::{WorkPull, Worker},
    runtime::{RuntimeTask, TaskStateProvider, ToolContext},
    task_volume::{ChildFile, Component, Control, FileKey, FileValue, Query},
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolInvocation,
};
use acyclic_fs::facade::FsWork as _;
use std::cell::Cell;

thread_local! {
    static CUT: Cell<Option<&'static str>> = const { Cell::new(None) };
}

pub(super) fn stop(point: &'static str, operation: OperationId) -> Result<()> {
    if CUT.with(|cut| cut.get() == Some(point)) {
        CUT.with(|cut| cut.set(None));
        return Err(Error::Indeterminate(operation));
    }
    Ok(())
}

#[tokio::test]
async fn child_file_recovery_reuses_preparation_admission_and_status_cuts_before_merge()
-> Result<()> {
    for point in ["prepared", "admitted", "status"] {
        let directory = tempfile::tempdir().map_err(storage)?;
        std::fs::write(directory.path().join("source.txt"), "original").map_err(storage)?;
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("retained answer".into())])
                .with_context_capacity(ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: 4096,
                }),
        );
        let model = crate::model::Model::new(
            "scripted".into(),
            "recovery".into(),
            "1".into(),
            serde_json::Value::Null,
        )?;
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
        let worker = Worker {
            id: "child-file-crash-cuts".into(),
            available: Default::default(),
            labels: Default::default(),
        };
        let host = harness.runtime.task_host();
        let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
        else {
            panic!("parent lease")
        };
        host.start_task(&lease, &mut None).await?;
        let fence = LeaseFence::from(&lease.reservation);
        let context = harness.runtime.task_context(parent, fence.clone()).await?;
        let files = Arc::clone(&harness.task_files);
        let reference = Component::new("worker").map_err(storage)?;
        let mut request = FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Request,
            },
            b"answer".to_vec(),
        );
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
        let expected = TaskId::from_bytes(
            admission::child_operation(files.volume(), reference.as_str(), b"child")?.into_bytes(),
        );
        CUT.with(|cut| cut.set(Some(point)));
        assert!(
            matches!(
                harness.service_child(files.clone(), reference.clone(), request.clone(), None).await,
                Err(Error::Indeterminate(operation)) if operation == OperationId::from_bytes(expected.into_bytes())
            ),
            "crash cut {point} was not reached"
        );
        assert!(CUT.with(|cut| cut.get().is_none()));
        assert!(provider.requests().is_empty());
        drop(context);
        drop(files);
        harness.close().await?;

        let reopened = LocalHarness::open(config()?).await?;
        let files = Arc::clone(&reopened.task_files);
        let (child, view) = reopened
            .admit_child_request(files.as_ref(), &request)
            .await?;
        assert_eq!(child, expected);
        let (same, replay) = reopened
            .admit_child_request(files.as_ref(), &request)
            .await?;
        assert_eq!(
            (same, replay.workspace().id()),
            (child, view.workspace().id())
        );
        let parent_context = reopened.runtime.task_context(parent, fence.clone()).await?;
        parent_context
            .stage_file(
                Some(reopened.project()),
                OperationId::new(),
                "source.txt",
                b"changed after cut",
                "text/plain",
                "source.txt",
            )
            .await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget::UNBOUNDED;
        let mut accounting = budget.accounting(&cancellation);
        assert_eq!(
            view.read("/source.txt", 1024, &mut accounting)
                .await
                .map_err(storage)?
                .bytes
                .as_ref(),
            b"original"
        );
        drop(accounting);
        drop(parent_context);
        let host = reopened.runtime.task_host();
        let WorkPull::Claimed(lease) = host
            .pull_work(
                &worker,
                Some(OperationId::from_bytes(child.into_bytes())),
                &mut None,
            )
            .await?
        else {
            panic!("child lease")
        };
        host.start_task(&lease, &mut None).await?;
        let context = reopened
            .runtime
            .task_context(child, LeaseFence::from(&lease.reservation))
            .await?;
        let admitted = host.observe_admission(child).await?;
        let command: crate::workflow::WorkflowCommand =
            serde_json::from_value(admitted.input["model"].clone()).map_err(storage)?;
        let bytes = context.read_file(&command.payload, None).await?;
        let model: crate::filesystem::ModelTaskCommand =
            crate::contract::json_from_slice(&bytes).map_err(storage)?;
        let project = model.selected_output_volume.unwrap();
        context
            .stage_file(
                Some(&project),
                OperationId::new(),
                "child.txt",
                b"retained change",
                "text/plain",
                "child.txt",
            )
            .await?;
        drop(context);
        reopened
            .service_child(files.clone(), reference.clone(), request.clone(), None)
            .await?;
        let generation = files.inner.head().await.map_err(storage)?;
        files
            .write(
                generation,
                &FileValue::new(
                    FileKey::Child {
                        reference: reference.clone(),
                        file: ChildFile::Control,
                    },
                    Control::Archive.bytes().to_vec(),
                ),
            )
            .await
            .map_err(storage)?;
        reopened
            .service_child(files.clone(), reference.clone(), request, None)
            .await?;
        let result = files
            .observe(
                Query::File(FileKey::Child {
                    reference,
                    file: ChildFile::Result(1),
                }),
                None,
                1,
            )
            .await
            .map_err(storage)?
            .snapshot
            .unwrap();
        let result =
            crate::task_volume::ChildTurnResult::parse(&result.files[0].bytes).map_err(storage)?;
        assert!(
            matches!(result.outcome, crate::Outcome::Succeeded(ref answer)
            if answer.text == "retained answer")
        );
        assert_eq!(provider.requests().len(), 1);
        let parent_context = reopened.runtime.task_context(parent, fence).await?;
        let operation = OperationId::new();
        let context = ToolContext::new(Arc::new(parent_context), operation, "merge", None)?;
        let tool = reopened.tools().get("fs", None, None)?.unwrap();
        let result = tool
            .executor
            .execute(
                ToolInvocation {
                    operation_id: operation,
                    call_id: "merge".into(),
                    name: "fs".into(),
                    arguments: serde_json::json!({"command":"merge agents/worker"}),
                },
                Some(context),
            )
            .await?;
        assert_eq!(result.value["success"], true);
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget::UNBOUNDED;
        let mut accounting = budget.accounting(&cancellation);
        assert_eq!(
            reopened
                .workspace()
                .unwrap()
                .view()
                .read("/child.txt", 1024, &mut accounting)
                .await
                .map_err(storage)?
                .bytes
                .as_ref(),
            b"retained change"
        );
        reopened.close().await?;
    }
    Ok(())
}
