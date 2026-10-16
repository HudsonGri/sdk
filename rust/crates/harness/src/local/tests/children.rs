use super::*;
use crate::{
    conversation::ContentPublisher,
    distributed::{WorkPull, Worker},
    runtime::{RuntimeScope, RuntimeTask, TaskSpawner},
    scheduler::LeaseFence,
};

#[cfg(feature = "native-execution")]
#[tokio::test]
async fn child_request_prepares_a_replayable_lazy_fork_and_runs_under_its_original_owner()
-> Result<()> {
    use crate::task_volume::{ChildFile, Component, FileKey, FileValue, Query};
    use acyclic_fs::facade::FsWork as _;
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::write(directory.path().join("source.txt"), b"parent source")
        .map_err(super::super::storage)?;
    let provider = scripted([ScriptedTurn::Text("child done".into())]);
    let harness = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model()?, provider.clone()),
    ))
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(operation, "parent").await?
    else {
        panic!("parent admission");
    };
    let worker = Worker {
        id: "lazy-child-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease");
    };
    host.start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let context = harness.runtime.task_context(parent, fence.clone()).await?;
    let files = harness.task_files();
    let reference = Component::new("worker").map_err(super::super::storage)?;
    let mut request = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        },
        b"inspect the child files".to_vec(),
    );
    let snapshot = files
        .observe(Query::Child(reference.clone()), None, 8)
        .await
        .map_err(super::super::storage)?
        .snapshot
        .unwrap();
    let (condition, deadline) = context
        .owned_mailbox
        .as_ref()
        .unwrap()
        .file_publication_condition()
        .await?;
    files
        .write_if(snapshot.generation, &request, parent, condition, deadline)
        .await
        .map_err(super::super::storage)?;
    request.requester = Some(parent);
    let (child, view) = harness.admit_child_request(files, &request).await?;
    assert_ne!(
        view.workspace().id(),
        harness.workspace().unwrap().view().workspace().id()
    );
    assert!(provider.requests().is_empty());
    let admitted = host.observe_admission(child).await?;
    assert_eq!(admitted.parent, Some(parent));
    assert!(!admitted.resource_proofs.is_empty());
    assert!(
        !admitted.grants.contains(
            &harness
                .project()
                .capability(VolumeOperation::Write, &mut None)?
        )
    );
    assert!(
        !admitted
            .grants
            .contains(&harness.project().directory_read_capability("", &mut None)?)
    );
    let WorkPull::Claimed(lease) = host
        .pull_work(&worker, Some(admitted.operation_id), &mut None)
        .await?
    else {
        panic!("child lease");
    };
    host.start_task(&lease, &mut None).await?;
    let child_context = harness
        .runtime
        .task_context(child, LeaseFence::from(&lease.reservation))
        .await?;
    let command: crate::workflow::WorkflowCommand =
        serde_json::from_value(admitted.input["model"].clone()).map_err(super::super::storage)?;
    let bytes = child_context.read_file(&command.payload, None).await?;
    let model: crate::filesystem::ModelTaskCommand =
        crate::contract::json_from_slice(&bytes).map_err(super::super::storage)?;
    let project = model.selected_output_volume.unwrap();
    child_context
        .stage_file(
            Some(&project),
            OperationId::new(),
            "child.txt",
            b"child only",
            "text/plain",
            "child.txt",
        )
        .await?;
    assert!(!directory.path().join("child.txt").exists());
    let cancel = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::WorkBudget::UNBOUNDED;
    let mut accounting = budget.accounting(&cancel);
    assert_eq!(
        view.read("/source.txt", 1024, &mut accounting)
            .await
            .map_err(super::super::storage)?
            .bytes
            .as_ref(),
        b"parent source"
    );
    assert_eq!(
        view.read("/child.txt", 1024, &mut accounting)
            .await
            .map_err(super::super::storage)?
            .bytes
            .as_ref(),
        b"child only"
    );
    // Property: child fs authority is exactly its sealed project binding plus
    // the tool grant and original call identity, even after scope attenuation.
    {
        use proptest::prelude::*;
        let child_context = Arc::new(child_context);
        let tool = harness.tools().get("fs", None, None)?.unwrap();
        let read = project.capability(VolumeOperation::Read, &mut None)?;
        let write = project.capability(VolumeOperation::Write, &mut None)?;
        proptest::test_runner::TestRunner::new(ProptestConfig {
            source_file: Some(file!()),
            ..ProptestConfig::with_cases(32)
        })
        .run(&(any::<[u8; 16]>(), "[a-z]{1,16}"), |(bytes, call)| {
            for mask in 0u8..32 {
                let grants = [read.as_str(), write.as_str(), "tool:call:fs"]
                    .into_iter()
                    .enumerate()
                    .filter(|(index, _)| mask & (1 << index) != 0)
                    .map(|(_, grant)| grant.to_owned());
                let narrowed = child_context
                    .scoped(
                        crate::Capabilities::new(grants),
                        child_context.scope().limits(),
                    )
                    .unwrap();
                let context = ToolContext::new(
                    Arc::new(narrowed),
                    OperationId::from_bytes(bytes),
                    &call,
                    None,
                )
                .unwrap();
                let mut actual = bytes;
                if mask & 8 == 0 {
                    actual[0] ^= 1;
                }
                let invocation = ToolInvocation {
                    operation_id: OperationId::from_bytes(actual),
                    call_id: if mask & 16 != 0 {
                        call.clone()
                    } else {
                        format!("{call}-other")
                    },
                    name: "fs".into(),
                    arguments: json!({"command":"status"}),
                };
                prop_assert_eq!(
                    tool.executor
                        .authorize(None, Some(&context), &invocation, None)
                        .is_ok(),
                    mask == 31
                );
            }
            Ok(())
        })
        .map_err(super::super::storage)?;
    }
    let publisher = harness.input_publisher_with_limit(harness.limits.file_bytes)?;
    let message = OperationId::new();
    let (payload, _) = publisher
        .stage_once(
            message,
            "inputs/lazy-child-request.md",
            b"inspect the child files",
            "text/plain",
            "request.md",
            None,
        )?
        .await?;
    host.send_mail(parent, &fence, child, message, payload, None)
        .await?;
    harness.advance(child, &harness.commands()?).await?;
    assert_eq!(provider.requests().len(), 1);
    assert!(host.outcome(child).await?.is_none());
    let (replayed, reopened) = harness.admit_child_request(files, &request).await?;
    assert_eq!(replayed, child);
    assert_eq!(reopened.workspace().id(), view.workspace().id());
    let mut changed = request.clone();
    changed.bytes = acyclic_stream::OwnedBytes::from_static(b"forged replacement");
    assert!(harness.admit_child_request(files, &changed).await.is_err());
    host.cancel(child).await?;
    host.cancel(parent).await?;
    Ok(())
}

#[tokio::test]
async fn child_agent_keeps_its_admission_and_waits_after_each_received_turn() -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let provider = scripted([
            ScriptedTurn::Text("first child turn".into()),
            ScriptedTurn::Text("second child turn".into()),
        ]);
        let harness =
            LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
        let parent_operation = OperationId::new();
        let Admission::Accepted(RuntimeTask::Durable {
            task_id: parent, ..
        }) = harness.submit(parent_operation, "parent").await?
        else {
            panic!("parent admission");
        };
        let worker = Worker {
            id: "child-agent-test".into(),
            available: Default::default(),
            labels: Default::default(),
        };
        let host = harness.runtime.task_host();
        let WorkPull::Claimed(lease) = host
            .pull_work(&worker, Some(parent_operation), &mut None)
            .await?
        else {
            panic!("parent lease");
        };
        host.start_task(&lease, &mut None).await?;
        let fence = LeaseFence::from(&lease.reservation);
        let admitted = host.observe_admission(parent).await?;
        let scope = RuntimeScope::new(admitted.grants, admitted.limits)?
            .with_run_limits(admitted.run_limits, &mut None)?;
        let operation = OperationId::new();
        let (receive, model) = harness
            .stage_turn_with_limits(
                operation,
                "follow the received child request",
                scope.limits(),
            )
            .await?;
        let definition = harness
            .runtime
            .harness()
            .task::<serde_json::Value, serde_json::Value>("acyclic.child_agent@1")?;
        let record = harness
            .runtime
            .harness()
            .registered_admission(
                operation,
                &definition.identity().name,
                &definition.identity().version,
                serde_json::json!({"receive":receive,"model":model}),
                Some((parent, &scope)),
                None,
                None,
            )?
            .0;
        let Admission::Accepted(child) = host.admit_owned(record, fence.clone()).await? else {
            panic!("child admission");
        };
        let publisher = crate::filesystem::FilesystemContentPublisher::new(
            harness.files.clone(),
            &harness.journal,
            &harness.verifier,
            &harness.signed,
            scope.limits().file_bytes,
            None,
        )?;
        use crate::runtime::TaskStateProvider as _;
        for (index, text) in [(1, "first request"), (2, "second request")] {
            let message = OperationId::new();
            let (payload, _) = publisher
                .stage_once(
                    message,
                    &format!("inputs/child-agent/{index}.md"),
                    text.as_bytes(),
                    "text/plain",
                    "request.md",
                    None,
                )?
                .await?;
            host.send_mail(parent, &fence, child, message, payload, None)
                .await?;
            harness.advance(child, &harness.commands()?).await?;
            let outcome = host.outcome(child).await?;
            assert!(
                outcome.is_none(),
                "child stopped after input {index}: {outcome:?}"
            );
            assert_eq!(provider.requests().len(), index);
            if index == 1 {
                let (hidden, _) = publisher
                    .stage_once(
                        OperationId::new(),
                        "private/unreadable.json",
                        b"1",
                        "application/json",
                        "private.json",
                        None,
                    )?
                    .await?;
                let slot = host.workflow_suspension(child, &mut None).await?.unwrap();
                assert!(matches!(
                    host.resume_workflow(
                        child,
                        slot.revision,
                        hidden,
                        crate::IdempotencyKey::new("unreadable-wake")?,
                        slot.waiting_command,
                        &mut None,
                    )
                    .await,
                    Err(crate::Error::Unauthorized(_))
                ));
                let (readable, _) = publisher
                    .stage_once(
                        OperationId::new(),
                        "inputs/readable-wake.json",
                        b"1",
                        "application/json",
                        "readable.json",
                        None,
                    )?
                    .await?;
                host.resume_workflow(
                    child,
                    slot.revision,
                    readable,
                    crate::IdempotencyKey::new("readable-wake")?,
                    slot.waiting_command,
                    &mut None,
                )
                .await?;
                let WorkPull::Claimed(resumed) =
                    host.pull_work(&worker, Some(operation), &mut None).await?
                else {
                    panic!("readable input did not wake the child");
                };
                host.start_task(&resumed, &mut None).await?;
                assert_eq!(
                    host.workflow_input(child, &LeaseFence::from(&resumed.reservation), None)
                        .await?,
                    Some((slot.revision, serde_json::json!(1)))
                );
            }
            harness.advance(child, &harness.commands()?).await?;
            assert_eq!(provider.requests().len(), index);
        }
        host.cancel(child).await?;
        assert!(matches!(
            host.outcome(child).await?,
            Some(crate::Outcome::Cancelled)
        ));
        host.cancel(parent).await?;
        Ok::<(), crate::Error>(())
    })
    .await
    .map_err(super::super::storage)?
}

#[tokio::test]
async fn child_turn_admission_replays_under_the_parent_lease_and_shared_depth_limit() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([]);
    let mut config = volume_config(directory.path(), project, provider.clone())?;
    config.session_limits.depth = 1;
    let harness = LocalHarness::open(config).await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = harness.submit(operation, "parent").await?
    else {
        panic!("parent admission was not accepted");
    };
    let worker = Worker {
        id: "child-admission-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let host = harness.runtime.task_host();
    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("parent lease was not claimed");
    };
    host.start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    let admitted = host.observe_admission(parent).await?;
    let parent_scope = RuntimeScope::new(admitted.grants, admitted.limits)?
        .with_run_limits(admitted.run_limits, &mut None)?;
    let scope = crate::task_volume::child_scope(&parent_scope, Some(&["read".into()]))?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable { task_id: child, .. }) = harness
        .submit_child_turn(operation, "reader", (parent, &scope, &fence))
        .await?
    else {
        panic!("child admission was not accepted");
    };
    assert!(matches!(
        harness.submit_child_turn(operation, "reader", (parent, &scope, &fence)).await?,
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) if task_id == child
    ));
    let mut stale = fence.clone();
    stale.reservation_id.push_str("-stale");
    assert!(
        harness
            .submit_child_turn(operation, "reader", (parent, &scope, &stale))
            .await
            .is_err()
    );
    let admitted = host.observe_admission(child).await?;
    assert_eq!(admitted.parent, Some(parent));
    assert_eq!(admitted.grants, *scope.grants());
    assert_eq!(admitted.limits, scope.limits());
    assert_eq!(admitted.run_limits, scope.run_limits());
    assert!(admitted.grants.contains("tool:call:read"));
    assert!(!admitted.grants.contains("tool:call:write"));
    let children = harness.runtime.children(parent, None, None, 8).await?;
    assert_eq!(children.entries.len(), 1);
    assert_eq!(children.entries[0].task_id, child);

    let WorkPull::Claimed(lease) = host.pull_work(&worker, Some(operation), &mut None).await?
    else {
        panic!("child lease was not claimed");
    };
    host.start_task(&lease, &mut None).await?;
    let fence = LeaseFence::from(&lease.reservation);
    assert!(matches!(
        harness
            .submit_child_turn(
                OperationId::new(),
                "too deep",
                (child, &scope, &fence)
            )
            .await,
        Err(Error::Invalid(reason)) if reason.contains("depth limit exhausted")
    ));
    assert!(
        harness
            .runtime
            .children(child, None, None, 8)
            .await?
            .entries
            .is_empty()
    );
    assert!(provider.requests().is_empty());
    Ok(())
}
