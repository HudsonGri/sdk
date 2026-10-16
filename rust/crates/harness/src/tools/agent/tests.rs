use super::*;
use crate::task_volume::ChildStatus;
use crate::{
    AgentId, Capabilities, OperationId, TaskId,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    resources::ProviderRef,
};
use acyclic_stream::MemoryStream;
use proptest::prelude::*;
use std::sync::Arc;

async fn files() -> TaskFiles<MemoryStream> {
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
        "agent-tool".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )
    .unwrap();
    let issuer = AuthorityIssuer::new(
        "agent-tool",
        [1; 32],
        Authority {
            kind: AggregateKind::Task,
            id: TaskId::from_bytes([2; 16]).to_string(),
        },
    );
    let grants = [VolumeOperation::Read, VolumeOperation::Write]
        .map(|operation| volume.capability(operation, &mut None).unwrap());
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new(grants),
            Some(agent),
            None,
            &mut None,
        )
        .unwrap();
    TaskFiles::open(
        Arc::new(MemoryStream::default()),
        &volume,
        &issuer.verifier(&mut None).unwrap(),
        &scope,
        4096,
    )
    .await
    .unwrap()
}

fn limits() -> Limits {
    Limits {
        file_bytes: 4096,
        render_bytes: 4096,
        attachments: 16,
        ..Limits::default()
    }
}

async fn acknowledge(files: &TaskFiles<MemoryStream>, state: ChildState) {
    let snapshot = files
        .observe(Query::Child(Component::new("worker").unwrap()), None, 16)
        .await
        .unwrap()
        .snapshot
        .unwrap();
    let previous = snapshot.files.iter().find_map(|file| match &file.key {
        FileKey::Child {
            file: ChildFile::Status,
            ..
        } => Some(ChildStatus::parse(&file.bytes).unwrap()),
        _ => None,
    });
    let inputs = if matches!(state, ChildState::Cancelled | ChildState::Archived) {
        previous.map(|status| status.inputs)
    } else {
        None
    }
    .unwrap_or_else(|| {
        snapshot
            .files
            .iter()
            .filter(|file| {
                matches!(
                    &file.key,
                    FileKey::Child {
                        file: ChildFile::Request | ChildFile::Inbox(_),
                        ..
                    }
                )
            })
            .count() as u64
    });
    let status = ChildStatus {
        task: previous
            .map(|status| status.task)
            .unwrap_or(crate::TaskId::from_bytes([7; 16])),
        state,
        settled: true,
        inputs,
    };
    let reference = Component::new("worker").unwrap();
    let status_key = FileKey::Child {
        reference: reference.clone(),
        file: ChildFile::Status,
    };
    let mut generation = snapshot.generation;
    if state == ChildState::Running {
        let pending = ChildStatus {
            settled: false,
            ..status
        };
        generation = files
            .inner
            .append(
                generation,
                &FileValue::new(status_key.clone(), serde_json::to_vec(&pending).unwrap()),
            )
            .await
            .unwrap();
        for turn in 1..=inputs {
            let key = FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Result(turn),
            };
            if snapshot.files.iter().any(|file| file.key == key) {
                continue;
            }
            generation = files.inner.append(generation, &FileValue::new(key,
                serde_json::to_vec(&json!({"task":status.task,"turn":turn,
                    "outcome":{"Succeeded":{"text":"child answer","metadata":null,"steps":1,"attachments":[]}}})).unwrap(),
            )).await.unwrap();
        }
    }
    files
        .inner
        .append(
            generation,
            &FileValue::new(status_key, serde_json::to_vec(&status).unwrap()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn spawn_admission_waits_for_the_harness_status_file() {
    let files = files().await;
    let reference = Component::new("worker").unwrap();
    call(
        &files,
        json!({"command":"spawn", "reference":"worker", "prompt":"work"}),
        OperationId::new(),
    )
    .await
    .unwrap();
    assert!(
        admission_status(&files, &reference, limits(), false)
            .await
            .unwrap()
            .is_none()
    );
    let (status, ()) = futures::join!(
        admission_status(&files, &reference, limits(), true),
        acknowledge(&files, ChildState::Running),
    );
    let status = status.unwrap().unwrap();
    assert_eq!(
        status["execution"]["task"],
        json!(TaskId::from_bytes([7; 16]))
    );
}

async fn call(
    files: &TaskFiles<MemoryStream>,
    command: Value,
    operation: OperationId,
) -> Result<Value> {
    access(
        files,
        serde_json::from_value(command).unwrap(),
        operation,
        limits(),
        false,
        None,
    )
    .await
    .map(|result| result.unwrap())
}

#[tokio::test]
async fn spawn_and_send_replay_the_same_files_and_archive_remains_inspectable() {
    let files = files().await;
    let operation = OperationId::from_bytes([3; 16]);
    let spawn = json!({"command":"spawn","reference":"worker","prompt":"work"});
    for _ in 0..2 {
        call(&files, spawn.clone(), operation).await.unwrap();
    }
    let send = json!({"command":"send","reference":"worker","message":"more work"});
    for _ in 0..2 {
        call(&files, send.clone(), operation).await.unwrap();
    }
    let snapshot = files
        .observe(Query::Children, None, 16)
        .await
        .unwrap()
        .snapshot
        .unwrap();
    assert_eq!(snapshot.files.len(), 2);
    assert!(snapshot.files.iter().any(
        |file| file.key.path() == "agents/worker/request.md" && file.bytes.as_ref() == b"work"
    ));
    assert!(
        call(
            &files,
            json!({"command":"send","reference":"worker","message":"changed"}),
            operation
        )
        .await
        .is_err()
    );
    let archived = call(
        &files,
        json!({"command":"control","reference":"worker","action":"archive"}),
        operation,
    )
    .await
    .unwrap();
    assert_eq!(archived["state"], json!(ChildState::Archived));
    assert_eq!(archived["settled"], false);
    assert_eq!(
        call(&files, json!({"command":"list"}), operation)
            .await
            .unwrap(),
        json!({"agents":[archived]})
    );
    acknowledge(&files, ChildState::Archived).await;
    let archived = call(
        &files,
        json!({"command":"status","reference":"worker"}),
        operation,
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &files,
            json!({"command":"wait","reference":"worker"}),
            operation
        )
        .await
        .unwrap(),
        archived
    );
    assert_eq!(
        call(
            &files,
            json!({"command":"status","reference":"worker"}),
            operation
        )
        .await
        .unwrap(),
        archived
    );
    assert_eq!(
        call(&files, json!({"command":"list"}), operation)
            .await
            .unwrap(),
        json!({"agents":[]})
    );
    assert!(
        call(
            &files,
            json!({"command":"control","reference":"worker","action":"resume"}),
            operation
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn wait_follows_the_existing_snapshot_cut_and_sees_control_without_polling() {
    let files = files().await;
    let operation = OperationId::from_bytes([3; 16]);
    call(
        &files,
        json!({"command":"spawn","reference":"worker","prompt":"work"}),
        operation,
    )
    .await
    .unwrap();
    let mut waiter = Box::pin(call(
        &files,
        json!({"command":"wait","reference":"worker"}),
        operation,
    ));
    assert!(matches!(
        futures::poll!(&mut waiter),
        std::task::Poll::Pending
    ));
    call(
        &files,
        json!({"command":"control","reference":"worker","action":"interrupt"}),
        operation,
    )
    .await
    .unwrap();
    assert!(matches!(
        futures::poll!(&mut waiter),
        std::task::Poll::Pending
    ));
    acknowledge(&files, ChildState::Interrupted).await;
    let result = waiter.await.unwrap();
    assert_eq!(result["state"], json!(ChildState::Interrupted));
    assert_eq!(result["settled"], true);
}

#[tokio::test]
async fn reconciliation_does_not_reapply_a_superseded_control() {
    let files = files().await;
    let operation = OperationId::from_bytes([3; 16]);
    call(
        &files,
        json!({"command":"spawn","reference":"worker","prompt":"work"}),
        operation,
    )
    .await
    .unwrap();
    let pause = json!({"command":"control","reference":"worker","action":"pause"});
    call(&files, pause.clone(), operation).await.unwrap();
    let paused = access(
        &files,
        serde_json::from_value(pause.clone()).unwrap(),
        operation,
        limits(),
        true,
        None,
    )
    .await
    .unwrap();
    assert!(paused.is_some());
    call(
        &files,
        json!({"command":"control","reference":"worker","action":"resume"}),
        operation,
    )
    .await
    .unwrap();
    let before = files
        .observe(Query::Children, None, 16)
        .await
        .unwrap()
        .snapshot
        .unwrap()
        .generation;
    assert!(
        access(
            &files,
            serde_json::from_value(pause).unwrap(),
            operation,
            limits(),
            true,
            None,
        )
        .await
        .unwrap()
        .is_none()
    );
    let after = files
        .observe(Query::Children, None, 16)
        .await
        .unwrap()
        .snapshot
        .unwrap()
        .generation;
    assert_eq!(before, after);
    assert_eq!(
        call(
            &files,
            json!({"command":"status","reference":"worker"}),
            operation
        )
        .await
        .unwrap()["state"],
        json!(ChildState::Running)
    );
}

#[tokio::test]
async fn new_inbox_files_make_a_previously_settled_status_pending() {
    let files = files().await;
    let operation = OperationId::from_bytes([3; 16]);
    call(
        &files,
        json!({"command":"spawn","reference":"worker","prompt":"work"}),
        operation,
    )
    .await
    .unwrap();
    acknowledge(&files, ChildState::Running).await;
    assert_eq!(
        call(
            &files,
            json!({"command":"wait","reference":"worker"}),
            operation
        )
        .await
        .unwrap()["settled"],
        true
    );
    call(
        &files,
        json!({"command":"send","reference":"worker","message":"more work"}),
        operation,
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &files,
            json!({"command":"status","reference":"worker"}),
            operation
        )
        .await
        .unwrap()["settled"],
        false
    );
    let mut waiter = Box::pin(call(
        &files,
        json!({"command":"wait","reference":"worker"}),
        operation,
    ));
    assert!(matches!(
        futures::poll!(&mut waiter),
        std::task::Poll::Pending
    ));
    call(
        &files,
        json!({"command":"control","reference":"worker","action":"cancel"}),
        operation,
    )
    .await
    .unwrap();
    assert!(matches!(
        futures::poll!(&mut waiter),
        std::task::Poll::Pending
    ));
    acknowledge(&files, ChildState::Cancelled).await;
    assert_eq!(waiter.await.unwrap()["state"], json!(ChildState::Cancelled));
}

#[tokio::test]
async fn agent_file_access_retains_the_callers_file_and_snapshot_bounds() {
    let files = files().await;
    let operation = OperationId::from_bytes([3; 16]);
    let spawn = json!({"command":"spawn","reference":"worker","prompt":"four"});
    let before = files
        .observe(Query::Children, None, 16)
        .await
        .unwrap()
        .snapshot
        .unwrap()
        .generation;
    let restricted = Limits {
        file_bytes: 3,
        ..limits()
    };
    assert!(
        access(
            &files,
            serde_json::from_value(spawn.clone()).unwrap(),
            operation,
            restricted,
            false,
            None,
        )
        .await
        .is_err()
    );
    assert_eq!(
        files
            .observe(Query::Children, None, 16)
            .await
            .unwrap()
            .snapshot
            .unwrap()
            .generation,
        before
    );
    call(&files, spawn, operation).await.unwrap();
    let restricted = Limits {
        render_bytes: 3,
        ..limits()
    };
    assert!(
        access(
            &files,
            AgentCommand::Status {
                reference: Component::new("worker").unwrap()
            },
            operation,
            restricted,
            false,
            None,
        )
        .await
        .is_err()
    );
}

proptest! {
    #[test]
    fn tool_arguments_round_trip(reference in "[a-z][a-z0-9_-]{0,20}", body in ".{1,100}") {
        let reference = Component::new(reference).unwrap();
        for command in [
            AgentCommand::Spawn { reference: reference.clone(), prompt: body.clone(), model: None, tools: Some(vec!["read".into()]) },
            AgentCommand::Send { reference: reference.clone(), message: body },
            AgentCommand::Control { reference: reference.clone(), action: Control::Archive },
            AgentCommand::Wait { reference: reference.clone() },
            AgentCommand::Status { reference }, AgentCommand::List,
        ] {
            prop_assert_eq!(serde_json::from_slice::<AgentCommand>(&serde_json::to_vec(&command).unwrap()).unwrap(), command);
        }
    }
}
