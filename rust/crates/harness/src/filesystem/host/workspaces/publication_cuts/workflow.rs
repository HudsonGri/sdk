use super::*;
use crate::{
    AgentId, Capabilities, OperationId as WorkflowOperationId,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{FilesystemJournalBinding, FilesystemWorkflowJournal},
    workflow::{
        MachineCheckpoint, MachineIdentity, MachineStatus, MachineTransition, WorkflowAdmission,
        WorkflowCommand, WorkflowJournal, WorkflowRecord,
    },
};
use acyclic_stream::StreamProvider;
use serde_json::json;

mod provider;

async fn schedule<P: StreamProvider>(stream: Arc<P>, delayed: u8) -> Result<Error> {
    let barrier = Arc::new(Mutex::new(None));
    let stream = Arc::new(provider::ScheduledStream {
        inner: stream,
        barrier: barrier.clone(),
    });
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let filesystem = Fs::new(
        acyclic_fs::StreamAuthorityStore::new(stream.clone()),
        acyclic_fs::LogicalObjectStore::new(Arc::new(objects), bucket),
        EmbeddedCapabilities::MEMORY,
    );
    let provider = ProviderRef::new(
        "workflow-cut".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        format!("workflow-race-{delayed}"),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "workflow-cut",
        [6; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "race".into(),
        },
    );
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([4; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let verifier = issuer.verifier(&mut None).expect("verifier copy");
    let grant = crate::conversation::ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let payload = host
        .put_content(
            &volume,
            &grant,
            "commands/count.json",
            b"3",
            "application/json",
            "count.json",
            65_536,
            &IdempotencyKey::new("command-payload")?,
            None,
            None,
        )
        .await?;
    let workflow_id = format!("race-{delayed}");
    let binding = || FilesystemJournalBinding {
        volume: &volume,
        verifier: &verifier,
        scope: &scope,
        maximum_bytes: 65_536,
        io: None,
    };
    let journal =
        FilesystemWorkflowJournal::new(stream.clone(), host.clone(), &workflow_id, binding())?;
    let other = FilesystemWorkflowJournal::new(stream, host, &workflow_id, binding())?;
    let initial = MachineCheckpoint {
        machine: MachineIdentity {
            name: "counter".into(),
            version: "1".into(),
            digest: [7; 32],
        },
        revision: 0,
        state: json!(0),
    };
    journal
        .admit(WorkflowAdmission {
            operation_id: WorkflowOperationId::from_bytes([9; 16]),
            request_digest: [5; 32],
            initial: initial.clone(),
        })
        .await?;
    let record = |operation| -> Result<WorkflowRecord> {
        Ok(WorkflowRecord {
            operation_id: WorkflowOperationId::from_bytes([operation; 16]),
            idempotency_key: IdempotencyKey::new(format!("candidate-{operation}"))?,
            prior: initial.clone(),
            input: json!(3),
            transition: MachineTransition {
                state: json!(3),
                status: MachineStatus::Suspended,
                commands: vec![WorkflowCommand {
                    operation_id: WorkflowOperationId::from_bytes([15; 16]),
                    kind: "counter.publish".into(),
                    payload: payload.clone(),
                }],
            },
        })
    };
    let first = record(delayed)?;
    let second = record(if delayed == 13 { 14 } else { 13 })?;
    let (entered, entering) = oneshot::channel();
    let (release, released) = oneshot::channel();
    *barrier.lock().unwrap() = Some(Barrier {
        entered,
        release: released,
    });
    let pending = journal.commit(0, first.idempotency_key.clone(), first.clone());
    tokio::pin!(pending);
    tokio::select! {
        result = &mut pending => panic!("workflow finished before its scheduled append: {result:?}"),
        entered = entering => entered.unwrap(),
    }
    assert_eq!(
        other
            .commit(0, second.idempotency_key.clone(), second.clone())
            .await?,
        second
    );
    release.send(()).unwrap();
    let loser = pending
        .await
        .expect_err("both competing workflow records committed");
    assert_eq!(journal.replay(0, 8, None)?.await?.0, vec![second]);
    Ok(loser)
}

fn assert_conflict(error: &Error) {
    assert!(
        matches!(error, Error::Conflict(_)),
        "lost workflow commit race: {error:?}"
    );
}

#[tokio::test]
async fn competing_workflow_commits_preserve_the_conflict_class() -> Result<()> {
    for delayed in [13, 14] {
        assert_conflict(
            &schedule(Arc::new(acyclic_stream::MemoryStream::default()), delayed).await?,
        );
    }
    Ok(())
}

#[cfg(feature = "grpc")]
#[tokio::test]
async fn http_competing_workflow_commits_preserve_the_conflict_class() -> Result<()> {
    let (server, client) = http_client().await;
    for delayed in [13, 14] {
        assert_conflict(&schedule(Arc::new(client.clone()), delayed).await?);
    }
    server.abort();
    Ok(())
}
