use super::*;
use crate::{
    approvals::{ApprovalPolicy, ApprovalReviewFile, RuleVerdict},
    conversation::FileDescriptor,
    executor::ExecutionJournal as _,
    interaction::{ApprovalDecisionFile, ApprovalSubject},
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskFiles},
};

struct Initialized {
    runtime: Runtime,
    files: Arc<TaskFiles<MemoryStream>>,
    responder: Scope,
    task: TaskId,
    fixture: Fixture,
    _conversation: AuthorityIssuer,
}

async fn setup() -> Result<Initialized> {
    let mut fixture = Fixture::new().await?;
    let grants = Capabilities::new(
        fixture
            .scope
            .grants()
            .iter()
            .chain([crate::contract::capability::INTERACTION_ROUTE]),
    );
    fixture.signed =
        fixture
            .issuer
            .issue("owner", &grants, fixture.signed.agent(), None, &mut None)?;
    fixture.scope = RuntimeScope::new(grants, Limits::default())?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let files = Arc::new(
        TaskFiles::open(
            fixture.stream.clone(),
            &fixture.volume,
            &verifier,
            &fixture.signed,
            65_536,
        )
        .await?,
    );
    let gate = Arc::new(ApprovalPolicy::default().bind()?);
    let mut runtime = fixture.runtime_with_policy(None, Some(gate)).await?;
    runtime.bind_task_files(Some(files.clone()));
    let conversation = AuthorityIssuer::new(
        "approval-binding",
        [113; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "approval-binding".into(),
        },
    );
    let owner = conversation.issue(
        "conversation",
        &Capabilities::new([
            crate::contract::capability::INTERACTION_OPEN.to_owned(),
            fixture
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            fixture
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        ]),
        fixture.signed.agent(),
        None,
        &mut None,
    )?;
    runtime = runtime
        .with_interaction_owner(
            conversation.verifier(&mut None)?,
            SchemaRegistry::new(),
            owner,
            fixture.volume.clone(),
        )?
        .with_approval_authority(&conversation)?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease, &mut None).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let id = crate::InteractionId::from_bytes([114; 16]);
    let responder = conversation.issue(
        "human",
        &Capabilities::new([
            "interaction:resolve".to_owned(),
            format!("interaction:respond:{id}"),
        ]),
        None,
        None,
        &mut None,
    )?;
    Ok(Initialized {
        runtime,
        files,
        responder,
        task,
        fixture,
        _conversation: conversation,
    })
}

async fn request(
    fixture: &Fixture,
    runtime: &Runtime,
    task: TaskId,
    files: &Arc<TaskFiles<MemoryStream>>,
) -> Result<FileValue> {
    let owner = runtime
        .interaction_owner
        .as_ref()
        .ok_or_else(|| Error::Invalid("missing fixture owner".into()))?;
    let journal = FilesystemExecutionJournal::new(
        fixture.stream.clone(),
        fixture.filesystem.clone(),
        &SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &fixture.volume,
            verifier: &fixture.issuer.verifier(&mut None)?,
            scope: &fixture.signed,
            maximum_bytes: 65_536,
            io: None,
        },
    )?
    .for_approval_projection(task)
    .with_optional_task_files(Some(files.clone()))?
    .with_interaction_owner(
        &owner.verifier,
        &owner.schemas,
        &owner.scope,
        &owner.volume,
        None,
    )?;
    let id = crate::InteractionId::from_bytes([114; 16]);
    let interaction = Interaction::approval(
        "One authenticated binding".into(),
        OperationId::from_bytes(task.into_bytes()),
        [5; 32],
        ApprovalSubject::Opaque {},
    )?;
    journal.open_interaction(id, &interaction, None)?.await?;
    let request = files
        .snapshot(
            &Query::File(FileKey::Task {
                task: Component::new(task.to_string())?,
                file: TaskFile::ApprovalRequest(Component::new(id.to_string())?),
            }),
            1,
        )
        .await?
        .files
        .remove(0);
    let digest = FileDescriptor::from_bytes(&request.bytes, "application/json", &mut None)?;
    let review = ApprovalReviewFile {
        principal: "human".into(),
        request_sha256: ApprovalDecisionFile::new("human", digest.sha256(), 1, true, None)?
            .request_sha256,
        policy_digest: runtime
            .approval_policy_identity()
            .ok_or_else(|| Error::Invalid("missing fixture policy".into()))?
            .digest,
        stage: 0,
        verdict: RuleVerdict::Ask,
        reason: None,
    };
    let file = FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string())?,
            file: TaskFile::ApprovalReview {
                request: Component::new(id.to_string())?,
                principal: Component::new(ApprovalDecisionFile::principal_file("human"))?,
            },
        },
        crate::contract::canonical_json_bytes(&review)?,
    );
    files.inner.append(files.inner.head().await?, &file).await?;
    Ok(FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string())?,
            file: TaskFile::ApprovalDecision {
                request: Component::new(id.to_string())?,
                principal: Component::new(ApprovalDecisionFile::principal_file("human"))?,
            },
        },
        crate::contract::canonical_json_bytes(&ApprovalDecisionFile::new(
            "human",
            digest.sha256(),
            1,
            true,
            None,
        )?)?,
    ))
}

#[tokio::test]
async fn approval_submission_consumes_one_authenticated_binding_for_both_entry_points() -> Result<()>
{
    use super::super::interactions::APPROVAL_BINDINGS;
    for reviewed in [false, true] {
        let initialized = setup().await?;
        let runtime = &initialized.runtime;
        let task = initialized.task;
        let files = &initialized.files;
        let responder = &initialized.responder;
        let decision = request(&initialized.fixture, runtime, task, files).await?;
        APPROVAL_BINDINGS.with(|count| count.set(0));
        if reviewed {
            runtime.submit_reviewed_approval(task, &decision).await?;
        } else {
            runtime
                .write_approval_decision(task, &decision, responder)
                .await?;
        }
        APPROVAL_BINDINGS.with(|count| assert_eq!(count.get(), 1));
        assert!(
            matches!(files.snapshot(&Query::File(decision.key.clone()), 1).await?.files.as_slice(), [file] if file.bytes == decision.bytes)
        );
        // Old shape: selecting the review opens a view, then re-entering the
        // public writer opens another. The same one-binding oracle detects it.
        APPROVAL_BINDINGS.with(|count| count.set(0));
        drop(runtime.approval_files(task).await?);
        runtime
            .write_approval_decision(task, &decision, responder)
            .await?;
        APPROVAL_BINDINGS.with(|count| assert_eq!(count.get(), 2));
        let mut forged = decision.clone();
        let mut body: ApprovalDecisionFile = crate::contract::json_from_slice(&forged.bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        body.request_sha256 = "11".repeat(32);
        forged.bytes = crate::contract::canonical_json_bytes(&body)?.into();
        assert!(
            runtime
                .submit_reviewed_approval(task, &forged)
                .await
                .is_err()
        );
        assert!(
            runtime
                .write_approval_decision(task, &forged, responder)
                .await
                .is_err()
        );
        assert!(
            matches!(files.snapshot(&Query::File(decision.key.clone()), 1).await?.files.as_slice(), [file] if file.bytes == decision.bytes)
        );
    }
    Ok(())
}
