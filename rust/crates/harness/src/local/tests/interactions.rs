use super::*;
use crate::{
    registry::ComponentIdentity,
    runtime::{RuntimeScope, ToolPolicy, ToolPolicyDecision},
    store::{HistoryContentFuture, history::HistoryIo},
    task_volume::{Component, Query, TaskFile},
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Count(Arc<AtomicUsize>);
static ECHO: Echo = Echo;

impl ToolExecutor for Count {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        ECHO.execute(invocation, context)
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        ECHO.reconcile(invocation, context)
    }
}

struct Manual;

impl ToolPolicy for Manual {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            name: "test.manual".into(),
            version: "1".into(),
            digest: [9; 32],
        }
    }

    fn evaluate<'a>(
        &'a self,
        _: &'a ToolInvocation,
        _: &'a RuntimeScope,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>> {
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            return crate::NativeFutureBundle::new(work, || async move {
                let mut io = crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                );
                let prompt = io.clone_text("Approve this exact call?")?;
                Ok((
                    ToolPolicyDecision::RequireApproval { prompt },
                    Some(io.into_custody()?),
                ))
            })
            .map(HistoryContentFuture::Native);
        }
        Ok(HistoryContentFuture::Ordinary {
            future: Box::pin(async {
                Ok((
                    ToolPolicyDecision::RequireApproval {
                        prompt: "Approve this exact call?".into(),
                    },
                    None,
                ))
            }),
            frame_bytes: 0,
        })
    }

    fn identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        io.retain_owned(std::mem::size_of::<ComponentIdentity>() as u64)?;
        let name = io.clone_text("test.manual")?;
        let version = io.clone_text("1")?;
        Ok((
            ComponentIdentity {
                name,
                version,
                digest: [9; 32],
            },
            io.into_custody()?,
        ))
    }
}

#[tokio::test]
async fn local_manual_approval_reopens_without_dispatching_the_tool() -> Result<()> {
    approval_case(true).await?;
    // Independent no-policy fixture proves the same invocation can execute.
    approval_case(false).await
}

#[tokio::test]
async fn interaction_owner_rejects_agent_rebinding_and_task_audience() -> Result<()> {
    use crate::core::{AggregateKind, Authority, AuthorityIssuer};

    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let volume = volume(directory.path()).await?;
    let stream = Arc::new(
        acyclic_stream::LocalStream::open(directory.path().join("stream"), Default::default())
            .await
            .map_err(super::super::storage)?,
    );
    let issuer = |kind| {
        AuthorityIssuer::new(
            "local-test",
            [7; 32],
            Authority {
                kind,
                id: "local-owner".into(),
            },
        )
    };
    let conversation = issuer(AggregateKind::Conversation);
    let agent = crate::AgentId::from_bytes([1; 16]);
    let (verifier, scope) =
        super::super::interactions::open(&stream, &conversation, agent, &volume).await?;
    verifier.verify(&scope, &mut None)?;
    assert!(
        scope
            .capabilities()
            .contains(crate::contract::capability::INTERACTION_OPEN)
    );
    assert!(
        !scope
            .capabilities()
            .contains(crate::contract::capability::INTERACTION_RESOLVE)
    );
    super::super::interactions::open(&stream, &conversation, agent, &volume).await?;
    assert!(matches!(
        super::super::interactions::open(
            &stream,
            &conversation,
            crate::AgentId::from_bytes([2; 16]),
            &volume,
        )
        .await,
        Err(Error::Conflict(_))
    ));
    assert!(
        issuer(AggregateKind::Task)
            .verifier(&mut None)?
            .verify(&scope, &mut None)
            .is_err()
    );
    Ok(())
}

async fn approval_case(gated: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "exact-call".into(),
                "test.custom".into(),
                json!({"private":"arguments"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("done".into()),
    ]);
    let calls = Arc::new(AtomicUsize::new(0));
    let config = || -> Result<LocalHarnessConfig> {
        let mut tool = tool("test.custom");
        tool.executor = Arc::new(Count(calls.clone()));
        let config = volume_config(directory.path(), project.clone(), provider.clone())?
            .without_default_tools()
            .tool(tool);
        Ok(if gated {
            config.tool_policy(Arc::new(Manual))
        } else {
            config
        })
    };
    let local = LocalHarness::open(config()?).await?;
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        local.submit(OperationId::new(), "call the tool").await?
    else {
        panic!("durable admission required");
    };
    local.advance(task_id, &local.commands()?).await?;
    assert_eq!(
        local.runtime.task_host().outcome(task_id).await?.is_none(),
        gated
    );
    let query =
        Query::Approvals(Component::new(task_id.to_string()).map_err(super::super::storage)?);
    let before = local
        .task_files
        .inner
        .snapshot(&query, 8)
        .await
        .map_err(super::super::storage)?;
    assert_eq!(before.files.len(), usize::from(gated));
    if gated {
        assert!(matches!(
            &before.files[0].key,
            crate::task_volume::FileKey::Task {
                file: TaskFile::ApprovalRequest(_),
                ..
            }
        ));
        let request: crate::interaction::Interaction =
            serde_json::from_slice(&before.files[0].bytes).map_err(super::super::storage)?;
        assert!(
            matches!(request, crate::interaction::Interaction::Approval {
        subject: crate::interaction::ApprovalSubject::Tool { ref call_id, ref arguments, .. }, ..
    } if call_id == "exact-call" && arguments == &json!({"private":"arguments"}))
        );
    }
    drop(local);
    let reopened = LocalHarness::open(config()?).await?;
    reopened.advance(task_id, &reopened.commands()?).await?;
    assert_eq!(
        reopened
            .runtime
            .task_host()
            .outcome(task_id)
            .await?
            .is_none(),
        gated
    );
    let after = reopened
        .task_files
        .inner
        .snapshot(&query, 8)
        .await
        .map_err(super::super::storage)?;
    assert_eq!(after.files, before.files);
    assert_eq!(provider.requests().len(), if gated { 1 } else { 2 });
    assert_eq!(calls.load(Ordering::SeqCst), usize::from(!gated));
    if gated {
        use crate::interaction::ApprovalDecisionFile;
        use crate::task_volume::{FileKey, FileValue};
        use sha2::Digest as _;

        let FileKey::Task {
            file: TaskFile::ApprovalRequest(request),
            ..
        } = &before.files[0].key
        else {
            panic!("approval request required");
        };
        let id = crate::InteractionId::from_bytes(
            request.as_str().parse::<uuid::Uuid>().unwrap().into_bytes(),
        );
        let digest = sha2::Sha256::digest(&before.files[0].bytes).into();
        let body = ApprovalDecisionFile::new(
            LocalHarness::APPROVAL_PRINCIPAL,
            &digest,
            1,
            true,
            Some("Reviewed the arguments".into()),
        )?;
        let file = FileValue::new(
            FileKey::Task {
                task: Component::new(task_id.to_string())?,
                file: TaskFile::ApprovalDecision {
                    request: request.clone(),
                    principal: Component::new(ApprovalDecisionFile::principal_file(
                        LocalHarness::APPROVAL_PRINCIPAL,
                    ))?,
                },
            },
            serde_json::to_vec(&body).map_err(super::super::storage)?,
        );
        let mut wrong_task = file.clone();
        let mut other_task = task_id.into_bytes();
        other_task[0] ^= 1;
        if let FileKey::Task { task, .. } = &mut wrong_task.key {
            *task = Component::new(crate::TaskId::from_bytes(other_task).to_string())?;
        }
        assert!(matches!(
            reopened
                .write_approval_decision(task_id, id, &wrong_task)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let fixture = config()?;
        let owner = super::super::owner::Owner::open(
            fixture
                .state_directory
                .as_deref()
                .expect("explicit test state directory"),
            &fixture.project,
        )?;
        let mut foreign_key = owner.key;
        foreign_key[0] ^= 1;
        let forged = crate::core::AuthorityIssuer::new(
            "acyclic.local",
            foreign_key,
            reopened
                .interaction_issuer
                .as_ref()
                .unwrap()
                .verifier(&mut None)?
                .audience()
                .clone(),
        )
        .issue(
            LocalHarness::APPROVAL_PRINCIPAL,
            &crate::Capabilities::new([
                crate::contract::capability::INTERACTION_RESOLVE.to_owned(),
                crate::contract::capability::interaction_respond(id),
            ]),
            reopened.signed.agent(),
            None,
            &mut None,
        )?;
        assert!(matches!(
            reopened
                .runtime
                .write_approval_decision(task_id, &file, &forged)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut other_id = id.into_bytes();
        other_id[0] ^= 1;
        assert!(matches!(
            reopened
                .write_approval_decision(task_id, crate::InteractionId::from_bytes(other_id), &file)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut wrong = body.clone();
        let different = if wrong.request_sha256.starts_with('0') {
            "1"
        } else {
            "0"
        };
        wrong.request_sha256.replace_range(..1, different);
        let mut wrong_file = file.clone();
        wrong_file.bytes = serde_json::to_vec(&wrong)
            .map_err(super::super::storage)?
            .into();
        assert!(matches!(
            reopened
                .write_approval_decision(task_id, id, &wrong_file)
                .await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let unchanged = reopened.task_files.inner.snapshot(&query, 8).await?;
        assert_eq!(unchanged.files, before.files);
        reopened.write_approval_decision(task_id, id, &file).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let decided = reopened.task_files.inner.snapshot(&query, 8).await?;
        assert_eq!(decided.files.len(), 2);
        assert!(
            decided
                .files
                .iter()
                .any(|value| value.key == file.key && value.bytes == file.bytes)
        );
        assert_eq!(reopened.resume(task_id).await?.text, "done");
        reopened.write_approval_decision(task_id, id, &file).await?;
        drop(reopened);
        let completed = LocalHarness::open(config()?).await?;
        assert_eq!(completed.resume(task_id).await?.text, "done");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(provider.requests().len(), 2);
    }
    Ok(())
}
