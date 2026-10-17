use super::*;
use crate::{
    ComponentIdentity,
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
    Ok(())
}
