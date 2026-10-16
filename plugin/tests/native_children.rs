//! Separate native hook processes progress while their parent awaits a child.
#[path = "support/native_hooks.rs"]
mod native_hooks;
use acyclic_harness::{
    external_executor::{ExternalHost, ExternalRequest, request},
    task_volume::{Component, FileKey, Query, TaskFile, TaskVolume},
};
use acyclic_stream::grpc::Client;
use futures::StreamExt as _;
use native_hooks::*;
use serde_json::json;

async fn started(files: &TaskVolume<Client>, call: &ExternalRequest) -> Result {
    let key = FileKey::Task {
        task: Component::new(call.task_id().to_string())?,
        file: TaskFile::RequestStarted(Component::new(call.operation_id.to_string())?),
    };
    let mut observed = files.observe(Query::File(key.clone()), None, 1).await?;
    if observed
        .snapshot
        .is_some_and(|snapshot| snapshot.files.iter().any(|file| file.key == key))
    {
        return Ok(());
    }
    while let Some(change) = observed.changes.next().await {
        let change = change?;
        if change.file.as_ref().is_some_and(|file| file.key == key) {
            return Ok(());
        }
    }
    Err("command-start follow ended".into())
}

#[tokio::test]
async fn exposed_wait_uses_the_returned_child_handle_for_every_host() -> Result {
    let fixture = Fixture::new()?;
    let cwd = fixture.path();
    for host in [
        ExternalHost::Codex,
        ExternalHost::ClaudeCode,
        ExternalHost::Pi,
    ] {
        let pi = host == ExternalHost::Pi;
        let session = host.name();
        let attach = if pi { "session_start" } else { "SessionStart" };
        hook(
            cwd,
            host.name(),
            attach,
            json!({"session_id":session,"cwd":cwd}),
        )
        .await?;
        let spawned = hook(
            cwd,
            host.name(),
            if pi {
                "subagent_start"
            } else {
                "SubagentStart"
            },
            json!({"session_id":session,"agent_id":"child"}),
        )
        .await?;
        let child = spawned["Succeeded"]
            .as_str()
            .ok_or("spawn did not return a child handle")?;
        hook(
            cwd,
            host.name(),
            attach,
            json!({"session_id":session,"agent_id":"child","cwd":cwd}),
        )
        .await?;
        let event = if pi { "tool_call" } else { "PreToolUse" };
        let arguments = json!({"child":child});
        let input = json!({"session_id":session,"tool_use_id":"wait","tool_name":
            if pi {"acyclic_wait"} else {"mcp__acyclic__acyclic_wait"},"tool_input":arguments});
        let call = request(
            host,
            event,
            json!({"session_id":session,"tool_use_id":"wait",
            "tool_name":"acyclic_wait","tool_input":arguments}),
        )?;
        let results = files(cwd, &format!("external/results/{}", call.task_id())).await?;
        let mut waiting = Box::pin(hook(cwd, host.name(), event, input));
        tokio::select! {
            result = &mut waiting => panic!("wait returned before child completion: {result:?}"),
            result = started(&results,&call) => result?,
        }
        let (answer, ended) = tokio::join!(
            waiting,
            hook(
                cwd,
                host.name(),
                if pi { "session_shutdown" } else { "SessionEnd" },
                json!({"session_id":session,"agent_id":"child"})
            )
        );
        let answer = answer?;
        ended?;
        if pi {
            assert_eq!(answer["isError"], false);
            assert!(answer["details"].is_null());
        } else {
            assert_eq!(answer["hookSpecificOutput"]["permissionDecision"], "allow");
        }
        let settled = results
            .snapshot(
                &Query::File(call.result_key(Component::new(call.task_id().to_string())?)?),
                1,
            )
            .await?;
        assert_eq!(settled.files.len(), 1);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&settled.files[0].bytes)?,
            json!({"Succeeded":null})
        );
    }
    fixture.close().await
}

#[tokio::test]
async fn native_subagent_stop_completes_the_child_before_parent_continuation() -> Result {
    let fixture = Fixture::new()?;
    let cwd = fixture.path();
    for host in [
        ExternalHost::Codex,
        ExternalHost::ClaudeCode,
        ExternalHost::Pi,
    ] {
        let pi = host == ExternalHost::Pi;
        let session = host.name();
        let attach = if pi { "session_start" } else { "SessionStart" };
        hook(
            cwd,
            host.name(),
            attach,
            json!({"sessionId":session,"cwd":cwd}),
        )
        .await?;
        hook(
            cwd,
            host.name(),
            if pi {
                "subagent_start"
            } else {
                "SubagentStart"
            },
            json!({"sessionId":session,"agentId":"child","agentType":"worker"}),
        )
        .await?;
        hook(
            cwd,
            host.name(),
            attach,
            json!({"sessionId":session,"agentId":"child","cwd":cwd}),
        )
        .await?;
        let event = if pi { "subagent_stop" } else { "SubagentStop" };
        let input = json!({"sessionId":session,"agentId":"child"});
        let join = request(host, event, input.clone())?;
        // The native host sends no child SessionEnd before continuing its
        // parent. Stop itself must settle the child, and retries must agree.
        assert_eq!(
            hook(cwd, host.name(), event, input.clone()).await?,
            json!({"Succeeded":null})
        );
        assert_eq!(
            hook(cwd, host.name(), event, input).await?,
            json!({"Succeeded":null})
        );
        let continued = hook(
            cwd,
            host.name(),
            if pi { "tool_call" } else { "PreToolUse" },
            json!({"session_id":session,"tool_use_id":"continued",
                "tool_name":if pi {"write"} else {"mcp__acyclic__write"},
                "tool_input":{"path":format!("{}.txt",host.name()),"text":"parent continued",
                    "media_type":"text/plain","display_name":"continued.txt"}}),
        )
        .await?;
        if pi {
            assert_eq!(continued["isError"], false, "{continued}");
        } else {
            assert_eq!(
                continued["hookSpecificOutput"]["permissionDecision"], "allow",
                "{continued}"
            );
        }
        assert_eq!(
            std::fs::read_to_string(cwd.join(format!("{}.txt", host.name())))?,
            "parent continued"
        );
        let results = files(cwd, &format!("external/results/{}", join.task_id())).await?;
        assert_eq!(
            results
                .snapshot(
                    &Query::File(join.result_key(Component::new(join.task_id().to_string())?)?),
                    1,
                )
                .await?
                .files
                .len(),
            1
        );
    }
    fixture.close().await
}

#[tokio::test]
async fn cancelling_a_passive_native_tool_settles_it_and_keeps_the_session_usable() -> Result {
    let fixture = Fixture::new()?;
    let cwd = fixture.path();
    hook(
        cwd,
        "pi",
        "session_start",
        json!({"sessionId":"cancel-wait","cwd":cwd}),
    )
    .await?;
    let input = json!({"sessionId":"cancel-wait","toolCallId":"receive",
        "toolName":"acyclic_receive","input":{"after":0}});
    let call = request(ExternalHost::Pi, "tool_call", input.clone())?;
    let results = files(cwd, &format!("external/results/{}", call.task_id())).await?;
    let waiting = start_hook(cwd, "pi", "tool_call", input).await?;
    started(&results, &call).await?;
    assert!(
        results
            .snapshot(
                &Query::File(call.result_key(Component::new(call.task_id().to_string())?)?),
                1
            )
            .await?
            .files
            .is_empty()
    );
    // A cancellation is published independently of the occupied command slot.
    let original = start_hook(
        cwd,
        "pi",
        "tool_cancel",
        json!({"sessionId":"cancel-wait","toolCallId":"receive"}),
    )
    .await?;
    // Pi's raw tool-call process reports a failed tool as a nonzero exit. Use
    // the request result file to assert settlement without treating it as success.
    let settled = async {
        let key = call.result_key(Component::new(call.task_id().to_string())?)?;
        let mut observed = results.observe(Query::File(key), None, 1).await?;
        if let Some(snapshot) = observed.snapshot {
            if let Some(file) = snapshot.files.first() {
                return Ok::<_, Box<dyn std::error::Error>>(serde_json::from_slice::<
                    serde_json::Value,
                >(&file.bytes)?);
            }
        }
        while let Some(change) = observed.changes.next().await {
            if let Some(file) = change?.file {
                return Ok(serde_json::from_slice(&file.bytes)?);
            }
        }
        Err("result follow ended".into())
    };
    let answer = settled.await?;
    assert!(answer.get("Failed").is_some(), "{answer}");
    let cancelled = original.wait_with_output().await?;
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    assert!(!waiting.wait_with_output().await?.status.success());
    hook(
        cwd,
        "pi",
        "tool_call",
        json!({"sessionId":"cancel-wait","toolCallId":"next",
        "toolName":"write","input":{"path":"after-cancel.txt","content":"usable"}}),
    )
    .await?;
    assert_eq!(
        std::fs::read_to_string(cwd.join("after-cancel.txt"))?,
        "usable"
    );
    fixture.close().await
}
