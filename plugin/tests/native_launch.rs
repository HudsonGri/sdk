//! Native children run under the existing owner while their hook slot stays free.
#[path = "support/native_hooks.rs"]
mod native_hooks;
#[path = "support/native_actions.rs"]
mod native_actions;
use native_actions::Tools;
use futures::FutureExt as _;
use native_hooks::*;
use serde_json::{Value, json};
use std::{path::Path, process::Stdio};

fn native_spawn(id: &str, helper: &str) -> Value {
    let session = uuid::Uuid::new_v4().to_string();
    json!({"id":id,"prompt":helper,"executor":{
        "session":{"host":"pi","session_id":session,"caller":null},
        "executable":std::env::current_exe().unwrap(),
        "arguments":[{"kind":"text","value":"--ignored"},{"kind":"text","value":"--exact"},
            {"kind":"prompt"},{"kind":"text","value":"--nocapture"}],
        "session_bytes":json!({"type":"session","version":3,"id":session,
            "timestamp":"2026-10-17T00:00:00Z","cwd":"/project"}).to_string(),
        "timeout_ms":8000
    }})
}

fn tool_value(answer: Value) -> Result<Value> {
    assert_eq!(answer["isError"], false, "{answer}");
    let text = answer["content"]
        .as_array()
        .ok_or("missing tool content")?
        .iter()
        .filter_map(|part| part["text"].as_str())
        .collect::<String>();
    Ok(serde_json::from_str(&text)?)
}

#[tokio::test]
async fn native_process_runs_nested_hooks_in_isolated_children_and_replays_without_launching()
-> Result {
    let fixture = Fixture::new()?;
    std::fs::write(fixture.path().join("preserved.txt"), "parent contents")?;
    let mut owner = Some(fixture.start_owner().await?);
    let result = std::panic::AssertUnwindSafe(async {
        let cwd = fixture.path();
        let mut parent = Tools::open(cwd, acyclic_harness::external_executor::ExternalHost::Pi, None).await?;
        tokio::time::timeout(std::time::Duration::from_secs(8), async {
            let args = native_spawn("child", "native_child_process");
            let child = parent.call("spawn", "acyclic_spawn", args.clone()).await?;
            let child = child.as_str().ok_or("missing child task")?;
            let outcome =
                parent.call("wait", "acyclic_wait", json!({"child":child})).await?;
            assert_eq!(outcome["success"], true, "{outcome}");
            assert!(!cwd.join("child.txt").exists());
            assert!(!cwd.join("leaf.txt").exists());
            let inspected = parent.call(
                "inspect",
                "acyclic_inspect",
                json!({"task":child}),
            )
            .await?;
            assert_eq!(inspected["admission"]["parent"], json!(parent.task));
            let diff = parent.call(
                "diff",
                "fs",
                json!({"command":format!("diff agents/{child}")}),
            )
            .await?;
            assert_eq!(
                diff["result"]["Filesystem"]["Data"]["kind"], "diff",
                "{diff}"
            );
            assert_eq!(
                diff["result"]["Filesystem"]["Data"]["value"]["fileChanges"], 2,
                "{diff}"
            );
            parent.call(
                "selected-import",
                "fs",
                json!({"command":format!("restore --source agents/{child} -- child.txt")}),
            )
            .await?;
            assert_eq!(
                std::fs::read_to_string(cwd.join("child.txt"))?,
                "isolated native effect"
            );
            assert!(!cwd.join("leaf.txt").exists());
            assert_eq!(
                std::fs::read_to_string(cwd.join("preserved.txt"))?,
                "parent contents"
            );
            fixture.close_owner(owner.take().unwrap()).await?;
            owner = Some(fixture.start_owner().await?);
            parent.reconnect().await?;
            let replay = parent.call("spawn", "acyclic_spawn", args).await?;
            assert_eq!(replay, child);
            assert_eq!(
                parent.call("wait", "acyclic_wait", json!({"child":child})).await?,
                outcome
            );
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await?
    })
    .catch_unwind()
    .await;
    if matches!(&result, Ok(Err(_))) {
        let _ = retained_diagnostics(fixture.path()).await;
    }
    let stopped = if let Some(owner) = owner {
        fixture.close_owner(owner).await
    } else {
        Ok(())
    };
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    stopped
}

// These are the actual native executables launched by the production custody
// task. They use the installed CLI and inherited authenticated client binding.
async fn owned_hook(event: &str, input: Value) -> Result<Value> {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_acyclic"));
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    hook_using(command, "pi", event, input).await
}

async fn owned_call(session: &str, id: &str, name: &str, input: Value) -> Result<Value> {
    tool_value(
        owned_hook(
            "tool_call",
            json!({"sessionId":session,"toolCallId":id,"toolName":name,"input":input}),
        )
        .await?,
    )
}

async fn native_child(nested: bool) -> Result {
    let cwd = std::env::current_dir()?;
    // create_new proves the custody command did not execute twice after reopen.
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(cwd.join("executed"))?;
    let header: Value = serde_json::from_slice(&std::fs::read(cwd.join("session.jsonl"))?)?;
    let session = header["id"]
        .as_str()
        .ok_or("missing native session identity")?;
    owned_hook("session_start", json!({"sessionId":session,"cwd":cwd})).await?;
    owned_call(session,"write","write",json!({"path":if nested {"child.txt"} else {"leaf.txt"},"content":"isolated native effect"})).await?;
    if nested {
        let leaf = owned_call(
            session,
            "spawn-leaf",
            "acyclic_spawn",
            native_spawn("leaf", "native_leaf_process"),
        )
        .await?;
        let outcome =
            owned_call(session, "wait-leaf", "acyclic_wait", json!({"child":leaf})).await?;
        assert_eq!(outcome["success"], true, "{outcome}");
        owned_call(session,"import-leaf","fs",json!({"command":format!("restore --source agents/{} -- leaf.txt",leaf.as_str().ok_or("missing leaf task")?)})).await?;
    }
    owned_hook("session_shutdown", json!({"sessionId":session,"cwd":cwd})).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "launched as the native child by the custody regression"]
async fn native_child_process() -> Result {
    native_child(true).await
}

#[tokio::test]
#[ignore = "launched as the native descendant by the custody regression"]
async fn native_leaf_process() -> Result {
    native_child(false).await
}

// Read the accepted task files on failure before closing their provider. This
// distinguishes an unlaunched child, a running effect and a stranded result.
async fn retained_diagnostics(cwd: &Path) -> Result {
    use acyclic_harness::task_volume::{Query, TaskVolume};
    use acyclic_stream::{ChildrenPageRequest, StreamPath, StreamProvider as _};
    let client = std::sync::Arc::new(client(cwd).await?);
    for parent in ["external/requests", "external/results"] {
        let page = client
            .children_page(ChildrenPageRequest {
                parent: Some(StreamPath::new(parent)?),
                after: None,
                hierarchy_version: None,
                limit: 64,
            })
            .await?;
        assert!(
            page.next_after.is_none(),
            "native regression exceeded its task bound"
        );
        for child in page.children {
            let files = TaskVolume::open_existing(client.clone(), child.path.clone()).await?;
            for file in files.snapshot(&Query::Root, 128).await?.files {
                let preview = &file.bytes[..file.bytes.len().min(2048)];
                eprintln!(
                    "{} {}: {}",
                    child.path.as_str(),
                    file.key.path(),
                    String::from_utf8_lossy(preview)
                );
            }
        }
    }
    Ok(())
}
