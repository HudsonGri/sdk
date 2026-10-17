//! Native hook/MCP processes share LocalHarness request and result files.
use serde_json::{Value, json};
use std::path::Path;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout},
};

#[path = "support/native_hooks.rs"]
mod native_hooks;
use native_hooks::*;

#[path = "native_tools/commands.rs"]
mod commands;

#[path = "native_tools/parallel.rs"]
mod parallel;

struct ResultClient {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    next_id: u64,
}

impl ResultClient {
    async fn open(directory: &Path, host: &str, binding: Option<&Path>) -> Result<Self> {
        let mut launch = command(directory);
        if let Some(binding) = binding {
            launch.env("ACYCLIC_HARNESS_BINDING", binding);
        }
        if host == "codex" {
            // Codex gives MCP a small standard environment plus the manifest's
            // explicit env_vars. Hooks inherit the calling host's environment.
            launch.env("HOME", directory.parent().ok_or("MCP home missing")?);
            let overrides: Vec<_> = launch
                .as_std()
                .get_envs()
                .map(|(key, value)| (key.to_owned(), value.map(std::ffi::OsStr::to_owned)))
                .collect();
            launch.env_clear();
            let manifest: Value = serde_json::from_str(include_str!("../mcp.json"))?;
            let declared = manifest["mcpServers"]["acyclic"]["env_vars"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str);
            for name in [
                "HOME",
                "PATH",
                "USERPROFILE",
                "SYSTEMROOT",
                "COMSPEC",
                "PATHEXT",
                "TEMP",
                "TMP",
            ]
            .into_iter()
            .chain(declared)
            {
                let value = match overrides.iter().find(|(key, _)| key == name) {
                    Some((_, value)) => value.clone(),
                    None => std::env::var_os(name),
                };
                if let Some(value) = value {
                    launch.env(name, value);
                }
            }
        }
        let mut child: Child = launch.arg("__tools").spawn()?;
        let mut input = child.stdin.take().ok_or("MCP stdin missing")?;
        let mut output = BufReader::new(child.stdout.take().ok_or("MCP stdout missing")?).lines();
        let write = |value: Value| format!("{value}\n").into_bytes();
        input.write_all(&write(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"native-probe","version":"1"}}}))).await?;
        let initialized: Value = serde_json::from_str(
            &output
                .next_line()
                .await?
                .ok_or("MCP initialization ended")?,
        )?;
        assert_eq!(initialized["id"], 1);
        assert!(initialized.get("error").is_none(), "{initialized}");
        input
            .write_all(&write(
                json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            ))
            .await?;
        Ok(Self {
            child,
            input,
            output,
            next_id: 2,
        })
    }

    async fn result(&mut self, arguments: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.input.write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"write","arguments":arguments}})).as_bytes()).await?;
        let response: Value =
            serde_json::from_str(&self.output.next_line().await?.ok_or("MCP result ended")?)?;
        assert_eq!(response["id"], id);
        assert!(response.get("error").is_none(), "{response}");
        Ok(response["result"].clone())
    }

    async fn close(mut self) -> Result {
        drop(self.input);
        assert!(self.child.wait().await?.success());
        Ok(())
    }
}

#[tokio::test]
async fn native_hooks_publish_once_and_mcp_only_returns_the_stored_result() -> Result {
    tokio::time::timeout(std::time::Duration::from_secs(8),async {
        for host in ["codex","claude-code","pi"] {
            let directory = Fixture::new()?;
            let session = uuid::Uuid::new_v4().to_string();
            let cwd = directory.path().to_str().ok_or("non-UTF8 test directory")?;
            let pi = host == "pi";
            hook(directory.path(),host,if pi {"session_start"} else {"SessionStart"},json!({"session_id":session,"cwd":cwd})).await?;
            let arguments = json!({"path":"native.txt","text":"one harness write","media_type":"text/plain","display_name":"native.txt"});
            let request = json!({"session_id":session,"cwd":cwd,"tool_use_id":"call","tool_name":if pi {"write"} else {"mcp__acyclic__write"},"tool_input":arguments});
            let answer = hook(directory.path(),host,if pi {"tool_call"} else {"PreToolUse"},request.clone()).await?;
            assert_eq!(std::fs::read_to_string(directory.path().join("native.txt"))?,"one harness write");
            let published = std::fs::metadata(directory.path().join("native.txt"))?.modified()?;
            if pi {
                assert_eq!(answer["isError"],false);
                assert!(answer["details"]["file"].is_object());
            } else {
                assert_eq!(answer["hookSpecificOutput"]["permissionDecision"],"allow");
                let arguments = answer["hookSpecificOutput"]["updatedInput"].clone();
                assert!(arguments["_acyclic"].is_string());
                // Standard plugins start MCP at the package root, independent
                // of the user's selected checkout. The file names its binding.
                let mcp = directory.path().parent().unwrap().join("mcp");
                std::fs::create_dir(&mcp)?;
                let binding = directory.path().parent().unwrap().join("binding.json");
                std::fs::write(&binding,serde_json::to_vec(&json!({"provider":"local","workspace":directory.path()}))?)?;
                let mut original = None;
                for binding in [None, Some(binding.as_path())] {
                    let mut results = ResultClient::open(&mcp, host, binding).await?;
                    let first = results.result(arguments.clone()).await?;
                    assert!(first["structuredContent"]["file"].is_object(),"{first}");
                    if let Some(original) = &original { assert_eq!(&first,original); }
                    assert_eq!(results.result(arguments.clone()).await?,first);
                    let mut changed = arguments.clone();
                    changed["text"] = json!("forged");
                    assert_eq!(results.result(changed).await?["isError"],true);
                    if let Some(binding) = binding {
                        std::fs::write(binding,serde_json::to_vec(&json!({"provider":"local","workspace":mcp}))?)?;
                        assert_eq!(results.result(arguments.clone()).await?["isError"],true);
                    }
                    results.close().await?;
                    original = Some(first);
                }
                assert_eq!(std::fs::read_dir(directory.path().parent().unwrap().join("owner/workspaces"))?.count(),1);
            }
            assert_eq!(hook(directory.path(),host,if pi {"tool_call"} else {"PreToolUse"},request).await?,answer);
            assert_eq!(std::fs::metadata(directory.path().join("native.txt"))?.modified()?,published);
            hook(directory.path(),host,if pi {"session_shutdown"} else {"SessionEnd"},json!({"session_id":session,"cwd":cwd})).await?;
            directory.close().await?;
        }
        Ok::<_,Box<dyn std::error::Error>>(())
    }).await??;
    Ok(())
}
