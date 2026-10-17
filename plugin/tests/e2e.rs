//! Native hook processes are clients, and cannot create a second task owner.
use acyclic_harness::external_executor::{ExternalHost, hooks};
use serde::Deserialize;
use serde_json::Value;
use std::{
    io::Write as _,
    process::{Command, Stdio},
};

#[path = "activation/mod.rs"]
mod activation;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Deserialize)]
struct Scenario {
    host: ExternalHost,
    event: String,
    input: Value,
}

#[cfg(unix)]
#[test]
fn claude_command_hooks_deliver_arguments_through_the_real_shell() -> Result {
    let directory = tempfile::tempdir()?;
    let quoted = directory
        .path()
        .join("owned ' $(never-run) `native` directory");
    std::fs::create_dir(&quoted)?;
    let executable = quoted.join("acyclic");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_acyclic"), &executable)?;
    let missing = directory.path().join("missing-binding.json");
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!(
        "../../rust/crates/harness/tests/external_hooks.json"
    ))?;
    let groups = acyclic_harness::external_executor::claude_hooks(executable.to_str().unwrap());
    for scenario in scenarios
        .into_iter()
        .filter(|scenario| scenario.host == ExternalHost::ClaudeCode)
    {
        let command = groups[scenario.event.as_str()][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        let mut child = Command::new("sh")
            .args(["-c", command])
            .current_dir(directory.path())
            .env("ACYCLIC_HARNESS_BINDING", &missing)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or("hook stdin")?
            .write_all(&serde_json::to_vec(&scenario.input)?)?;
        let output = child.wait_with_output()?;
        assert_eq!(output.status.code(), Some(2));
        let answer: Value = serde_json::from_slice(&output.stdout)?;
        if scenario.event == "PreToolUse" {
            assert_eq!(answer["hookSpecificOutput"]["permissionDecision"], "deny");
        } else {
            assert!(
                answer["systemMessage"].is_string(),
                "hook did not receive its event arguments: {answer}"
            );
        }
    }
    assert!(!directory.path().join("never-run").exists());
    Ok(())
}

#[test]
fn invalid_platform_binding_fails_closed_without_starting_a_plugin_host() -> Result {
    let directory = tempfile::tempdir()?;
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!(
        "../../rust/crates/harness/tests/external_hooks.json"
    ))?;
    for scenario in scenarios {
        let mut child = Command::new(env!("CARGO_BIN_EXE_acyclic"))
            .args(["__hook", scenario.host.name(), &scenario.event])
            .current_dir(directory.path())
            .env(
                "ACYCLIC_HARNESS_BINDING",
                directory.path().join("missing-binding.json"),
            )
            .env("HOME", directory.path())
            .env("USERPROFILE", directory.path())
            .env("XDG_STATE_HOME", directory.path())
            .env("LOCALAPPDATA", directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or("missing hook stdin")?
            .write_all(&serde_json::to_vec(&scenario.input)?)?;
        let output = child.wait_with_output()?;
        assert_eq!(output.status.code(), Some(2));
        let answer: Value = serde_json::from_slice(&output.stdout)?;
        if scenario.event == "PreToolUse" {
            assert_eq!(
                answer.pointer("/hookSpecificOutput/permissionDecision"),
                Some(&Value::String("deny".into()))
            );
        } else if scenario.event == "tool_call" {
            assert_eq!(answer.get("block"), Some(&Value::Bool(true)));
        } else {
            assert!(
                answer
                    .get("systemMessage")
                    .and_then(Value::as_str)
                    .is_some_and(|message| !message.is_empty())
            );
        }
        assert_eq!(
            std::fs::read_dir(directory.path())?.count(),
            0,
            "a hook may not initialize an independent host"
        );
    }
    Ok(())
}

#[test]
fn host_manifests_route_every_event_to_the_common_client() -> Result {
    let codex: Value = serde_json::from_str(include_str!("../hooks/hooks.json"))?;
    let pi: Value = serde_json::from_str(include_str!("../pi/package.json"))?;
    let claude: Value = serde_json::from_str(include_str!("../claude/.claude-plugin/plugin.json"))?;
    assert_eq!(claude, acyclic_harness::external_executor::claude_manifest(env!("CARGO_PKG_VERSION")));
    for spec in hooks(ExternalHost::Codex) {
        let hook = &codex["hooks"][spec.event][0]["hooks"][0];
        assert_eq!(hook["type"], "command");
        assert!(
            hook["command"]
                .as_str()
                .is_some_and(|command| command.ends_with(&format!("__hook codex {}", spec.event)))
        );
        assert_eq!(hook["timeout"].as_u64(), Some(spec.timeout_seconds()));
    }
    for spec in hooks(ExternalHost::Pi) {
        assert_eq!(
            pi["acyclic"]["hooks"][spec.event]["args"],
            serde_json::json!(["__hook", "pi", spec.event])
        );
    }
    let mcp: Value = serde_json::from_str(include_str!("../.mcp.json"))?;
    assert_eq!(
        mcp["mcpServers"]["acyclic"]["args"],
        serde_json::json!(["__tools"])
    );
    for file in ["../src/main.rs", "../pi/index.ts"] {
        let source = std::fs::read_to_string(
            std::path::Path::new(file)
                .strip_prefix("../")
                .map(|path| std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))?,
        )?;
        assert!(source.lines().count() <= 300);
        for old in [
            "ConcurrentServiceControl",
            "ControlEndpoint",
            "thread_stack_size",
            "setTimeout",
            "while (true)",
        ] {
            assert!(!source.contains(old), "retired owner in {file}: {old}");
        }
    }
    Ok(())
}
