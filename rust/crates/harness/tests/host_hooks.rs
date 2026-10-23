//! Native manifest and installer contracts, without a provider or agent runtime.
use acyclic_harness::host_hooks::{
    CLAUDE_EVENTS, claude_hook_executable, claude_hooks, claude_manifest,
};
use proptest::prelude::*;
use serde_json::json;

#[test]
fn manifest_contains_only_native_lifecycle_commands() {
    let manifest = claude_manifest("fixture");
    assert!(manifest.get("mcpServers").is_none());
    assert_eq!(manifest["version"], "fixture");
    let events = manifest["hooks"].as_object().unwrap();
    assert_eq!(events.len(), CLAUDE_EVENTS.len());
    for event in CLAUDE_EVENTS {
        let hook = &events[event][0]["hooks"][0];
        assert!(hook.get("args").is_none());
        assert_eq!(
            hook["command"],
            format!("\"${{CLAUDE_PLUGIN_ROOT}}/../bin/acyclic\" __hook claude-code {event}")
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn installed_commands_preserve_one_executable_argument(directory in r"[^\x00]{0,80}") {
        let executable = format!("{directory}/acyclic");
        #[cfg(windows)]
        let expected = executable.replace('\\', "/");
        #[cfg(not(windows))]
        let expected = executable;
        for (event, groups) in claude_hooks(&expected) {
            let group = &groups[0];
            let hook = &group["hooks"][0];
            prop_assert!(hook.get("args").is_none());
            prop_assert_eq!(acyclic_harness::tools::parse_command(hook["command"].as_str().unwrap())?,
                vec![expected.clone(), "__hook".into(), "claude-code".into(), event.into()]);
            prop_assert_eq!(claude_hook_executable(event, group), Some(expected.clone()));
            let mut foreign = group.clone();
            foreign["hooks"][0]["permissionDecision"] = json!("allow");
            prop_assert!(claude_hook_executable(event, &foreign).is_none());
        }
    }
}

#[test]
fn ownership_rejects_split_arguments_foreign_hooks_and_changed_lifecycle() {
    for (event, groups) in claude_hooks("/owned/acyclic") {
        let mut split = groups[0].clone();
        split["hooks"][0]["command"] = json!("/owned/acyclic");
        split["hooks"][0]["args"] = json!(["__hook", "claude-code", event]);
        assert!(claude_hook_executable(event, &split).is_none());
        let mut foreign = groups[0].clone();
        foreign["hooks"][0]["command"] = json!(format!("'foreign' __hook claude-code {event}"));
        assert!(claude_hook_executable(event, &foreign).is_none());
        let mut changed = groups[0].clone();
        changed["hooks"][0]["command"] = json!("'/owned/acyclic' __hook claude-code AnotherEvent");
        assert!(claude_hook_executable(event, &changed).is_none());
        assert!(claude_hook_executable("AnotherEvent", &groups[0]).is_none());
    }
}

#[cfg(unix)]
#[test]
fn native_shell_receives_arguments_from_both_manifest_and_installer() {
    use std::os::unix::fs::PermissionsExt as _;
    let temporary = tempfile::tempdir().unwrap();
    let package = temporary.path().join("package with spaces and 'quotes'");
    let bin = package.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::create_dir_all(package.join("claude")).unwrap();
    let executable = bin.join("acyclic");
    std::fs::write(&executable, "#!/bin/sh\nprintf '%s\\n' \"$@\"\n").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    let manifest = claude_manifest("fixture");
    let installed = claude_hooks(executable.to_str().unwrap());
    for event in CLAUDE_EVENTS {
        for hook in [
            &manifest["hooks"][event][0]["hooks"][0],
            &installed[event][0]["hooks"][0],
        ] {
            let output = std::process::Command::new("sh")
                .arg("-c")
                .arg(hook["command"].as_str().unwrap())
                .env("CLAUDE_PLUGIN_ROOT", package.join("claude"))
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!("__hook\nclaude-code\n{event}\n")
            );
        }
    }
}
