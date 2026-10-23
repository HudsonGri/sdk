use super::*;

#[test]
fn malformed_and_oversized_inputs_fail_before_any_workspace_request() {
    assert!(read_json(&b"not-json"[..]).is_err());
    assert!(read_json(vec![b' '; 65_537].as_slice()).is_err());
    assert_eq!(
        read_json(&b"{\"cwd\":\"fixture\"}"[..])
            .unwrap()
            .get("cwd")
            .unwrap()
            .clone(),
        "fixture"
    );
}

#[test]
fn failed_coordination_denies_execution_without_granting_native_permissions() {
    let denied = hook_failure("PreToolUse", "owner unavailable");
    assert_eq!(
        denied
            .pointer("/hookSpecificOutput/permissionDecision")
            .unwrap()
            .clone(),
        "deny"
    );
    assert_eq!(
        hook_failure("SubagentStart", "missing fork")
            .get("continue")
            .unwrap()
            .clone(),
        false
    );
    assert_eq!(
        hook("codex", "PreToolUse", json!({}))
            .pointer("/hookSpecificOutput/permissionDecision")
            .unwrap()
            .clone(),
        "deny"
    );
}

#[test]
fn claude_installation_preserves_foreign_hooks_permissions_and_unrelated_settings() -> Result<()> {
    let foreign = json!({"hooks":[{"type":"command","command":"foreign guard"}],"matcher":".*"});
    let original = json!({"permissions":{"deny":["Read(secret)"]},"theme":"dark","hooks":{"PreToolUse":[foreign]}});
    let mut document = original.clone();
    let executable = std::env::temp_dir().join("a path").join(if cfg!(windows) {
        "acyclic.exe"
    } else {
        "acyclic"
    });
    for _ in 0..2 {
        install::update_claude(&mut document, &executable.to_string_lossy(), true)?;
    }
    assert_eq!(
        document
            .pointer("/hooks/PreToolUse")
            .and_then(Value::as_array)
            .ok_or("fixture hook missing")?
            .len(),
        2
    );
    assert_eq!(document.get("permissions"), original.get("permissions"));
    assert_eq!(document.get("theme"), Some(&json!("dark")));
    install::update_claude(&mut document, &executable.to_string_lossy(), false)?;
    assert_eq!(document, original);
    let mut invalid = json!({"hooks":{"PreToolUse":"foreign"}});
    assert!(install::update_claude(&mut invalid, &executable.to_string_lossy(), true).is_err());
    Ok(())
}

#[test]
fn old_codex_releases_and_ambiguous_version_output_are_rejected() {
    for old in [
        "codex-cli 0.146.0",
        "codex-cli 0.163.9",
        "unknown",
        "0.164.0",
    ] {
        assert!(acyclic_harness::host_hooks::check_codex_version(old).is_err());
    }
    assert!(acyclic_harness::host_hooks::check_codex_version("codex-cli 0.164.0\n").is_ok());
}
