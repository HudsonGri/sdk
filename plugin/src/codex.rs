//! Native launch arguments. No provider, native tool replacement, or agent loop.
use super::*;
use std::process::{Command, Stdio};

pub(super) fn validate(host: &str, args: &[String]) -> Result<()> {
    let (sandbox, approval, allowed): (&str, &str, &[&str]) = match host {
        "codex" => (
            "--sandbox",
            "--ask-for-approval",
            &[
                "--sandbox",
                "--ask-for-approval",
                "--config",
                "--model",
                "--profile",
            ],
        ),
        "claude-code" => (
            "--settings",
            "--permission-mode",
            &[
                "--settings",
                "--permission-mode",
                "--model",
                "--allowedTools",
                "--disallowedTools",
                "--tools",
                "--setting-sources",
                "--plugin-dir",
            ],
        ),
        "pi" => (
            "",
            "",
            &[
                "--model",
                "--provider",
                "--thinking",
                "--tools",
                "--extension",
                "--session-dir",
            ],
        ),
        _ => return Err("host launcher supports codex, claude-code or pi".into()),
    };
    if args
        .chunks(2)
        .any(|pair| !matches!(pair, [name, _] if allowed.contains(&name.as_str())))
    {
        return Err("provide explicit native policy arguments as --option VALUE pairs; prompts belong in the native host".into());
    }
    for name in [sandbox, approval] {
        if name.is_empty() {
            continue;
        }
        if args
            .chunks(2)
            .filter(|pair| matches!(*pair, [key, _] if key == name))
            .count()
            != 1
        {
            return Err(format!(
                "host requires exactly one {name} VALUE; children inherit it unchanged"
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn program(host: &str) -> Result<&'static str> {
    match host {
        "codex" => Ok("codex"),
        "claude-code" => Ok("claude"),
        "pi" => Ok("pi"),
        _ => Err("unsupported native host".into()),
    }
}

pub(super) fn child(
    host: &str,
    args: &[String],
    cwd: &Path,
    session: Option<&str>,
) -> Result<Command> {
    validate(host, args)?;
    let mut command = Command::new(program(host)?);
    command
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if host == "codex" {
        command.args(args).args(["exec", "--json", "--cd"]).arg(cwd);
        if let Some(id) = session {
            command.args(["resume", id]);
        }
        command.arg("-");
    } else if host == "claude-code" {
        command
            .args(["-p", "--output-format", "stream-json", "--verbose"])
            .args(args);
        if let Some(id) = session {
            command.args(["--resume", id]);
        }
    } else {
        command.args(["--mode", "json", "-p"]).args(args);
        if let Some(file) = session {
            command.args(["--session", file]);
        }
    }
    Ok(command)
}

pub(super) fn launch(args: &[String]) -> Result<()> {
    let host = args.first().ok_or("host requires codex or claude-code")?;
    let flags = args.get(1..).unwrap_or_default();
    let flags = flags.strip_prefix(&["--".to_owned()]).unwrap_or(flags);
    validate(host, flags)?;
    let cwd = env::current_dir()?;
    let location = host_files::request(
        &cwd,
        HostAction::Open {
            host: host.clone(),
            session: acyclic_harness::OperationId::new().to_string(),
        },
    )?;
    let task = serde_json::from_value(location.get("task").cloned().ok_or("native task absent")?)?;
    let status = Command::new(program(host)?)
        .args(flags)
        .current_dir(&cwd)
        .env("ACYCLIC_HOST_ARGS", serde_json::to_string(flags)?)
        .env("ACYCLIC_WORKSPACE", &cwd)
        .env(
            "ACYCLIC_TASK",
            location
                .get("task")
                .and_then(Value::as_str)
                .ok_or("native task absent")?,
        )
        .status()?;
    host_files::request(&cwd, HostAction::Capture { task })?;
    if !status.success() {
        return Err(format!("native {host} exited {status}").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_launch_preserves_policy_cwd_and_resume_without_bypass() -> Result<()> {
        let flags = [
            "--sandbox",
            "workspace-write",
            "--ask-for-approval",
            "never",
        ]
        .map(str::to_owned);
        assert!(validate("codex", &flags[..2]).is_err());
        let command = child("codex", &flags, Path::new("view"), Some("resume-id"))?;
        assert_eq!(command.get_current_dir(), Some(Path::new("view")));
        assert_eq!(
            command
                .get_args()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            [
                "--sandbox",
                "workspace-write",
                "--ask-for-approval",
                "never",
                "exec",
                "--json",
                "--cd",
                "view",
                "resume",
                "resume-id",
                "-"
            ]
        );
        assert!(
            !command
                .get_args()
                .any(|arg| arg.to_string_lossy().contains("bypass"))
        );
        Ok(())
    }
}
