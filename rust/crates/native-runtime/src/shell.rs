use std::process::Command;

/// Constructs a native host command, including npm's Windows batch launchers.
/// Callers append literal arguments and retain their existing process owner.
/// Rust's `Command` owns batch-file invocation and escaping; no shell script is
/// assembled here. Resolve before a caller changes the child's working directory.
#[must_use]
pub fn host_command(program: impl AsRef<std::ffi::OsStr>) -> Command {
    let program = program.as_ref();
    #[cfg(windows)]
    if let Some(path) = resolve_host_program(program) {
        return Command::new(path);
    }
    Command::new(program)
}

#[cfg(windows)]
fn resolve_host_program(program: &std::ffi::OsStr) -> Option<std::path::PathBuf> {
    use std::path::{Path, PathBuf};
    let program = Path::new(program);
    let current = std::env::current_dir().ok()?;
    let directories = if program.components().count() > 1 || program.is_absolute() {
        vec![PathBuf::new()]
    } else {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()).collect()
    };
    let extensions = std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".into());
    for directory in directories {
        let candidate = current.join(directory).join(program);
        if program.extension().is_none() {
            for extension in extensions.split(';').filter(|extension| {
                [".com", ".exe", ".bat", ".cmd"]
                    .iter()
                    .any(|supported| extension.eq_ignore_ascii_case(supported))
            }) {
                let mut path = candidate.as_os_str().to_os_string();
                path.push(extension);
                let path = PathBuf::from(path);
                if path.is_file() {
                    return Some(path);
                }
            }
        }
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Selects the native command shell shared by hooks and workspace process tools.
/// The caller supplies the workspace, environment, streams and process owner.
#[must_use]
pub fn shell_command(command: &str, selected: Option<&str>) -> Command {
    let selected = selected.unwrap_or(if cfg!(windows) { "powershell" } else { "sh" });
    let mut shell = Command::new(selected);
    if matches!(selected, "powershell" | "pwsh") {
        shell.args(["-NoProfile", "-NonInteractive", "-Command", command]);
    } else if selected == "cmd" {
        shell.args(["/d", "/s", "/c"]);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            shell.raw_arg(format!("\"{command}\""));
        }
        #[cfg(not(windows))]
        shell.arg(command);
    } else {
        shell.args(["-c", command]);
    }
    shell
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_selection_preserves_the_script_as_one_argument() {
        for selected in [
            None,
            Some("sh"),
            Some("bash"),
            Some("powershell"),
            Some("pwsh"),
        ] {
            let command = shell_command("echo 'a b'; exit 3", selected);
            let program = selected.unwrap_or(if cfg!(windows) { "powershell" } else { "sh" });
            assert_eq!(command.get_program(), program);
            let args = command.get_args().collect::<Vec<_>>();
            let expected = if matches!(program, "powershell" | "pwsh") {
                vec![
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    "echo 'a b'; exit 3",
                ]
            } else {
                vec!["-c", "echo 'a b'; exit 3"]
            };
            assert_eq!(args, expected);
        }
    }
}
