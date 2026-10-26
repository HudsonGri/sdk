//! The executable adapter for the existing shared process-tree owner.
//!
//! This module owns no Job, process-group enumeration or reaper. Both ordinary
//! external commands and `ConPTY` transports enter through `ProcessCustody`.

use super::{ProcessCustody, ProcessTree};
use std::ffi::OsString;
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};

struct State {
    tree: ProcessTree,
    cancelled: bool,
    failure: Option<io::Error>,
}

struct Directory(PathBuf);

#[derive(Debug)]
struct Invocation {
    executable: OsString,
    arguments: Vec<OsString>,
    inherit_input: bool,
    custody: Option<PathBuf>,
    completion: Option<PathBuf>,
}

impl Invocation {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
        let mut arguments = arguments.into_iter();
        let mut inherit_input = false;
        let mut custody = None;
        let mut completion = None;
        while let Some(argument) = arguments.next() {
            if argument == "--inherit-input" && !inherit_input {
                inherit_input = true;
            } else if argument == "--custody" || argument == "--completion" {
                let path = arguments.next().ok_or_else(invalid_arguments)?;
                if !Path::new(&path).is_absolute() {
                    return Err(invalid_arguments());
                }
                let destination = if argument == "--custody" {
                    &mut custody
                } else {
                    &mut completion
                };
                if destination.replace(PathBuf::from(path)).is_some() {
                    return Err(invalid_arguments());
                }
            } else if Path::new(&argument).is_absolute() {
                return Ok(Self {
                    executable: argument,
                    arguments: arguments.collect(),
                    inherit_input,
                    custody,
                    completion,
                });
            } else {
                return Err(invalid_arguments());
            }
        }
        Err(invalid_arguments())
    }
}

fn invalid_arguments() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "step=process-host-arguments; recovery=acyclic-process-host [--custody ABSOLUTE_DIRECTORY] [--completion ABSOLUTE_FILE] [--inherit-input] ABSOLUTE_EXECUTABLE ARGS",
    )
}

struct Completion(Option<std::fs::File>);

impl Completion {
    fn open(path: Option<&Path>) -> io::Result<Self> {
        path.map(|path| {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
        })
        .transpose()
        .map(Self)
    }

    fn retired(&mut self, code: i32, cancelled: bool) -> io::Result<()> {
        if let Some(file) = &mut self.0 {
            writeln!(
                file,
                "{{\"schema\":\"acyclic.process-custody-completion.v1\",\"process_tree_retired\":true,\"exit_code\":{code},\"cancelled\":{cancelled}}}"
            )?;
            file.sync_all()?;
        }
        Ok(())
    }
}

impl Directory {
    fn create() -> io::Result<Self> {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "acyclic-process-host-{}-{epoch}",
            std::process::id()
        ));
        // An existing name is rejected before any command starts.
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        // Never erase an outstanding custody record on failed cleanup.
        let _ = std::fs::remove_dir(&self.0);
    }
}

/// Runs one external command through the native runtime's process custody.
///
/// Arguments are an absolute executable followed by its literal arguments.
/// With `--inherit-input` before that executable, the child receives stdin
/// unchanged (for a `ConPTY` transport). Otherwise the child receives EOF and
/// input or EOF on the host's stdin requests cancellation. Output is inherited
/// unchanged; on Windows the custodian keeps its pipe leases through physical
/// retirement of the admitted root and watched descendants, including when
/// this host is killed outright. Terminal adapters admit their suspended root
/// through the same custodian before resuming it.
///
/// `--completion ABSOLUTE_FILE` exclusively reserves a receipt before launch
/// and writes its v1 completion only after physical process-tree retirement.
/// Failed admission or drain never produces a completed receipt. It confers
/// no mount-retirement capability. `--custody ABSOLUTE_DIRECTORY` reuses the
/// shared custody fence before admission; the caller must already serialize
/// this directory's owners. Without it the invocation has private custody.
///
/// This is an executable entry point: the caller must exit after it returns.
/// The control reader can still be blocked on stdin after a normal command
/// exit, and belongs to that process rather than a reusable runtime service.
pub fn run_external_process_host(arguments: impl IntoIterator<Item = OsString>) -> io::Result<i32> {
    let invocation = Invocation::parse(arguments)?;
    let mut completion = Completion::open(invocation.completion.as_deref())?;
    let directory = invocation
        .custody
        .is_none()
        .then(Directory::create)
        .transpose()?;
    let custody_path = invocation
        .custody
        .as_deref()
        .or_else(|| directory.as_ref().map(|directory| directory.0.as_path()))
        .ok_or_else(invalid_arguments)?;
    let custody = ProcessCustody::acquire(custody_path)?;
    let mut command = Command::new(invocation.executable);
    command
        .args(invocation.arguments)
        .stdin(if invocation.inherit_input {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody)?;
    // The sole waiter owns the actual Child. Cancellation only asks the same
    // tree's containment owner to terminate; it never substitutes PID polling.
    let child = tree.child.take().ok_or(io::ErrorKind::NotFound)?;
    tree.cleanup_on_drop = false;
    let state = Arc::new(Mutex::new(State {
        tree,
        cancelled: false,
        failure: None,
    }));
    if !invocation.inherit_input {
        let control = Arc::clone(&state);
        std::thread::Builder::new()
            .name("process-host-control".into())
            .spawn(move || {
                let input = io::stdin().read(&mut [0]);
                let mut state = control
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.cancelled = true;
                state.failure = state
                    .tree
                    .terminate_descendants()
                    .err()
                    .or_else(|| input.err());
            })?;
    }
    let waited = super::platform::wait_without_reaping(&child);
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let status = retire_after_observation(&mut state.tree, child, waited)?;
    if let Some(failure) = state.failure.take() {
        return Err(failure);
    }
    let code = if state.cancelled {
        124
    } else {
        exit_code(status)
    };
    completion.retired(code, state.cancelled)?;
    Ok(code)
}

fn retire_after_observation(
    tree: &mut ProcessTree,
    child: Child,
    observed: io::Result<()>,
) -> io::Result<ExitStatus> {
    // Restore the original child before entering the one ordered retirement
    // API. Failed observation leaves both child and durable recovery record
    // owned; the adapter never removes records or closes containment itself.
    tree.child = Some(child);
    observed?;
    let status = tree.try_wait(None)?.ok_or(io::ErrorKind::WouldBlock)?;
    tree.terminate(None)?;
    Ok(status)
}

fn exit_code(status: ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        status
            .code()
            .unwrap_or_else(|| 128 + status.signal().unwrap_or(1))
    }
    #[cfg(windows)]
    status.code().unwrap_or(125)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_host_observation_keeps_child_and_recovery_authority() {
        let root = tempfile::tempdir().unwrap();
        let custody = ProcessCustody::open(root.path()).unwrap();
        let mut command = super::super::tests::command("exit-code", root.path());
        let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody).unwrap();
        let child = tree.child.take().unwrap();
        tree.cleanup_on_drop = false;
        assert_eq!(
            retire_after_observation(&mut tree, child, Err(io::ErrorKind::Interrupted.into()))
                .unwrap_err()
                .kind(),
            io::ErrorKind::Interrupted,
        );
        assert!(
            tree.child.is_some(),
            "failed wait must retain the original child"
        );
        assert!(
            tree.custody.is_some(),
            "failed wait must retain recovery authority"
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        tree.terminate(None).unwrap();
        assert!(tree.is_reaped());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn process_host_options_stop_at_the_literal_executable() {
        let executable = std::env::current_exe().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let receipt = directory.path().join("completion.json");
        let parsed = Invocation::parse([
            "--completion".into(),
            receipt.clone().into_os_string(),
            "--custody".into(),
            directory.path().as_os_str().to_owned(),
            "--inherit-input".into(),
            executable.clone().into_os_string(),
            "--completion".into(),
            "literal & % child argument".into(),
        ])
        .unwrap();
        assert_eq!(parsed.executable, executable.as_os_str());
        assert_eq!(parsed.completion.as_deref(), Some(receipt.as_path()));
        assert_eq!(parsed.custody.as_deref(), Some(directory.path()));
        assert!(parsed.inherit_input);
        assert_eq!(
            parsed.arguments,
            ["--completion", "literal & % child argument"]
        );
    }

    #[test]
    fn process_host_rejects_relative_and_duplicate_receipt_admission() {
        let executable = std::env::current_exe().unwrap().into_os_string();
        let receipt = std::env::temp_dir().join("receipt.json").into_os_string();
        for arguments in [
            vec!["--completion".into(), "relative".into(), executable.clone()],
            vec!["--custody".into(), "relative".into(), executable.clone()],
            vec![
                "--completion".into(),
                receipt.clone(),
                "--completion".into(),
                receipt,
                executable,
            ],
        ] {
            assert_eq!(
                Invocation::parse(arguments).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn process_completion_is_exclusive_and_never_publishes_before_retirement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("completion.json");
        let mut completion = Completion::open(Some(&path)).unwrap();
        assert!(std::fs::read(&path).unwrap().is_empty());
        assert_eq!(
            Completion::open(Some(&path)).err().unwrap().kind(),
            io::ErrorKind::AlreadyExists
        );
        completion.retired(17, false).unwrap();
        let receipt = std::fs::read_to_string(path).unwrap();
        assert!(receipt.contains("\"exit_code\":17"));
        assert!(receipt.contains("\"process_tree_retired\":true"));
        assert!(!receipt.contains("mount"));
    }
}
