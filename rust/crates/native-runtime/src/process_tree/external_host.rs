//! The executable adapter for the existing shared process-tree owner.
//!
//! This module owns no Job, process-group enumeration or reaper. Both ordinary
//! external commands and `ConPTY` transports enter through `ProcessCustody`.

use super::{ProcessCustody, ProcessTree};
use std::ffi::OsString;
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};

struct State {
    tree: ProcessTree,
    cancelled: bool,
    failure: Option<io::Error>,
}

fn retire_child(state: &mut State, child: Child) -> io::Result<ExitStatus> {
    // Return the original child before entering the one ordered retirement
    // path. In particular, Unix must retain its unreaped identity if removing
    // the recovery record fails.
    state.tree.child = Some(child);
    let status = state.tree.try_wait(None);
    state.tree.terminate(None)?;
    status?.ok_or_else(|| io::ErrorKind::NotFound.into())
}

struct Directory(PathBuf);

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
/// This is an executable entry point: the caller must exit after it returns.
/// The control reader can still be blocked on stdin after a normal command
/// exit, and belongs to that process rather than a reusable runtime service.
pub fn run_external_process_host(arguments: impl IntoIterator<Item = OsString>) -> io::Result<i32> {
    let mut arguments = arguments.into_iter();
    let first = arguments.next().ok_or(io::ErrorKind::InvalidInput)?;
    let inherit_input = first == "--inherit-input";
    let executable = if inherit_input {
        arguments.next().ok_or(io::ErrorKind::InvalidInput)?
    } else {
        first
    };
    if !Path::new(&executable).is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process host requires an absolute executable",
        ));
    }
    let directory = Directory::create()?;
    let custody = ProcessCustody::open(&directory.0)?;
    let mut command = Command::new(executable);
    command
        .args(arguments)
        .stdin(if inherit_input {
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
    if !inherit_input {
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
    // The Child has crossed its physical wait. Keep all original custody
    // records and output leases until the same owner has drained descendants.
    let status = retire_child(&mut state, child);
    waited?;
    let status = status?;
    if let Some(failure) = state.failure.take() {
        return Err(failure);
    }
    if state.cancelled {
        return Ok(124);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        Ok(status
            .code()
            .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)))
    }
    #[cfg(windows)]
    Ok(status.code().unwrap_or(125))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_retirement_preserves_the_observed_native_exit_status() {
        let root = tempfile::tempdir().unwrap();
        let custody = ProcessCustody::open(&root.path().join("custody")).unwrap();
        let mut tree = ProcessTree::spawn_in_custody(
            &mut super::super::tests::command("exit-code", root.path()),
            &custody,
        )
        .unwrap();
        let child = tree.child.take().unwrap();
        super::super::platform::wait_without_reaping(&child).unwrap();
        let mut state = State {
            tree,
            cancelled: false,
            failure: None,
        };
        assert_eq!(retire_child(&mut state, child).unwrap().code(), Some(7));
        assert!(state.tree.is_reaped());
    }

    #[test]
    fn failed_external_retirement_keeps_the_canonical_recovery_owner() {
        let root = tempfile::tempdir().unwrap();
        let records = root.path().join("custody");
        let custody = ProcessCustody::open(&records).unwrap();
        let mut tree = ProcessTree::spawn_in_custody(
            &mut super::super::tests::command("partial", root.path()),
            &custody,
        )
        .unwrap();
        super::super::tests::ready(root.path(), "partial-ready", || {
            tree.try_wait(None).map_or(true, |status| status.is_some())
        });
        let record = std::fs::read_dir(&records)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        // A real failed unlink, rather than a synthetic process result.
        std::fs::remove_file(&record).unwrap();
        std::fs::create_dir(&record).unwrap();
        tree.terminate_descendants().unwrap();
        let child = tree.child.take().unwrap();
        super::super::platform::wait_without_reaping(&child).unwrap();
        let mut state = State {
            tree,
            cancelled: false,
            failure: None,
        };
        let failed = retire_child(&mut state, child).is_err();
        let retained = state.tree.custody.is_some() && !state.tree.is_reaped();
        #[cfg(unix)]
        let unreaped = state.tree.child.is_some();
        // Restore the actual failed filesystem boundary before assertions so
        // even an old implementation's failed oracle can drain its owner.
        std::fs::remove_dir(&record).unwrap();
        state.tree.terminate(None).unwrap();
        assert!(failed);
        assert!(retained);
        #[cfg(unix)]
        assert!(
            unreaped,
            "failed record retirement must retain the unreaped child"
        );
        assert!(state.tree.is_reaped());
        assert_eq!(std::fs::read_dir(&records).unwrap().count(), 0);
    }
}
