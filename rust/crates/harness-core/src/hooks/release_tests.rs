//! A hook or stdio MCP server that the host starts must not hold the host's
//! own output: a reader of that output sees its end when the host closes
//! it, even while the started process still runs.
//!
//! The test runs an owner (this test executable) whose output is a pipe
//! the test reads. The owner starts a parked child through the entry point,
//! closes its output, and waits; the parked child holds an exclusive lock
//! while it runs. The negative control starts the same child through
//! `std`, which hands it every inheritable handle, the owner's output among
//! them.

use super::native::run_hook;
use super::{HookFailure, HookRun, HookSpec};
use crate::native_environment::variable;
use acyclic_native_runtime::command::{Command, Stdio};
use acyclic_native_runtime::{FileLock, FileLockMode};
use std::fs::{self, File};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

const MODE: &str = "ACYCLIC_RELEASE_TEST_MODE";
const ROOT: &str = "ACYCLIC_RELEASE_TEST_ROOT";
const PARK: &str = "ACYCLIC_RELEASE_TEST_PARK";
const OWNER: &str = "hooks::release_tests::release_owner";
const PARKED: &str = "hooks::release_tests::release_parked_child";

fn exe() -> PathBuf {
    std::env::current_exe().expect("test executable")
}

fn lock_path(root: &Path) -> PathBuf {
    root.join("parked.lock")
}

/// The parked child's arguments: this executable, running only
/// [`release_parked_child`].
fn parked_arguments() -> [&'static str; 3] {
    ["--exact", PARKED, "--nocapture"]
}

/// Serves the parked child: holds the lock, publishes its PID, and runs
/// until killed.
#[test]
fn release_parked_child() {
    let Some(root) = variable(PARK) else {
        return;
    };
    let root = PathBuf::from(root);
    let lock = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path(&root))
        .expect("lock file");
    let _held = FileLock::acquire(lock, FileLockMode::Exclusive).expect("hold lock");
    fs::write(root.join(format!("parked-{}", std::process::id())), b"").expect("publish");
    loop {
        thread::park();
    }
}

/// Serves the owner: starts the parked child as its mode says, closes its
/// own output, and runs until killed.
#[test]
fn release_owner() {
    let Some(mode) = variable(MODE) else {
        return;
    };
    let mode = mode.into_string().expect("UTF-8 mode");
    let root = PathBuf::from(variable(ROOT).expect("owner root"));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let _started: Box<dyn std::any::Any> = match mode.as_str() {
        "hook" => {
            let hook = Arc::new(HookSpec {
                run: HookRun::Executable {
                    executable: exe().to_str().expect("UTF-8 executable").to_owned(),
                    argv: parked_arguments().map(str::to_owned).to_vec(),
                },
                timeout_ms: 600_000,
                failure: HookFailure::Report,
            });
            let request = Arc::new((root.clone(), root.clone(), b"{}".to_vec()));
            Box::new(runtime.spawn(run_hook(hook, request, Arc::new(|| Ok(())), 65_536)))
        }
        #[cfg(feature = "mcp-stdio")]
        "mcp" => {
            // The test harness reports on its output, which an MCP client
            // reads as protocol; the server's own child writes elsewhere.
            let mut command = Command::new("cmd.exe");
            command.raw_arg(format!(
                "/d /c \"\"{}\" {} > NUL\"",
                exe().display(),
                parked_arguments().join(" ")
            ));
            Box::new(runtime.spawn(crate::mcp::stdio::StdioMcpTransport::spawn(
                command, 65_536, 600_000,
            )))
        }
        "control" => Box::new(
            std::process::Command::new(exe())
                .args(parked_arguments())
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("std child"),
        ),
        other => panic!("unknown mode {other}"),
    };
    while parked(&root).is_none() {
        thread::sleep(Duration::from_millis(5));
    }
    acyclic_native_runtime::testing::close_standard_output().expect("close output");
    fs::write(root.join("owner-ready"), b"").expect("ready");
    loop {
        thread::park();
    }
}

fn parked(root: &Path) -> Option<u32> {
    fs::read_dir(root).ok()?.flatten().find_map(|entry| {
        entry
            .file_name()
            .to_str()?
            .strip_prefix("parked-")?
            .parse()
            .ok()
    })
}

/// Whether the parked child still holds its lock.
fn held(root: &Path) -> bool {
    let file = File::open(lock_path(root)).expect("lock file");
    matches!(
        FileLock::try_acquire(file, FileLockMode::Exclusive),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    )
}

/// Starts an owner in `mode` and reports whether its output ended within
/// `bound` after it closed it, and whether the parked child still ran when
/// the result was read. Returns the parked child's PID and the root.
fn output_ends_while_the_child_runs(
    mode: &str,
    bound: Duration,
) -> (bool, bool, u32, tempfile::TempDir) {
    let root = tempfile::tempdir().expect("root");
    let mut command = Command::new(exe());
    command
        .args(["--exact", OWNER, "--nocapture"])
        .env(MODE, mode)
        .env(ROOT, root.path())
        .env(PARK, root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut owner = command.spawn().expect("owner");
    let mut output = owner.stdout.take().expect("owner output");
    let (ended, end) = mpsc::channel();
    thread::spawn(move || {
        let _ = ended.send(output.read_to_end(&mut Vec::new()).map(|_| ()));
    });
    while !root.path().join("owner-ready").exists() {
        assert!(
            owner.try_wait().expect("owner").is_none(),
            "owner exited early"
        );
        thread::sleep(Duration::from_millis(5));
    }
    let reached = end.recv_timeout(bound);
    let running = held(root.path());
    let pid = parked(root.path()).expect("parked child");
    owner.kill().expect("kill owner");
    owner.wait().expect("reap owner");
    (matches!(reached, Ok(Ok(()))), running, pid, root)
}

/// Asserts the parked child dies with its owner: its lock is released.
fn dies_with_its_owner(root: &Path) {
    let file = File::open(lock_path(root)).expect("lock file");
    let (released, release) = mpsc::channel();
    thread::spawn(move || {
        let _ = released.send(FileLock::acquire(file, FileLockMode::Exclusive).map(drop));
    });
    assert!(
        matches!(release.recv_timeout(Duration::from_secs(30)), Ok(Ok(()))),
        "the started tree outlived its owner"
    );
}

fn releases_the_owners_output(mode: &str) {
    // The bound only detects an output the child still holds.
    let (ended, running, _, root) = output_ends_while_the_child_runs(mode, Duration::from_secs(30));
    assert!(ended, "the {mode} child held the owner's output");
    assert!(running, "the {mode} child ended before the output did");
    dies_with_its_owner(root.path());
}

/// Negative control: a child started through `std` holds the output.
fn std_holds_the_owners_output() {
    let (ended, running, pid, _root) =
        output_ends_while_the_child_runs("control", Duration::from_secs(2));
    let killed = std::process::Command::new("taskkill")
        .args(["/F", "/PID", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("taskkill");
    assert!(killed.success(), "{killed}");
    assert!(!ended, "a std-started child released the owner's output");
    assert!(running, "the std-started child ended");
}

#[test]
fn a_hook_releases_the_owners_output() {
    releases_the_owners_output("hook");
    std_holds_the_owners_output();
}

#[cfg(feature = "mcp-stdio")]
#[test]
fn a_stdio_mcp_server_releases_the_owners_output() {
    releases_the_owners_output("mcp");
    std_holds_the_owners_output();
}
