//! Drives the TUI case on a real pseudo-terminal.
//!
//! The case process is the terminal's foreground program, as `graphcoder`
//! is when a person runs it: it opens the terminal, negotiates, renders, and
//! reads keys from it. The driver types the prompt once the TUI has drawn its
//! composer, and quits once the scripted provider has served its last turn
//! and the TUI shows the session idle.
use super::{CASE, SETTLED};
use std::{
    io::{Read as _, Write as _},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// A hang is a failure, never a wait: the whole TUI turn takes seconds.
const DEADLINE: Duration = Duration::from_secs(90);

/// The terminal host: script(1) on Unix, the repository's ConPTY host on Windows.
fn host(executable: &Path, scratch: &Path) -> Command {
    #[cfg(target_os = "linux")]
    let command = {
        let mut command = Command::new("script");
        command.args(["-q", "-e", "-f", "-c"]).arg(format!(
            "stty rows 24 cols 80; ulimit -s 1024; exec '{}'",
            executable.display()
        ));
        command.arg("/dev/null");
        command
    };
    #[cfg(all(unix, not(target_os = "linux")))]
    let command = {
        let mut command = Command::new("script");
        command
            .args(["-q", "/dev/null", "/bin/sh", "-c"])
            .arg(format!(
                "stty rows 24 cols 80; ulimit -s 1024; exec '{}'",
                executable.display()
            ));
        command
    };
    #[cfg(windows)]
    let command = {
        let scripts = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(6)
            .expect("repository root")
            .join("scripts");
        let catalog = scratch.join("unused-catalog.json");
        std::fs::write(&catalog, b"{}").unwrap();
        let mut command = Command::new("powershell.exe");
        command
            .args(["-NoProfile", "-NonInteractive", "-File"])
            .arg(scripts.join("graphcoder-conpty.ps1"))
            .arg("-Binary")
            .arg(executable)
            .arg("-Workspace")
            .arg(scratch)
            .arg("-Catalog")
            .arg(catalog)
            .arg("-SettingsRoot")
            .arg(scratch)
            .args(["-DeadlineMilliseconds", "90000"]);
        command
    };
    #[cfg(unix)]
    let _ = scratch;
    command
}

/// The visible text of terminal output: escape sequences removed.
fn visible(output: &[u8]) -> String {
    let text = String::from_utf8_lossy(output);
    let mut visible = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            visible.push(c);
            continue;
        }
        match chars.next() {
            // CSI: parameters and intermediates up to a final byte.
            Some('[') => {
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
            // OSC, DCS and the other strings: up to BEL or ST.
            Some(']' | 'P' | '_' | '^' | 'X') => {
                while let Some(c) = chars.next() {
                    if c == '\u{7}' || (c == '\u{1b}' && chars.peek() == Some(&'\\')) {
                        chars.next();
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    visible
}

struct Session {
    child: Child,
    output: Arc<Mutex<Vec<u8>>>,
    started: Instant,
}

impl Session {
    /// Waits until `ready` holds for the output so far, failing at the deadline.
    fn until(&mut self, what: &str, ready: impl Fn(&[u8]) -> bool) {
        loop {
            if ready(&self.output.lock().unwrap()) {
                return;
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("the TUI exited ({status}) before {what}: {}", self.screen());
            }
            if self.started.elapsed() > DEADLINE {
                let _ = self.child.kill();
                panic!("the TUI did not reach {what}: {}", self.screen());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn screen(&self) -> String {
        let output = self.output.lock().unwrap();
        let text = visible(&output);
        let skipped = text.chars().count().saturating_sub(2000);
        text.chars().skip(skipped).collect()
    }

    fn send(&mut self, keys: &[u8]) {
        let stdin = self.child.stdin.as_mut().unwrap();
        stdin.write_all(keys).unwrap();
        stdin.flush().unwrap();
    }
}

/// Runs the TUI case on a real terminal and requires it to pass its budget.
pub(super) fn run(executable: &Path) {
    let scratch = tempfile::tempdir().unwrap();
    let settled = scratch.path().join("settled");
    let mut child = host(executable, scratch.path())
        .env(CASE, "tui")
        .env(SETTLED, &settled)
        .env("TERM", "xterm-256color")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = Arc::new(Mutex::new(Vec::new()));
    let reader = {
        let output = Arc::clone(&output);
        let mut stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            let mut bytes = [0; 4096];
            while let Ok(count @ 1..) = stdout.read(&mut bytes) {
                output.lock().unwrap().extend_from_slice(&bytes[..count]);
            }
        })
    };
    let mut session = Session {
        child,
        output,
        started: Instant::now(),
    };
    // The composer is drawn and the cursor shown: the TUI reads keys now.
    session.until("its composer", |output| {
        visible(output).contains("GraphCoder") && output.windows(6).any(|w| w == b"\x1b[?25h")
    });
    session.send(b"\x1b[200~go\x1b[201~\r");
    session.until("the scripted turn's end", |_| settled.exists());
    let served = session.output.lock().unwrap().len();
    // The session shows idle again after the last turn.
    session.until("an idle session", |output| {
        visible(&output[served.min(output.len())..]).contains('\u{25cb}')
    });
    session.send(b"\x04");
    let status = loop {
        if let Some(status) = session.child.try_wait().unwrap() {
            break status;
        }
        if session.started.elapsed() > DEADLINE {
            let _ = session.child.kill();
            panic!("the TUI did not quit: {}", session.screen());
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    reader.join().unwrap();
    // The case reports its depth on the terminal it ran on.
    let report = visible(&session.output.lock().unwrap());
    if let Some(line) = report
        .lines()
        .find(|line| line.contains("GraphCoder turn:"))
    {
        eprintln!("tui: {}", line.trim());
    }
    assert!(
        status.success(),
        "main-thread tui ({status}): {}",
        session.screen()
    );
}
