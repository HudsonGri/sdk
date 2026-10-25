//! Thin command protocol over the sole native process-tree implementation.
use acyclic_native_runtime::{ProcessCustody, ProcessTree, process_admission::ProcessAdmission};
use serde_json::{Value, json};
use std::io::{self, BufRead as _, Read as _, Write as _};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender};

const MAX_CONTROL_BYTES: u64 = 1024 * 1024;
enum Event {
    Command(Value),
    Closed,
    Invalid,
    Exited(io::Result<()>),
    Output(usize, Vec<u8>),
    OutputEnd(io::Result<()>),
}

fn controls(sender: &SyncSender<Event>) {
    let mut input = io::stdin().lock();
    loop {
        let mut line = Vec::new();
        let read = input
            .by_ref()
            .take(MAX_CONTROL_BYTES + 1)
            .read_until(b'\n', &mut line);
        let event = match read {
            Ok(0) => Event::Closed,
            Ok(_) if line.len() as u64 <= MAX_CONTROL_BYTES && line.last() == Some(&b'\n') => {
                match serde_json::from_slice(&line) {
                    Ok(value) => Event::Command(value),
                    Err(_) => Event::Invalid,
                }
            }
            _ => Event::Invalid,
        };
        let last = matches!(event, Event::Closed | Event::Invalid);
        if sender.send(event).is_err() || last {
            return;
        }
    }
}

fn reply(value: &Value) -> io::Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, value)?;
    writeln!(output)?;
    output.flush()
}

fn command(value: &Value) -> io::Result<Command> {
    let argv = value
        .get("argv")
        .and_then(Value::as_array)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let values: Vec<&str> = argv
        .iter()
        .map(|v| v.as_str().ok_or(io::ErrorKind::InvalidInput))
        .collect::<Result<_, _>>()?;
    let (program, args) = values.split_first().ok_or(io::ErrorKind::InvalidInput)?;
    if !Path::new(program).is_absolute() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let mut command = Command::new(program);
    command.args(args).stdin(Stdio::null());
    if let Some(cwd) = value.get("cwd") {
        command.current_dir(cwd.as_str().ok_or(io::ErrorKind::InvalidInput)?);
    }
    if let Some(environment) = value.get("env") {
        command.env_clear();
        for (key, value) in environment.as_object().ok_or(io::ErrorKind::InvalidInput)? {
            command.env(key, value.as_str().ok_or(io::ErrorKind::InvalidInput)?);
        }
    }
    Ok(command)
}

fn publish_output(log: &mut Option<std::fs::File>, stream: usize, bytes: &[u8]) -> io::Result<()> {
    let log = log.as_mut().ok_or(io::ErrorKind::InvalidData)?;
    log.write_all(bytes)?;
    log.flush()?;
    reply(&json!({"stream":stream,"output":bytes}))
}

/// A command completes only after the same owner drains its entire tree.
fn execute(
    mut command: Command,
    custody: &ProcessCustody,
    sender: &SyncSender<Event>,
    events: &Receiver<Event>,
    mut log: Option<std::fs::File>,
) -> io::Result<Option<i32>> {
    if log.is_some() {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
    }
    let mut tree = ProcessTree::spawn_in_custody(&mut command, custody)?;
    let watch = tree.exit_notification()?;
    let exit_sender = sender.clone();
    let waiter = std::thread::Builder::new()
        .name("process-exit".into())
        .spawn(move || {
            let _ = exit_sender.send(Event::Exited(watch.await_exit()));
        })?;
    let mut readers = Vec::new();
    if log.is_some() {
        let pipes: [Box<dyn io::Read + Send>; 2] = [
            Box::new(tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?),
            Box::new(tree.take_stderr().ok_or(io::ErrorKind::BrokenPipe)?),
        ];
        for (stream, mut pipe) in pipes.into_iter().enumerate() {
            let output_sender = sender.clone();
            readers.push(
                std::thread::Builder::new()
                    .name("process-output".into())
                    .spawn(move || {
                        let result = (|| {
                            let mut bytes = [0; 4096];
                            loop {
                                let read = pipe.read(&mut bytes)?;
                                if read == 0 {
                                    return Ok(());
                                }
                                output_sender
                                    .send(Event::Output(
                                        stream,
                                        bytes
                                            .get(..read)
                                            .ok_or(io::ErrorKind::InvalidData)?
                                            .to_vec(),
                                    ))
                                    .map_err(|_| io::ErrorKind::BrokenPipe)?;
                            }
                        })();
                        let _ = output_sender.send(Event::OutputEnd(result));
                    })?,
            );
        }
    }
    let mut exited = false;
    let mut ends = 0;
    let status = loop {
        match events.recv() {
            Ok(Event::Output(stream, bytes)) => {
                let result = publish_output(&mut log, stream, &bytes);
                if let Err(error) = result {
                    break Err(error);
                }
            }
            Ok(Event::OutputEnd(result)) => {
                ends += 1;
                if let Err(error) = result {
                    break Err(error);
                }
            }
            Ok(Event::Exited(result)) => {
                exited = true;
                break result.and_then(|()| tree.await_exit().map(Some));
            }
            Ok(Event::Closed) => break Ok(None),
            _ => break Err(io::ErrorKind::InvalidData.into()),
        }
    };
    // Never publish completion, release admission, or accept another command
    // while a descendant or its custodian can still act.
    let drain = tree.terminate(None);
    // Pump already-owned reader/exit completions after drain. No observer can
    // remain blocked sending into a full control queue while we join it.
    while !exited || ends < readers.len() {
        match events.recv() {
            Ok(Event::Exited(_)) => exited = true,
            Ok(Event::OutputEnd(_)) => ends += 1,
            Ok(Event::Output(stream, bytes)) => {
                if let Some(log) = &mut log {
                    log.write_all(&bytes)?;
                }
                if status.is_ok() {
                    reply(&json!({"stream":stream,"output":bytes}))?;
                }
            }
            Ok(_) => {}
            Err(_) => return Err(io::ErrorKind::BrokenPipe.into()),
        }
    }
    waiter
        .join()
        .map_err(|_| io::Error::other("process exit waiter panicked"))?;
    for reader in readers {
        reader
            .join()
            .map_err(|_| io::Error::other("process output reader panicked"))?;
    }
    drain?;
    Ok(status?.map(|status| status.code().unwrap_or(1)))
}

fn serve(
    root: &Path,
    sender: &SyncSender<Event>,
    events: &Receiver<Event>,
    fleet: bool,
) -> io::Result<()> {
    #[cfg(windows)]
    let admission = if fleet {
        ProcessAdmission::acquire_fleet(root)?
    } else {
        ProcessAdmission::acquire(root)?
    };
    #[cfg(not(windows))]
    let admission = {
        if fleet {
            return Err(io::ErrorKind::Unsupported.into());
        }
        ProcessAdmission::acquire(root)?
    };
    reply(&json!({"ready":true,"lease_wait_seconds":admission.lease_wait().as_secs_f64()}))?;
    loop {
        match events.recv() {
            Ok(Event::Command(value)) => {
                let child = command(&value)?;
                let log = value
                    .get("log")
                    .and_then(Value::as_str)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                let log = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(log)?;
                match execute(child, admission.custody(), sender, events, Some(log))? {
                    Some(code) => reply(&json!({"exit":code}))?,
                    None => return Ok(()),
                }
            }
            Ok(Event::Closed) => return Ok(()),
            _ => return Err(io::ErrorKind::InvalidData.into()),
        }
    }
}

fn main() {
    let result = run();
    match result {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("process host: {error}");
            std::process::exit(125);
        }
    }
}

/// Exclusive scratch ownership for a one-command transport adapter.
struct Directory(std::path::PathBuf);
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
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        // Failed retirement preserves its durable custody record.
        let _ = std::fs::remove_dir(&self.0);
    }
}

fn run() -> io::Result<i32> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let (mode, rest) = args.split_first().ok_or(io::ErrorKind::InvalidInput)?;
    if mode == "--fence" {
        let [root] = rest else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        ProcessCustody::open(Path::new(root))?.fence_stale()?;
        return Ok(0);
    }
    let inherit_input = mode == "--inherit-input";
    let (sender, events) = mpsc::sync_channel(1);
    if !inherit_input {
        let control = sender.clone();
        std::thread::Builder::new()
            .name("process-control".into())
            .spawn(move || controls(&control))?;
    }
    if mode == "--session" || mode == "--fleet-session" {
        let [root] = rest else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        serve(Path::new(root), &sender, &events, mode == "--fleet-session")?;
        return Ok(0);
    }
    let directory;
    let (root, argv) = if mode == "--run" {
        let (root, argv) = rest.split_first().ok_or(io::ErrorKind::InvalidInput)?;
        (Path::new(root), argv)
    } else {
        directory = Directory::create()?;
        (
            directory.0.as_path(),
            if inherit_input { rest } else { args.as_slice() },
        )
    };
    let (program, args) = argv.split_first().ok_or(io::ErrorKind::InvalidInput)?;
    if !Path::new(program).is_absolute() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let admission = ProcessAdmission::acquire(root)?;
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(if inherit_input {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    Ok(execute(command, admission.custody(), &sender, &events, None)?.unwrap_or(124))
}
