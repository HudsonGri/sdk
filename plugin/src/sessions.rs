//! MCP coordination shim. Native CLIs own agent loops, tools and policy.
use super::*;
use acyclic_harness::{local::host_owner::SessionAction, task_volume::Control};
use acyclic_native_runtime::{FileLock, FileLockMode, ProcessCustody, ProcessTree};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

struct Running {
    tree: ProcessTree,
    input: Option<std::thread::JoinHandle<io::Result<()>>>,
    turn: u64,
    events: EventLines,
    answer: String,
    session: Option<String>,
}
fn spawn_input(
    mut input: impl io::Write + Send + 'static,
    prompt: Vec<u8>,
) -> io::Result<std::thread::JoinHandle<io::Result<()>>> {
    std::thread::Builder::new().spawn(move || input.write_all(&prompt))
}
impl Running {
    fn finish_input(&mut self) -> Result<()> {
        if let Some(input) = self.input.take() {
            input
                .join()
                .map_err(|_| io::Error::other("native input writer panicked"))??;
        }
        Ok(())
    }
    fn check_input(&mut self) -> Result<()> {
        if self.input.as_ref().is_some_and(|input| input.is_finished()) {
            self.finish_input()?;
        }
        Ok(())
    }
}
#[derive(Default)]
struct EventLines {
    bytes: Vec<u8>,
    oversized: bool,
}
impl EventLines {
    fn push(&mut self, chunk: &[u8]) -> Vec<Value> {
        self.bytes.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(end) = self.bytes.iter().position(|byte| *byte == b'\n') {
            let bytes = self.bytes.drain(..=end).collect::<Vec<_>>();
            if !self.oversized
                && end <= 65_536
                && let Ok(event) = serde_json::from_slice(&bytes)
            {
                events.push(event);
            }
            self.oversized = false;
        }
        if self.bytes.len() > 65_536 {
            self.bytes.clear();
            self.oversized = true;
        }
        events
    }
}
struct Sessions {
    host: String,
    cwd: PathBuf,
    parent: TaskId,
    flags: Vec<String>,
    custody: Arc<ProcessCustody>,
    children: BTreeMap<String, Running>,
    recovered: BTreeSet<String>,
    _ownership: FileLock,
    _root: acyclic_fs::HeldRoot,
}

impl Sessions {
    fn open(host: &str) -> Result<Self> {
        let flags: Vec<String> = serde_json::from_str(&env::var("ACYCLIC_HOST_ARGS").map_err(
            |_| "launch the native parent with acyclic host HOST -- POLICY_ARGS before spawning",
        )?)?;
        codex::validate(host, &flags)?;
        let cwd = PathBuf::from(
            env::var_os("ACYCLIC_WORKSPACE").ok_or("native workspace context absent")?,
        )
        .canonicalize()?;
        let parent = parse_task(&env::var("ACYCLIC_TASK")?)?;
        let root = acyclic_fs::hold_root(&host_files::workspace_root(&cwd)?)?.create_directories(
            &PathBuf::from(".acyclic-sdk/host-processes").join(parent.to_string()),
        )?;
        let ownership = FileLock::try_acquire(
            root.open_file(Path::new("owner.lock"), true)?,
            FileLockMode::Exclusive,
        )?;
        // Fence the preceding owner and all its processes before recovering files.
        let custody = ProcessCustody::acquire(root.path())?;
        Ok(Self {
            host: host.into(),
            cwd,
            parent,
            flags,
            custody,
            children: BTreeMap::new(),
            recovered: BTreeSet::new(),
            _ownership: ownership,
            _root: root,
        })
    }
    fn request(&self, reference: &str, action: SessionAction) -> Result<Value> {
        Ok(host_files::request(
            &self.cwd,
            HostAction::Session {
                parent: self.parent,
                reference: reference.into(),
                action,
            },
        )?)
    }
    fn start(&mut self, reference: &str) -> Result<()> {
        if self.children.contains_key(reference) {
            return Ok(());
        }
        self.recover(reference)?;
        if self.children.len() >= 16 {
            return Ok(());
        }
        let turn = self.request(reference, SessionAction::Next)?;
        if turn.is_null() {
            return Ok(());
        }
        let task: TaskId =
            serde_json::from_value(turn.get("task").cloned().ok_or("child task absent")?)?;
        let number = turn
            .get("turn")
            .and_then(Value::as_u64)
            .ok_or("child turn absent")?;
        let launched = (|| -> Result<_> {
            let cwd = PathBuf::from(
                turn.get("cwd")
                    .and_then(Value::as_str)
                    .ok_or("child cwd absent")?,
            );
            let resume = if self.host == "pi" {
                turn.pointer("/session/file")
                    .or_else(|| turn.pointer("/session/id"))
            } else {
                turn.pointer("/session/id")
            };
            let mut command = codex::child(
                &self.host,
                &self.flags,
                &cwd,
                resume.and_then(Value::as_str),
            )?;
            command
                .env("ACYCLIC_WORKSPACE", &cwd)
                .env("ACYCLIC_TASK", task.to_string());
            let mut tree = ProcessTree::spawn_in_custody(&mut command, &self.custody)?;
            let input = tree.take_stdin().ok_or("native stdin absent")?;
            let prompt = turn
                .get("prompt")
                .and_then(Value::as_str)
                .ok_or("native prompt absent")?
                .as_bytes()
                .to_vec();
            // Native startup may not read stdin yet. Keep cancellation responsive.
            let input = spawn_input(input, prompt)?;
            Ok((tree, input))
        })();
        match launched {
            Ok((tree, input)) => {
                self.children.insert(
                    reference.into(),
                    Running {
                        tree,
                        input: Some(input),
                        turn: number,
                        events: EventLines::default(),
                        answer: String::new(),
                        session: None,
                    },
                );
            }
            Err(error) => {
                self.request(
                    reference,
                    SessionAction::Complete {
                        turn: number,
                        outcome: acyclic_harness::Outcome::Failed {
                            message: error.to_string(),
                        },
                    },
                )?;
            }
        }
        Ok(())
    }
    fn recover(&mut self, reference: &str) -> Result<()> {
        if !self.recovered.contains(reference) && !self.children.contains_key(reference) {
            self.request(reference, SessionAction::Recover)?;
            self.recovered.insert(reference.into());
        }
        Ok(())
    }
    fn output(&self, reference: &str, running: &mut Running) -> Result<bool> {
        let mut chunk = [0u8; 8192];
        // Bound each pass so a busy child cannot starve cancellation or peers.
        for _ in 0..16 {
            let Some(count) = running.tree.read_stdout(&mut chunk)? else {
                break;
            };
            if count == 0 {
                return Ok(true);
            }
            for event in running
                .events
                .push(chunk.get(..count).ok_or("native output count invalid")?)
            {
                let id = event
                    .get("thread_id")
                    .or_else(|| event.get("session_id"))
                    .or_else(|| {
                        (event.get("type").and_then(Value::as_str) == Some("session"))
                            .then(|| event.get("id"))
                            .flatten()
                    });
                if let Some(id) = id.and_then(Value::as_str)
                    && running.session.as_deref() != Some(id)
                {
                    self.request(reference, SessionAction::SaveSession { id: id.into() })?;
                    running.session = Some(id.into());
                }
                let text = if event.pointer("/item/type").and_then(Value::as_str)
                    == Some("agent_message")
                {
                    event.pointer("/item/text")
                } else {
                    event.get("result")
                };
                if let Some(text) = text.and_then(Value::as_str) {
                    running.answer = bounded_text(text);
                }
                if event.pointer("/message/role").and_then(Value::as_str) == Some("assistant")
                    && event.get("type").and_then(Value::as_str) == Some("message_end")
                    && let Some(parts) = event.pointer("/message/content").and_then(Value::as_array)
                {
                    running.answer = bounded_text(
                        &parts
                            .iter()
                            .filter_map(|part| part.get("text").and_then(Value::as_str))
                            .collect::<String>(),
                    );
                }
            }
        }
        Ok(false)
    }
    fn refresh(&mut self) -> Result<()> {
        let references: Vec<String> = serde_json::from_value(host_files::request(
            &self.cwd,
            HostAction::Sessions {
                parent: self.parent,
            },
        )?)?;
        for reference in references {
            self.recover(&reference)?;
            let observed = self.request(&reference, SessionAction::Observe)?;
            if observed
                .get("control")
                .and_then(Value::as_str)
                .is_some_and(|state| state != "resume")
            {
                if let Some(mut running) = self.children.remove(&reference) {
                    running.tree.terminate_descendants()?;
                    let _ = self.output(&reference, &mut running);
                    running.tree.terminate(None)?;
                    let _ = running.finish_input();
                    self.request(
                        &reference,
                        SessionAction::Complete {
                            turn: running.turn,
                            outcome: acyclic_harness::Outcome::Cancelled,
                        },
                    )?;
                }
                continue;
            }
            let Some(mut running) = self.children.remove(&reference) else {
                self.start(&reference)?;
                continue;
            };
            let mut exited = running
                .check_input()
                .and_then(|()| self.output(&reference, &mut running))
                .and_then(|_| running.tree.try_wait(None).map_err(Into::into));
            if matches!(&exited, Ok(Some(_))) {
                running.tree.terminate_descendants()?;
                match self.output(&reference, &mut running) {
                    Ok(true) => {}
                    Ok(false) => {
                        self.children.insert(reference, running);
                        continue;
                    }
                    Err(error) => exited = Err(error),
                }
            }
            let outcome = match exited {
                Err(error) => Some(acyclic_harness::Outcome::Failed {
                    message: error.to_string(),
                }),
                Ok(Some(status)) if status.success() => Some(acyclic_harness::Outcome::Succeeded(
                    acyclic_harness::executor::TurnOutput {
                        text: running.answer.clone(),
                        metadata: Value::Null,
                        steps: 1,
                        attachments: Vec::new(),
                    },
                )),
                Ok(Some(status)) => Some(acyclic_harness::Outcome::Failed {
                    message: format!("native host exited {status}"),
                }),
                Ok(None) => None,
            };
            if let Some(outcome) = outcome {
                running.tree.terminate(None)?;
                let outcome = match running.finish_input() {
                    Ok(()) => outcome,
                    Err(error) => acyclic_harness::Outcome::Failed {
                        message: error.to_string(),
                    },
                };
                self.request(
                    &reference,
                    SessionAction::Complete {
                        turn: running.turn,
                        outcome,
                    },
                )?;
            } else {
                self.children.insert(reference.clone(), running);
            }
            self.start(&reference)?;
        }
        Ok(())
    }
    fn stop(&mut self, reference: &str, control: Control) -> Result<Value> {
        self.request(reference, SessionAction::Control { control })?;
        if let Some(mut running) = self.children.remove(reference) {
            running.tree.terminate_descendants()?;
            let _ = self.output(reference, &mut running);
            running.tree.terminate(None)?;
            let _ = running.finish_input();
            self.request(
                reference,
                SessionAction::Complete {
                    turn: running.turn,
                    outcome: acyclic_harness::Outcome::Cancelled,
                },
            )?;
        } else {
            self.recover(reference)?;
        }
        self.request(reference, SessionAction::Observe)
    }
    fn call(&mut self, name: &str, args: &Value) -> Result<Value> {
        let object = args.as_object().ok_or("tool arguments must be an object")?;
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "reference" | "prompt"))
        {
            return Err("unsupported coordination argument".into());
        }
        let reference = object
            .get("reference")
            .and_then(Value::as_str)
            .ok_or("reference is required")?;
        let prompt = || {
            object
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or("prompt is required")
        };
        match name {
            "spawn" => {
                self.request(reference, SessionAction::Spawn { prompt: prompt()? })?;
                self.start(reference)?;
            }
            "send" => {
                self.request(reference, SessionAction::Send { prompt: prompt()? })?;
                self.start(reference)?;
            }
            "resume" => {
                if self.children.contains_key(reference) {
                    return Err("native session is still running".into());
                }
                self.recover(reference)?;
                self.request(
                    reference,
                    SessionAction::Control {
                        control: Control::Resume,
                    },
                )?;
                self.request(reference, SessionAction::Send { prompt: prompt()? })?;
                self.start(reference)?;
            }
            "archive" => return self.stop(reference, Control::Archive),
            "cancel" => return self.stop(reference, Control::Interrupt),
            "wait" => {}
            _ => return Err("unknown coordination tool".into()),
        }
        self.request(reference, SessionAction::Observe)
    }
}

fn bounded_text(text: &str) -> String {
    text.chars()
        .scan(0, |bytes, ch| {
            *bytes += ch.len_utf8();
            (*bytes <= 4096).then_some(ch)
        })
        .collect()
}
fn settled(value: &Value) -> bool {
    let stopped = value
        .get("control")
        .and_then(Value::as_str)
        .is_some_and(|state| state != "resume");
    value
        .pointer("/status/settled")
        .and_then(Value::as_bool)
        .unwrap_or(stopped)
        && (stopped || value.get("pending").and_then(Value::as_u64) == Some(0))
}
fn reply(id: &Value, result: Result<Value>) -> Result<()> {
    let result = match result {
        Ok(value) => json!({"content":[{"type":"text","text":value.to_string()}]}),
        Err(error) => json!({"isError":true,"content":[{"type":"text","text":error.to_string()}]}),
    };
    emit(&json!({"jsonrpc":"2.0","id":id,"result":result}))
}
fn emit(value: &Value) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn input_messages() -> mpsc::Receiver<io::Result<Value>> {
    let (send, receive) = mpsc::sync_channel(16);
    std::thread::spawn(move || {
        let input = io::stdin();
        for message in acyclic_harness::mcp::stdio::JsonLineReader::new(input.lock(), 65_536) {
            if send.send(message).is_err() {
                break;
            }
        }
    });
    receive
}

pub(super) fn serve(host: &str) -> Result<()> {
    let receive = input_messages();
    let mut sessions: Option<Sessions> = None;
    let mut waiting: Vec<(Value, String)> = Vec::new();
    let mut next_refresh = Instant::now();
    let serving = (|| -> Result<()> {
        loop {
            if Instant::now() >= next_refresh {
                if let Some(state) = sessions.as_mut() {
                    state.refresh()?;
                    let mut remaining = Vec::new();
                    for (id, reference) in waiting.drain(..) {
                        match state.request(&reference, SessionAction::Observe) {
                            Ok(value) if !settled(&value) => remaining.push((id, reference)),
                            result => reply(&id, result)?,
                        }
                    }
                    waiting = remaining;
                }
                next_refresh = Instant::now() + Duration::from_millis(100);
            }
            let message = match receive.recv_timeout(Duration::from_millis(20)) {
                Ok(Ok(value)) => value,
                Ok(Err(_)) => {
                    emit(
                        &json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"invalid JSON"}}),
                    )?;
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(_) => break,
            };
            let params = message.get("params").unwrap_or(&Value::Null);
            if message.get("method").and_then(Value::as_str) == Some("notifications/cancelled") {
                waiting.retain(|(id, _)| Some(id) != params.get("requestId"));
                continue;
            }
            let Some(id) = message.get("id") else {
                continue;
            };
            match message.get("method").and_then(Value::as_str) {
                Some("initialize") => emit(
                    &json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":params.get("protocolVersion").and_then(Value::as_str).unwrap_or("2025-06-18"),"capabilities":{"tools":{}},"serverInfo":{"name":"acyclic-sessions","version":env!("CARGO_PKG_VERSION")}}}),
                )?,
                Some("ping") => emit(&json!({"jsonrpc":"2.0","id":id,"result":{}}))?,
                Some("tools/list") => emit(&json!({"jsonrpc":"2.0","id":id,"result":catalog()}))?,
                Some("tools/call") => {
                    let name = params
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let args = params.get("arguments").unwrap_or(&Value::Null);
                    let result = (|| -> Result<Value> {
                        if sessions.is_none() {
                            sessions = Some(Sessions::open(host)?);
                        }
                        sessions
                            .as_mut()
                            .ok_or("session owner absent")?
                            .call(name, args)
                    })();
                    if name == "wait" && result.as_ref().is_ok_and(|value| !settled(value)) {
                        if waiting.len() >= 16 {
                            reply(id, Err("native wait concurrency bound reached".into()))?;
                            continue;
                        }
                        waiting.push((
                            id.clone(),
                            args.get("reference")
                                .and_then(Value::as_str)
                                .ok_or("reference absent")?
                                .into(),
                        ));
                    } else {
                        reply(id, result)?;
                    }
                }
                _ => emit(
                    &json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}),
                )?,
            }
        }
        Ok(())
    })();
    if let Some(mut sessions) = sessions {
        let mut failure = None;
        for reference in sessions.children.keys().cloned().collect::<Vec<_>>() {
            if let Err(error) = sessions.stop(&reference, Control::Interrupt) {
                failure = Some(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
    }
    serving
}

fn catalog() -> Value {
    json!({"tools":(["spawn","send","wait","archive","cancel","resume"].map(|name| {
        let prompt=matches!(name,"spawn"|"send"|"resume");
        json!({"name":name,"description":format!("{name} a separate native session in an admitted harness view. Native tools and policy are preserved. Native subagents are unmanaged."),"inputSchema":{"type":"object","properties":{"reference":{"type":"string"},"prompt":{"type":"string"}},"required":if prompt {vec!["reference","prompt"]} else {vec!["reference"]},"additionalProperties":false}})
    }))})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_stdin_never_runs_on_the_supervisor_thread() -> io::Result<()> {
        struct WorkerOnly(std::thread::ThreadId, mpsc::Sender<usize>);
        impl io::Write for WorkerOnly {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if std::thread::current().id() == self.0 {
                    return Err(io::Error::other(
                        "native stdin ran on the supervisor thread",
                    ));
                }
                self.1.send(bytes.len()).map_err(io::Error::other)?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (written, received) = mpsc::channel();
        spawn_input(
            WorkerOnly(std::thread::current().id(), written),
            vec![b'x'; 32_768],
        )?
        .join()
        .map_err(|_| io::Error::other("native input writer panicked"))??;
        assert_eq!(received.try_recv().map_err(io::Error::other)?, 32_768);
        Ok(())
    }
    #[test]
    fn oversized_native_tool_output_does_not_kill_or_wedge_the_host() {
        let mut events = EventLines::default();
        for _ in 0..16 {
            assert!(events.push(&[b'x'; 8192]).is_empty());
            assert!(events.bytes.len() <= 65_536);
        }
        assert_eq!(
            events.push(b"\n{\"type\":\"thread.started\",\"thread_id\":\"native\"}\n"),
            vec![json!({"type":"thread.started","thread_id":"native"})]
        );
    }
    #[test]
    fn waits_for_quiescence_and_all_accepted_inputs_without_a_tool_ledger() {
        assert!(!settled(&json!({"status":{"settled":false},"pending":0})));
        assert!(!settled(&json!({"status":{"settled":true},"pending":1})));
        assert!(settled(&json!({"status":{"settled":true},"pending":0})));
        assert!(settled(
            &json!({"status":{"settled":true},"pending":1,"control":"interrupt"})
        ));
        assert!(bounded_text(&"😀".repeat(4096)).len() <= 4096);
    }
}
