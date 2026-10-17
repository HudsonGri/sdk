//! Real argv/provider/LocalHarness/client qualification over a loopback endpoint.
mod instruction_review;
use acyclic_harness::{
    OperationId,
    client::{jsonl, prepare_local_prompt},
    task_volume::{Change, FileKey, TaskFile},
};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    io::Write as _,
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const ANSWER: &str = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture answer\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":64,\"output_tokens\":2}}}\n\n";

#[derive(Clone, Copy)]
enum Mode {
    Prompt,
    Instructions,
    FsRefusals,
    Jsonl,
    OpenJsonl,
    Dumb,
}

async fn execute(
    workspace: &Path,
    catalog: &Path,
    settings: &Path,
    operation: OperationId,
    mode: Mode,
) -> Vec<u8> {
    let jsonl = !matches!(mode, Mode::Prompt | Mode::Instructions | Mode::FsRefusals);
    let keep_open = matches!(mode, Mode::OpenJsonl);
    let mut input = tempfile::NamedTempFile::new().unwrap();
    let request = if jsonl {
        format!(
            "{}\n",
            jsonl::request(&prepare_local_prompt(operation, "hello").unwrap()).unwrap()
        )
        .into_bytes()
    } else {
        Vec::new()
    };
    input.write_all(&request).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_graphcoder"));
    command
        .env("CODEX_HOME", settings)
        .env("LOCALAPPDATA", settings)
        .env("XDG_DATA_HOME", settings)
        .env_remove("OPENAI_API_KEY")
        .current_dir(workspace)
        .arg("--workspace")
        .arg(workspace)
        .arg("--catalog")
        .arg(catalog)
        .arg("--settings-root")
        .arg(settings)
        .args([
            "--model",
            "graphcoder-qual/tiny",
            "--operation",
            &operation.to_string(),
        ])
        .stdin(if keep_open {
            Stdio::piped()
        } else {
            Stdio::from(input.reopen().unwrap())
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if matches!(mode, Mode::Dumb) {
        command.env("TERM", "dumb");
    } else if jsonl {
        command.args(["--input-format", "jsonl"]);
    } else {
        command.args(["-p", "hello"]);
    }
    // Readiness is the process's own exit, never a wall-clock budget: every
    // mode exits once its turn settles (open JSONL with stdin still held), and
    // a hang is the test runner's to terminate.
    let output = tokio::task::spawn_blocking(move || {
        let mut child = command.spawn().unwrap();
        let held = keep_open.then(|| {
            let mut stdin = child.stdin.take().unwrap();
            stdin.write_all(&request).unwrap();
            stdin
        });
        let output = child.wait_with_output().unwrap();
        drop(held);
        output
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "exit {:?}\n{}\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn catalog(directory: &Path, address: std::net::SocketAddr) -> std::path::PathBuf {
    let catalog = directory.join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    catalog
}

async fn scenario(mode: Mode) {
    let instructions = matches!(mode, Mode::Instructions);
    let fs_refusals = matches!(mode, Mode::FsRefusals);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let counter = requests.clone();
    let app = Router::new()
        .route(
            "/count",
            post(move || {
                let requests = counter.clone();
                async move {
                    requests.send("count").unwrap();
                    Json(json!({"input_tokens":64}))
                }
            }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let requests = requests.clone();
                async move {
                    assert!(body.to_string().contains("hello"));
                    if instructions {
                        assert_eq!(body["input"], json!([
                            {"type":"message","role":"system","content":[{"type":"input_text","text":"Keep the current goal in GOAL.md in the agent private volume; use active, blocked or done frontmatter status."}]},
                            {"type":"message","role":"system","content":[{"type":"input_text","text":"Keep each Markdown plan in plans/<id>.md beside GOAL.md in the agent private volume; use ordinary file tools and - [ ], - [~], - [x] task lists."}]},
                            {"type":"message","role":"system","content":[{"type":"input_text","text":"Volumes: . (rw), .volumes/scratch (rw)"}]},
                            {"type":"message","role":"user","content":[{"type":"input_text","text":"# AGENTS.md instructions for .\n\n<INSTRUCTIONS>\nReply using the workspace instructions. λ\n\n</INSTRUCTIONS>"}]},
                            {"type":"message","role":"user","content":[{"type":"input_text","text":"# CLAUDE.md instructions for .\n\n<INSTRUCTIONS>\nProject marker: claude-context λ\n</INSTRUCTIONS>"}]},
                            {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}
                        ]));
                        assert_eq!(body["store"], false);
                        assert_eq!(body["stream"], true);
                        assert!(body.get("instructions").is_none());
                    }
                    requests.send("model").unwrap();
                    let answer = if fs_refusals { fs_answer(&body) } else { ANSWER.to_owned() };
                    ([("content-type", "text/event-stream")], answer)
                }
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&settings).unwrap();
    if instructions {
        std::fs::write(
            workspace.join("AGENTS.md"),
            "Reply using the workspace instructions. λ\n",
        )
        .unwrap();
        std::fs::write(
            workspace.join("CLAUDE.md"),
            "Project marker: claude-context λ",
        )
        .unwrap();
    }
    let catalog = catalog(directory.path(), address);
    let operation = OperationId::new();
    let output = execute(&workspace, &catalog, &settings, operation, mode).await;
    let mut lines = std::str::from_utf8(&output).unwrap().lines();
    let header: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(header[0]["TaskSession"]["session"], operation.to_string());
    assert_eq!(header[0]["TaskSession"]["task"], operation.to_string());
    let mut answers = Vec::new();
    for line in lines {
        let change: Change = jsonl::parse_change(line).unwrap();
        for file in change.file.into_iter().chain(change.files) {
            if fs_refusals {
                assert!(
                    !matches!(file.key, FileKey::Child { .. }),
                    "refusal created a child file"
                );
            }
            if matches!(
                file.key,
                FileKey::Session {
                    file: TaskFile::ExecutionResult(_),
                    ..
                }
            ) {
                answers.push(serde_json::from_slice::<Value>(&file.bytes).unwrap());
            }
        }
    }
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0]["ok"]["text"], "fixture answer");
    assert_eq!(answers[0]["completion"], "completed");
    // A second real process restores the same request/result instead of generating again.
    execute(&workspace, &catalog, &settings, operation, mode).await;
    let mut generated = 0;
    let mut counted = 0;
    while let Ok(kind) = observed.try_recv() {
        match kind {
            "model" => generated += 1,
            "count" => counted += 1,
            _ => unreachable!(),
        }
    }
    assert_eq!(generated, if fs_refusals { 2 } else { 1 });
    assert!(counted > 0);
    stop.send(()).unwrap();
    server.await.unwrap();
}

// The provider sees the refusal through real HTTP, then finishes the same turn.
// Exact result identities/counts prevent a skipped invocation from passing.
fn fs_answer(body: &Value) -> String {
    const COMMANDS: [&str; 8] = [
        "ls .volumes/agents-extra",
        "ls .volumes/agents/",
        "cat .volumes/agents/worker/status.json",
        "ls .volumes/agents/../other",
        "ls .volumes/agents | cat",
        "ls .volumes/agents --volume ''",
        "ls .volumes/agents 'open",
        " \t\n",
    ];
    let results: Vec<_> = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .collect();
    if !results.is_empty() {
        assert_eq!(results.len(), COMMANDS.len());
        for (index, result) in results.iter().enumerate() {
            assert_eq!(result["call_id"], format!("refusal_{index}"));
            let output: Value = serde_json::from_str(result["output"].as_str().unwrap()).unwrap();
            assert!(
                output["error"]
                    .as_str()
                    .is_some_and(|error| !error.is_empty()),
                "{output}"
            );
        }
        return ANSWER.to_owned();
    }
    let mut answer = String::new();
    for (index, command) in COMMANDS.iter().enumerate() {
        let event = json!({"type":"response.output_item.done", "output_index":index,
            "item":{"type":"function_call","call_id":format!("refusal_{index}"),
                "name":"fs","arguments":json!({"command":command}).to_string()}});
        answer.push_str(&format!("data: {event}\n\n"));
    }
    answer.push_str(
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
    );
    answer
}

#[tokio::test]
async fn fs_inspection_refusals_reach_http_and_complete_the_shipping_binary() {
    scenario(Mode::FsRefusals).await;
}

#[tokio::test]
async fn prompt_argv_uses_the_real_registry_and_file_client() {
    scenario(Mode::Prompt).await;
}

#[tokio::test]
async fn fix_live_instructions_reach_the_loopback_responses_request_and_survive_reopen() {
    scenario(Mode::Instructions).await;
}

#[tokio::test]
async fn jsonl_stdin_eof_preserves_the_real_client_operation() {
    scenario(Mode::Jsonl).await;
}

#[tokio::test]
async fn settled_jsonl_exits_while_the_input_pipe_stays_open() {
    scenario(Mode::OpenJsonl).await;
}

#[tokio::test]
async fn dumb_terminal_uses_the_shared_headless_runner_without_tty() {
    scenario(Mode::Dumb).await;
}

// A process killed during a tool effect leaves the workspace usable by the next prompt.
const EFFECT: &str = "effect.txt";

fn tool_call() -> String {
    let call = json!({"type":"response.output_item.done","output_index":0,"item":{
        "type":"function_call","call_id":"interrupted","name":"bash",
        "arguments":json!({"command":format!("printf x >> {EFFECT}; exec sleep 30")}).to_string()
    }});
    format!(
        "data: {call}\n\ndata: {}\n\n",
        json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":64,"output_tokens":2}}})
    )
}

fn graphcoder(
    workspace: &Path,
    catalog: &Path,
    settings: &Path,
    operation: OperationId,
    prompt: &str,
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_graphcoder"));
    command
        .current_dir(workspace)
        .arg("--workspace")
        .arg(workspace)
        .arg("--catalog")
        .arg(catalog)
        .arg("--settings-root")
        .arg(settings)
        .args(["--model", "graphcoder-qual/tiny", "--operation"])
        .arg(operation.to_string())
        .args(["-p", prompt])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// Runs GraphCoder to its own exit. With `kill_when`, the process (and the
/// tool it spawned) is killed once that file exists, as a crash would end it.
async fn run(
    mut command: Command,
    kill_when: Option<std::path::PathBuf>,
) -> Result<std::process::Output, String> {
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    tokio::task::spawn_blocking(move || {
        let mut child = command.spawn().unwrap();
        if let Some(effect) = kill_when {
            while child.try_wait().unwrap().is_none() {
                if effect.exists() {
                    #[cfg(unix)]
                    Command::new("kill")
                        .args(["-KILL", "--", &format!("-{}", child.id())])
                        .status()
                        .unwrap();
                    #[cfg(not(unix))]
                    child.kill().unwrap();
                    let output = child.wait_with_output().unwrap();
                    return Err(format!(
                        "killed\nstdout: {}\nstderr: {}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ));
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        Ok(child.wait_with_output().unwrap())
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn killed_mid_tool_process_does_not_brick_the_next_prompt_or_a_live_one() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let generated = Arc::new(AtomicUsize::new(0));
    let responses = Arc::clone(&generated);
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move || {
                let first = responses.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    (
                        [("content-type", "text/event-stream")],
                        if first { tool_call() } else { ANSWER.into() },
                    )
                }
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&settings).unwrap();
    let catalog = catalog(directory.path(), address);
    let effect = workspace.join(EFFECT);
    let release = directory.path().join("release");
    let first = tokio::spawn(run(
        graphcoder(&workspace, &catalog, &settings, OperationId::new(), "start"),
        Some(release.clone()),
    ));
    while !effect.exists() {
        if first.is_finished() {
            panic!("first process ended before its effect: {:?}", first.await);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A second process never treats the first one's live turn as orphaned:
    // the workspace stream admits one owning process at a time.
    let contender = run(
        graphcoder(&workspace, &catalog, &settings, OperationId::new(), "contend"),
        None,
    )
    .await
    .unwrap();
    assert!(
        !contender.status.success() && !contender.stderr.is_empty(),
        "contender opened a live workspace: {}",
        String::from_utf8_lossy(&contender.stdout)
    );
    assert!(!first.is_finished(), "the live turn was disturbed");
    assert_eq!(generated.load(Ordering::SeqCst), 1);
    std::fs::write(&release, b"").unwrap();
    let killed = first
        .await
        .unwrap()
        .expect_err("the first process must be killed during its tool effect");
    assert!(killed.starts_with("killed"), "{killed}");
    // The next prompt settles the killed turn instead of rejecting admission.
    let output = run(
        graphcoder(&workspace, &catalog, &settings, OperationId::new(), "next"),
        None,
    )
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "exit {:?}\n{}\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let answers: Vec<Value> = std::str::from_utf8(&output.stdout)
        .unwrap()
        .lines()
        .skip(1)
        .flat_map(|line| {
            let change: Change = jsonl::parse_change(line).unwrap();
            change.file.into_iter().chain(change.files)
        })
        .filter(|file| {
            matches!(
                file.key,
                FileKey::Session {
                    file: TaskFile::ExecutionResult(_),
                    ..
                }
            )
        })
        .map(|file| serde_json::from_slice(&file.bytes).unwrap())
        .collect();
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0]["ok"]["text"], "fixture answer");
    // The killed effect ran once and its model call was never reissued.
    assert_eq!(std::fs::read_to_string(&effect).unwrap(), "x");
    assert_eq!(generated.load(Ordering::SeqCst), 2);
    stop.send(()).unwrap();
    server.await.unwrap();
}
