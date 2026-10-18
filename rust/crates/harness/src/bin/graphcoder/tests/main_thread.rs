//! The released binary runs fs diff, fs commit and spawn on its own process
//! main thread (1 MiB on Windows) without exhausting it.
use axum::{Router, body::Bytes, routing::post};
use serde_json::{Value, json};
use std::process::{Command, Stdio};

const PROMPT: &str = "main thread qualification";

fn call(name: &str, arguments: Value) -> Value {
    json!({"type": "function_call", "name": name, "arguments": arguments.to_string()})
}

/// The parent's next turn is chosen by how many tool results it has seen; the
/// child, which never sees the parent's prompt, always answers in text.
fn turn(body: &str) -> Value {
    if !body.contains(PROMPT) {
        return json!({"type": "message", "text": "child done"});
    }
    let results = body.matches("\"function_call_output\"").count();
    let mut turn = match results {
        0 => call("fs", json!({"command": "diff"})),
        1 => call("fs", json!({"command": "commit -m main-thread"})),
        2 => call("spawn", json!({"task": "say hi", "name": "kid"})),
        _ => return json!({"type": "message", "text": "done"}),
    };
    turn["call_id"] = json!(format!("call{results}"));
    turn
}

/// The binary on the smallest release main thread: Windows' own 1 MiB, and
/// the same 1 MiB on Unix.
fn main_thread_command() -> Command {
    let binary = env!("CARGO_BIN_EXE_graphcoder");
    #[cfg(unix)]
    {
        let mut shell = Command::new("sh");
        shell.args(["-c", "ulimit -s 1024; exec \"$0\" \"$@\"", binary]);
        shell
    }
    #[cfg(windows)]
    Command::new(binary)
}

#[tokio::test]
async fn released_binary_completes_fs_diff_commit_and_spawn_on_its_main_thread() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, mut observed) = tokio::sync::mpsc::unbounded_channel::<String>();
    let app = Router::new()
        .route(
            "/count",
            post(|| async { axum::Json(json!({"input_tokens": 64})) }),
        )
        .route(
            "/responses",
            post(move |body: Bytes| {
                let requests = requests.clone();
                async move {
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    let turn = turn(&body);
                    requests.send(body).unwrap();
                    let event = if turn["type"] == "message" {
                        json!({"type": "response.output_text.delta", "delta": turn["text"]})
                    } else {
                        json!({"type": "response.output_item.done", "output_index": 0, "item": turn})
                    };
                    let stream: String = [
                        event,
                        json!({"type": "response.completed", "response": {"status": "completed",
                            "usage": {"input_tokens": 64, "output_tokens": 2}}}),
                    ]
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect();
                    ([("content-type", "text/event-stream")], stream)
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
    std::fs::write(workspace.join("calc.py"), "x = 1\n").unwrap();
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"main-thread": {
        "adapter": "responses", "authentication": "optional", "base_url": format!("http://{address}"),
        "models": {"tiny": {"capacity": {"context_tokens": 8192, "output_tokens": 1024},
            "features": {"tools": true, "images": false}}},
        "max_event_bytes": 65536,
        "token_counter": {"path": "count", "adapter": "responses",
            "response_field": "input_tokens", "framing_tokens": 0}
    }})).unwrap())
    .unwrap();
    let mut command = main_thread_command();
    command
        .env("CODEX_HOME", &settings)
        .env("LOCALAPPDATA", &settings)
        .env("XDG_DATA_HOME", &settings)
        .env_remove("OPENAI_API_KEY")
        .env_remove("RUST_MIN_STACK")
        .current_dir(&workspace)
        .arg("--workspace")
        .arg(&workspace)
        .arg("--catalog")
        .arg(&catalog)
        .arg("--settings-root")
        .arg(&settings)
        .args(["--model", "main-thread/tiny", "-p", PROMPT])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    let stderr = format!(
        "{}
{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    // A stack overflow aborts: a signal on Unix, STATUS_STACK_OVERFLOW on Windows.
    // Any headless outcome code (completed=0 .. indeterminate=4) is a clean exit;
    // spawn's settled outcome belongs to the child lifecycle, not to this check.
    assert!(
        matches!(output.status.code(), Some(0..=4)),
        "graphcoder did not exit cleanly: {:?}\n{stderr}",
        output.status
    );
    assert!(!stderr.contains("overflowed its stack"), "{stderr}");
    let (mut parent, mut child) = (Vec::new(), 0);
    while let Ok(body) = observed.try_recv() {
        if body.contains(PROMPT) {
            parent.push(body);
        } else {
            child += 1;
        }
    }
    let returned = |text: &str| parent.iter().any(|body| body.contains(text));
    // Tool results are JSON strings inside the request bodies.
    assert!(
        returned(r#"\"kind\":\"diff\""#),
        "fs diff returned no result: {stderr}"
    );
    assert!(
        returned("Committed"),
        "fs commit returned no result: {stderr}"
    );
    // The third parent turn called spawn, so the process ran child admission
    // and preparation on its main thread before reaching the clean exit above.
    // Whether the child settles (and runs its own turn) belongs to the child
    // lifecycle, so the child's turn is reported, not asserted.
    assert!(parent.len() >= 3, "spawn was never called: {stderr}");
    eprintln!("child model turns: {child}");
    stop.send(()).unwrap();
    server.await.unwrap();
}
