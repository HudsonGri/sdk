//! Progress must escape the process while the provider's terminal SSE is withheld.
use acyclic_harness::task_volume::{Change, Channel, FileKey, TaskFile};
use acyclic_native_runtime::{ProcessInput, ProcessOutput, ProcessTree};
use axum::{Json, Router, body::Body, response::Response, routing::post};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Notify;

const DELTAS: &str = concat!(
    "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"thinking\"}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"working\"}\n\n",
);
const COMPLETED: &str = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":64,\"output_tokens\":2}}}\n\n";

async fn scenario(acknowledge_chunks: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let release = Arc::new(Notify::new());
    let provider_release = release.clone();
    let (records, mut recorded) = tokio::sync::mpsc::unbounded_channel();
    let (progress, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let release = provider_release.clone();
                let progress = progress.clone();
                records.send(body).unwrap();
                async move {
                    let initial = futures::stream::once(async { Ok::<_, Infallible>(DELTAS) });
                    let terminal = futures::stream::once(async move {
                        let visible =
                            tokio::time::timeout(Duration::from_secs(1), release.notified())
                                .await
                                .is_ok();
                        progress.send(visible).unwrap();
                        Ok::<_, Infallible>(COMPLETED)
                    });
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from_stream(initial.chain(terminal)))
                        .unwrap()
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
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"loopback":{
        "adapter":"responses", "authentication":"optional", "base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},
        "max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    let output = tokio::task::spawn_blocking(move || {
        acyclic_native_runtime::initialize_native_io_services().unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_graphcoder"));
        command
            .arg("--workspace")
            .arg(&workspace)
            .arg("--settings-root")
            .arg(&settings)
            .arg("--catalog")
            .arg(&catalog)
            .args(["--model", "loopback/tiny", "-p", "hello"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut pending = Vec::new();
        let mut seen = (false, false);
        let output = ProcessTree::spawn(&mut command)
            .unwrap()
            .wait_with_output(
                Duration::from_secs(6),
                65_536,
                None,
                || Ok(()),
                |chunk| {
                    if let ProcessOutput::Stdout(bytes) = chunk {
                        pending.extend_from_slice(bytes);
                        while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                            let line: Vec<_> = pending.drain(..=end).collect();
                            if let Ok(change) = serde_json::from_slice::<Change>(&line) {
                                for file in change.file.iter().chain(&change.files) {
                                    if matches!(
                                        file.key,
                                        FileKey::Task {
                                            file: TaskFile::Chunk { .. },
                                            ..
                                        }
                                    ) {
                                        match file.channel {
                                            Some(Channel::Text) => {
                                                assert_eq!(file.bytes.as_ref(), b"working");
                                                seen.0 = true;
                                            }
                                            Some(Channel::Reasoning) => {
                                                assert_eq!(file.bytes.as_ref(), b"thinking");
                                                seen.1 = true;
                                            }
                                            _ => panic!("unexpected chunk channel"),
                                        }
                                    }
                                }
                            }
                        }
                        if acknowledge_chunks && seen == (true, true) {
                            release.notify_one();
                        }
                    }
                    Ok(ProcessInput::default())
                },
                None,
                None,
            )
            .unwrap_or_else(|failure| {
                panic!(
                    "{}: {}",
                    failure.error,
                    String::from_utf8_lossy(&failure.stderr)
                )
            });
        assert_eq!(seen, (true, true));
        output
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        observed.recv().await.unwrap(),
        acknowledge_chunks,
        "terminal SSE must wait until both chunk files have escaped stdout"
    );
    let request = recorded.recv().await.unwrap();
    assert_eq!(request["model"], "tiny");
    assert_eq!(request["store"], false);
    assert!(
        recorded.try_recv().is_err(),
        "progress cannot resend generation"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn text_and_reasoning_are_visible_before_the_provider_completes() {
    scenario(true).await;
}

#[tokio::test]
async fn negative_control_final_output_cannot_acknowledge_live_chunks() {
    scenario(false).await;
}
