//! F24: `graphcoder -p` spawns a child that runs its own model turn while the
//! parent publishes beside it, and the parent's wait receives the child's
//! answer. Parent and child publications overlap on every run: the child's
//! admission and turn commit while the parent's spawn and wait calls stage
//! their own files, so a candidate that recorded its staging prefixes as file
//! records fails its rebase and the turn settles indeterminate.
use acyclic_harness::{
    OperationId,
    client::jsonl,
    task_volume::{Change, FileKey, TaskFile},
};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};

const CHILD_TASK: &str = "Answer with child finished";

fn events(events: &[Value]) -> String {
    let mut body = String::new();
    for event in events {
        body.push_str("data: ");
        body.push_str(&event.to_string());
        body.push_str("\n\n");
    }
    body
}

fn call(id: &str, name: &str, arguments: &Value) -> String {
    events(&[
        json!({"type":"response.output_item.done","output_index":0,"item":{
            "type":"function_call","call_id":id,"name":name,"arguments":arguments.to_string()}}),
        json!({"type":"response.completed","response":{"status":"completed",
            "usage":{"input_tokens":64,"output_tokens":2}}}),
    ])
}

fn text(answer: &str) -> String {
    events(&[
        json!({"type":"response.output_text.delta","delta":answer}),
        json!({"type":"response.completed","response":{"status":"completed",
            "usage":{"input_tokens":64,"output_tokens":2}}}),
    ])
}

/// Whether a user message in the request carries `needle`.
fn user_says(body: &Value, needle: &str) -> bool {
    body["input"].as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item["role"] == "user"
                && item["content"].as_array().is_some_and(|parts| {
                    parts.iter().any(|part| {
                        part["text"]
                            .as_str()
                            .is_some_and(|text| text.contains(needle))
                    })
                })
        })
    })
}

/// Tool outputs the parent has already received, in order.
fn outputs(body: &Value) -> Vec<String> {
    body["input"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item["type"] == "function_call_output")
        .map(|item| item["output"].to_string())
        .collect()
}

/// The loopback model: the child answers its task; the parent spawns, waits,
/// then answers with the wait output it saw.
fn respond(body: &Value) -> (&'static str, String) {
    if user_says(body, CHILD_TASK) {
        return ("child", text("child finished"));
    }
    let outputs = outputs(body);
    match outputs.len() {
        0 => (
            "parent",
            call(
                "spawn",
                "spawn",
                &json!({"task":CHILD_TASK,"name":"worker"}),
            ),
        ),
        1 => (
            "parent",
            call("wait", "wait", &json!({"agents":["worker"]})),
        ),
        _ => {
            let waited = outputs.last().map_or("", String::as_str);
            let answer = if waited.contains("child finished") {
                "parent saw child finished"
            } else {
                "parent missed the child"
            };
            ("parent", text(answer))
        }
    }
}

#[tokio::test]
async fn prompt_spawns_a_child_turn_and_waits_for_its_answer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let requests = requests.clone();
                async move {
                    let (model, answer) = respond(&body);
                    requests.send(model).unwrap();
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
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":32768,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    let operation = OperationId::new();
    let output = super::execute(
        &workspace,
        &catalog,
        &settings,
        operation,
        super::Mode::Prompt,
    )
    .await;
    let mut answers = Vec::new();
    for line in std::str::from_utf8(&output).unwrap().lines().skip(1) {
        let change: Change = jsonl::parse_change(line).unwrap();
        for file in change.file.into_iter().chain(change.files) {
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
    let mut calls = Vec::new();
    while let Ok(model) = observed.try_recv() {
        calls.push(model);
    }
    calls.sort_unstable();
    assert_eq!(answers.len(), 1, "{answers:?}");
    assert_eq!(
        answers[0]["ok"]["text"], "parent saw child finished",
        "{answers:?}; model calls {calls:?}"
    );
    assert_eq!(calls, ["child", "parent", "parent", "parent"]);
    stop.send(()).unwrap();
    server.await.unwrap();
}
