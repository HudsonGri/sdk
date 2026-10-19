use super::*;
use acyclic_harness::task_volume::Channel;

fn event(value: Value) -> String {
    format!("data: {value}\n\n")
}

async fn scenario(mode: Mode, answer: &'static str, stream_answer: bool) {
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
                    let tool_results = body["input"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|item| item["type"] == "function_call_output")
                        .count();
                    requests.send(tool_results).unwrap();
                    let mut response = String::new();
                    if tool_results < 2 {
                        response += &event(json!({"type":"response.output_text.delta",
                        "delta":format!("intermediate {tool_results} λ\n")}));
                        let tool = body["tools"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|tool| tool["name"] == "fs")
                            .unwrap();
                        response +=
                            &event(json!({"type":"response.output_item.done","output_index":1,
                        "item":{"type":"function_call","call_id":format!("status_{tool_results}"),
                            "name":tool["name"],"arguments":"{\"command\":\"status\"}"}}));
                    } else {
                        if stream_answer {
                            response += &event(
                                json!({"type":"response.output_text.delta", "output_index":0,
                            "delta":"older answer in the final response"}),
                            );
                            // Separate deltas must still form exactly one final message.
                            for delta in ["", answer] {
                                response += &event(
                                    json!({"type":"response.output_text.delta","delta":delta}),
                                );
                            }
                        }
                        // An older item's late completion cannot replace the last
                        // output position, including an explicitly empty answer.
                        for (position, text) in
                            [(1, answer), (0, "older answer in the final response")]
                        {
                            response += &event(
                                json!({"type":"response.output_item.done", "output_index":position,
                                "item":{"type":"message", "role":"assistant", "content":[
                                    {"type":"output_text", "text":text}]}}),
                            );
                        }
                    }
                    response += &event(json!({"type":"response.completed","response":{
                    "status":"completed","usage":{"input_tokens":64,"output_tokens":8}}}));
                    ([("content-type", "text/event-stream")], response)
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
    std::fs::write(workspace.join("input.txt"), "fixture input").unwrap();
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},
            "features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    let operation = OperationId::new();
    let output = execute(&workspace, &catalog, &settings, operation, mode).await;
    let mut results = Vec::new();
    let mut chunks = Vec::new();
    for line in std::str::from_utf8(&output).unwrap().lines().skip(1) {
        let change = jsonl::parse_change(line).unwrap();
        for file in change.file.into_iter().chain(change.files) {
            match file.key {
                FileKey::Session {
                    file: TaskFile::ExecutionResult(_),
                    ..
                } => {
                    results.push(serde_json::from_slice::<Value>(&file.bytes).unwrap());
                }
                FileKey::Task {
                    file: TaskFile::Chunk { .. },
                    ..
                } if file.channel == Some(Channel::Text) => {
                    chunks.push(String::from_utf8(file.bytes.to_vec()).unwrap());
                }
                _ => {}
            }
        }
    }
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["completion"], "completed");
    assert_eq!(results[0]["ok"]["steps"], 3);
    assert_eq!(results[0]["ok"]["text"], answer);
    assert_eq!(
        chunks
            .iter()
            .filter(|text| !text.is_empty())
            .cloned()
            .collect::<Vec<_>>(),
        [
            "intermediate 0 λ\n",
            "intermediate 1 λ\n",
            if stream_answer {
                "older answer in the final response"
            } else {
                ""
            },
            if stream_answer { answer } else { "" }
        ]
        .into_iter()
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>()
    );
    // Reopening exercises the persisted result through the harness client too.
    let reopened = execute(&workspace, &catalog, &settings, operation, mode).await;
    let (_, snapshot) = jsonl::parse_snapshot(
        std::str::from_utf8(&reopened)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    let restored = snapshot
        .files
        .iter()
        .find(|file| {
            matches!(
                file.key,
                FileKey::Session {
                    file: TaskFile::ExecutionResult(_),
                    ..
                }
            )
        })
        .unwrap();
    let bytes: Vec<_> = restored
        .segments
        .iter()
        .flat_map(|part| part.iter().copied())
        .collect();
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), results[0]);
    assert_eq!(
        std::iter::from_fn(|| observed.try_recv().ok()).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn prompt_final_message_excludes_intermediate_jsonl_frames() {
    scenario(Mode::Prompt, "  final λ\nanswer\n", true).await;
}

#[tokio::test]
async fn jsonl_final_message_excludes_intermediate_jsonl_frames() {
    scenario(Mode::Jsonl, "final answer", true).await;
}

#[tokio::test]
async fn empty_final_message_does_not_reuse_intermediate_text() {
    scenario(Mode::Prompt, "", true).await;
}

#[tokio::test]
async fn completed_output_without_deltas_is_rendered_and_retained() {
    scenario(Mode::Prompt, "completed λ\nanswer", false).await;
}

#[tokio::test]
async fn jsonl_completed_output_without_deltas_is_rendered_and_retained() {
    scenario(Mode::Jsonl, "", false).await;
}
