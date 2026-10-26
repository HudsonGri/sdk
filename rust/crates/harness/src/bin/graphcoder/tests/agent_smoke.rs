//! One shipping-binary smoke for the actor entry and exact replay receipt.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_prompt_replays_the_same_settled_turn_without_regeneration() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&settings).unwrap();
    let generated = Arc::new(AtomicUsize::new(0));
    let calls = generated.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let catalog = catalog(directory.path(), listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new()
            .route("/count", post(|| async { Json(json!({"input_tokens":64})) }))
            .route("/responses", post(move |Json(body): Json<Value>| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    assert!(body.to_string().contains("hello"));
                    ([("content-type", "text/event-stream")], ANSWER)
                }
            })))
            .await.unwrap();
    });
    let operation = OperationId::new();
    let mut first = None;
    // Reopen through the other public input entry, retaining the same intent.
    // Both must converge on the one actor receipt without a request-file shim.
    for mode in [Mode::Prompt, Mode::Jsonl] {
        let output = execute(&workspace, &catalog, &settings, operation, mode).await;
        let lines: Vec<_> = std::str::from_utf8(&output).unwrap().lines().collect();
        assert_eq!(lines.len(), 1, "one exact settlement, no legacy snapshot dialect");
        let reply: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(reply["kind"], "settled");
        assert_eq!(reply["id"], operation.to_string());
        assert_eq!(reply["entry"]["input"]["message"]["id"], operation.to_string());
        assert!(reply["entry"]["input"]["message"]["inbox"].is_u64());
        assert_eq!(reply["output"]["text"], "fixture answer");
        if let Some(first) = &first {
            assert_eq!(&reply, first, "replay retains conversation sequence, input receipt and output");
        } else {
            first = Some(reply);
        }
    }
    assert_eq!(generated.load(Ordering::SeqCst), 1, "a replay must not call the model again");
    server.abort();
    let _ = server.await;
}
