use super::*;

#[tokio::test]
async fn pre_dispatch_failures_settle_and_close_without_provider_requests() -> Result<()> {
    for (name, prompt, policy) in [
        (
            "no older source",
            "x".repeat(70_000),
            crate::context::ThresholdCompaction {
                response_reserve_tokens: 4096,
                recent_tokens: 100,
                ..Default::default()
            },
        ),
        (
            "mandatory source",
            "x".repeat(40_000),
            crate::context::ThresholdCompaction {
                response_reserve_tokens: 32_000,
                recent_tokens: 1,
                ..Default::default()
            },
        ),
    ] {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let provider = Arc::new(ScriptedModelProvider::new([]).with_context_capacity(
            ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: 32_000,
            },
        ));
        let harness = LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
                .without_default_tools()
                .compaction(crate::context::CompactionPolicy::Threshold(policy)),
        )
        .await?;
        let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
            harness.submit(OperationId::new(), &prompt).await?
        else {
            panic!("{name}: admission rejected")
        };
        assert!(
            matches!(harness.resume(task_id).await,
            Err(Error::TaskFailed { task, .. }) if task == task_id),
            "{name}: preparation did not settle as Failed"
        );
        assert!(
            provider.requests().is_empty(),
            "{name}: provider dispatched"
        );
        harness.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn post_dispatch_failure_keeps_the_turn_indeterminate() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([ScriptedTurn::Events(vec![ScriptedEvent::Error {
        error: "response interrupted after dispatch".into(),
    }])]);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?.without_default_tools(),
    )
    .await?;
    let result = harness.run("answer").await;
    assert!(matches!(result, Err(Error::Indeterminate(_))));
    assert_eq!(provider.requests().len(), 1);
    harness.disconnect_unfinished().await?;
    Ok(())
}

#[tokio::test]
async fn http_preparation_failure_closes() {
    use ::acyclic_harness as shipping;
    use ::acyclic_harness_providers::{Credentials, Registry};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let counts = Arc::new(AtomicUsize::new(0));
    let generations = Arc::new(AtomicUsize::new(0));
    let observed = (counts.clone(), generations.clone());
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let read = socket.read(&mut buffer).await.unwrap();
                assert_ne!(read, 0);
                input.extend_from_slice(&buffer[..read]);
                if let Some(end) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&input[..end]).to_ascii_lowercase();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    if input.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let body = if input.starts_with(b"POST /count ") {
                observed.0.fetch_add(1, Ordering::SeqCst);
                r#"{"input_tokens":70000}"#
            } else {
                observed.1.fetch_add(1, Ordering::SeqCst);
                r#"{"error":"generation must not start"}"#
            };
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap();
    registry.register("loopback".into(), serde_json::from_value(json!({
        "adapter":"responses", "authentication":"optional", "base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":65536,"output_tokens":4096},"features":{"tools":true,"images":false}}},
        "max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    })).unwrap()).unwrap();
    let provider = registry.resolve("loopback/tiny").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let project = volume(directory.path()).await.unwrap();
    let project = serde_json::from_value(serde_json::to_value(project).unwrap()).unwrap();
    let config = shipping::LocalHarnessConfig::new(
        directory.path().join("filesystem"),
        (provider.model().clone(), Arc::new(provider)),
    )
    .volume(project)
    .state_directory(directory.path().join("state"))
    .without_default_tools()
    .compaction(shipping::context::CompactionPolicy::Threshold(
        shipping::context::ThresholdCompaction {
            response_reserve_tokens: 4096,
            recent_tokens: 100,
            ..Default::default()
        },
    ));
    let harness = shipping::LocalHarness::open(config).await.unwrap();
    let result = harness.run("answer").await;
    assert!(
        matches!(&result, Err(shipping::Error::TaskFailed { message, .. }) if message.contains("mandatory content leaves no compaction source")),
        "{result:?}"
    );
    assert!(counts.load(Ordering::SeqCst) > 0);
    assert_eq!(generations.load(Ordering::SeqCst), 0);
    harness.close().await.unwrap();

    server.abort();
}
