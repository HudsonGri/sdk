//! Shipping `GraphCoder` against an SDK single-node deployed stock package.
use acyclic_actors::wire;
use acyclic_harness::{
    OperationId, client as files, local::LocalHarnessConfig, package::harness::HarnessCode,
};
use acyclic_harness_providers::{Credentials, Registry as Models};
use acyclic_local_server::{Package, Registry, Scope, Server, client, harness::Harness};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

mod build_metadata;

#[tokio::test]
async fn shipping_prompt_jsonl_replay_and_scope_use_the_deployed_package()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let app = Router::new().route("/count", post(|| async { Json(json!({"input_tokens":64})) }))
        .route("/responses", post(move |Json(body): Json<Value>| {
            let count = count.clone();
            async move {
                assert!(body.to_string().contains("hello"));
                count.fetch_add(1, Ordering::SeqCst);
                ([("content-type", "text/event-stream")], "data: {\"type\":\"response.output_text.delta\",\"delta\":\"deployed answer\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":64,\"output_tokens\":2}}}\n\n")
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let model_server = tokio::spawn(async move { axum::serve(listener, app).await });
    let catalog = directory.path().join("catalog.json");
    std::fs::write(
        &catalog,
        serde_json::to_vec(&json!({"fixture": {
            "adapter":"responses", "authentication":"optional", "base_url":format!("http://{address}"),
            "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},
            "max_event_bytes":4096, "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
        }}))?,
    )?;
    let mut models = Models::new(Credentials::explicit(BTreeMap::new()))?;
    models.load_catalog(&catalog)?;
    let provider = Arc::new(models.resolve("fixture/tiny")?);
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace)?;
    let local = Arc::new(
        HarnessCode::new()?
            .open_local(
                LocalHarnessConfig::new(&workspace, (provider.model().clone(), provider))
                    .hermetic()
                    .settings(
                        acyclic_harness::native_settings::SettingsConfig::with_roots(
                            workspace.clone(),
                            directory.path().join("settings"),
                            directory.path().join("codex"),
                        ),
                    ),
            )
            .await?,
    );
    let stream = local.task_runtime().task_host().workflow_stream();
    let filesystem = local.filesystem().filesystem().clone();
    let scope = Scope {
        account: b"local".to_vec(),
        namespace: "harness".into(),
    };
    let code = Arc::new(Harness::new(scope.clone(), local)?);
    let digest = code.digest();
    let mut registry = Registry::default();
    registry.package(code)?;
    let server = Server::start(stream, filesystem, registry).await?;
    let endpoint = server.endpoint();
    deploy(&endpoint, scope, digest).await?;
    for jsonl in [false, true] {
        let operation = OperationId::new();
        let request = format!(
            "{}\n",
            files::jsonl::request(&files::prepare_local_prompt(operation, "hello")?)?
        );
        for replay in [false, true] {
            let output = run_cli(&endpoint, directory.path(), operation, jsonl, &request).await?;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8(output.stdout)?.contains("deployed answer"),
                "replay={replay}"
            );
        }
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "reconnect must not dispatch a completed request again"
    );
    server.shutdown().await?;
    model_server.abort();
    Ok(())
}

async fn deploy(
    endpoint: &str,
    scope: Scope,
    digest: [u8; 32],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut api =
        wire::actors_service_client::ActorsServiceClient::new(client::channel(endpoint).await?);
    let deployment = wire::DeploymentRequest {
        name: scope.namespace,
        account_id: scope.account,
        code_sha256: digest.to_vec(),
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1000,
            memory_bytes: 8 * 1024 * 1024,
            checkpoint_bytes: 8 * 1024 * 1024,
        }),
        idempotency_key: "install".into(),
        ..Default::default()
    };
    api.deploy_namespace(deployment.clone()).await?;
    let mut wrong = deployment;
    wrong.account_id = b"another-account".to_vec();
    let Err(denied) = api.deploy_namespace(wrong).await else {
        return Err("foreign account deployment was admitted".into());
    };
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    Ok(())
}

async fn run_cli(
    endpoint: &str,
    directory: &std::path::Path,
    operation: OperationId,
    jsonl: bool,
    request: &str,
) -> Result<std::process::Output, Box<dyn std::error::Error + Send + Sync>> {
    let mut command = Command::new(build_metadata::executable());
    command
        .args([
            "--endpoint",
            endpoint,
            "--operation",
            &operation.to_string(),
            "--credentials",
            "deliberately-absent-credentials.json",
        ])
        .env("HOME", directory)
        .env("USERPROFILE", directory)
        .env("CODEX_HOME", directory.join("fixture-codex"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if jsonl {
        command.args(["--input-format", "jsonl"]);
    } else {
        command.args(["-p", "hello"]);
    }
    let mut child = command.spawn()?;
    if jsonl {
        child
            .stdin
            .take()
            .ok_or("missing stdin")?
            .write_all(request.as_bytes())?;
    } else {
        drop(child.stdin.take());
    }
    let output = tokio::task::spawn_blocking(move || child.wait_with_output()).await??;
    Ok(output)
}
