//! The documented command must publish an already deployed stock package.
use acyclic_harness::OperationId;
use acyclic_local_server::{Scope, harness};
use acyclic_native_runtime::{ProcessInput, ProcessOutput, ProcessTree};
use std::{
    io,
    process::{Command, Stdio},
    time::Duration,
};

#[expect(
    clippy::disallowed_macros,
    reason = "Cargo-provided companion executable metadata"
)]
const EXECUTABLE: &str = env!("CARGO_BIN_EXE_sdk-local");

#[tokio::test]
async fn command_is_ready_only_after_the_stock_package_is_deployed()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let catalog = directory.path().join("models.json");
    // Startup admits its model token counter before publishing readiness.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = axum::Router::new().route(
        "/count",
        axum::routing::post(|| async { axum::Json(serde_json::json!({"input_tokens":64})) }),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let model = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    std::fs::write(
        &catalog,
        serde_json::to_vec(&serde_json::json!({"fixture": {
            "adapter":"responses", "authentication":"optional", "base_url":format!("http://{address}"),
            "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},
            "max_event_bytes":4096, "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
        }}))?,
    )?;
    let runtime = tokio::runtime::Handle::current();
    acyclic_native_runtime::run_blocking_io(
        move || {
            let mut command = fixture_command(directory.path(), &catalog)?;
            let mut tree = ProcessTree::spawn(&mut command)?;
            let mut ready = Vec::new();
            let mut verified = false;
            let output = tree.wait_with_output(
                Duration::from_secs(5),
                16 * 1024,
                None,
                || Ok(()),
                |chunk| {
                    let ProcessOutput::Stdout(bytes) = chunk else {
                        return Ok(ProcessInput::default());
                    };
                    ready.extend_from_slice(bytes);
                    if !ready.contains(&b'\n') {
                        return Ok(ProcessInput::default());
                    }
                    let line = std::str::from_utf8(&ready).map_err(io::Error::other)?;
                    let endpoint = line
                        .split_whitespace()
                        .next()
                        .ok_or(io::ErrorKind::InvalidData)?;
                    runtime.block_on(async {
                        let scope = Scope {
                            account: b"command".to_vec(),
                            namespace: "installed".into(),
                        };
                        harness::connect(endpoint, scope.clone(), OperationId::new())
                            .await
                            .map_err(io::Error::other)?;
                        let foreign = Scope {
                            account: b"foreign".to_vec(),
                            ..scope
                        };
                        assert!(
                            harness::connect(endpoint, foreign, OperationId::new())
                                .await
                                .is_err()
                        );
                        Ok::<_, io::Error>(())
                    })?;
                    verified = true;
                    // The canonical process owner reaps the command and every
                    // descendant before returning the deliberate observation stop.
                    Err(io::ErrorKind::Interrupted.into())
                },
                None,
                None,
            );
            let failure = output.expect_err("the observation explicitly stops the owned command");
            assert!(
                verified,
                "{}; stderr={}",
                failure.error,
                String::from_utf8_lossy(&failure.stderr)
            );
            assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
            assert!(failure.cleanup_completed);
            assert!(tree.is_reaped());
            Ok::<_, io::Error>(())
        },
        None,
    )?
    .await??;
    let _ = stop.send(());
    model.await??;
    Ok(())
}

fn fixture_command(root: &std::path::Path, catalog: &std::path::Path) -> io::Result<Command> {
    let workspace = root.join("workspace");
    std::fs::create_dir(&workspace)?;
    let mut command = Command::new(acyclic_test_support::binary_path(EXECUTABLE));
    command.env_clear();
    // Startup pins the platform shell from PATH. Keep only executable lookup
    // and Windows system roots; model credentials and user profiles stay out.
    for key in ["PATH", "SystemRoot", "WINDIR"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    for key in [
        "HOME",
        "USERPROFILE",
        "CODEX_HOME",
        "LOCALAPPDATA",
        "APPDATA",
        "TMP",
        "TEMP",
        "TMPDIR",
    ] {
        command.env(key, root);
    }
    command
        .args([
            "--workspace",
            workspace.to_str().ok_or(io::ErrorKind::InvalidData)?,
            "--catalog",
            catalog.to_str().ok_or(io::ErrorKind::InvalidData)?,
            "--model",
            "fixture/tiny",
            "--account",
            "command",
            "--namespace",
            "installed",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    Ok(command)
}
