use super::{TestResult, local_workspace::Host, mcp_case::McpFixture};
use acyclic_harness::{
    IdempotencyKey, filesystem::WorkspaceMutation, native_settings::SettingsConfig,
    resources::WorkspaceRef,
};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{Request, Response, body::Incoming, service::service_fn};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};
use tokio::task::{JoinHandle, JoinSet};

pub(super) struct Server(Option<JoinHandle<TestResult>>);

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

impl Server {
    pub(super) async fn open(
        directory: &Path,
        fixture: McpFixture,
        host: Arc<Host>,
        workspace: WorkspaceRef,
    ) -> TestResult<(Self, SettingsConfig)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut server = json!({
            "type":"http", "url":format!("http://{}/mcp", listener.local_addr()?),
            "required":true, "schema_exposure":&fixture.schema_exposure,
            "disabled_tools":&fixture.disabled_tools,
        });
        if let Some(enabled) = &fixture.enabled_tools {
            server["enabled_tools"] = serde_json::to_value(enabled)?;
        }
        let home = directory
            .parent()
            .ok_or("missing fixture root")?
            .join("settings");
        std::fs::create_dir(&home)?;
        std::fs::write(
            home.join(".claude.json"),
            serde_json::to_vec(&json!({"mcpServers":{&fixture.server:server}}))?,
        )?;
        let settings = SettingsConfig::new(directory, &home).codex_home(home.join(".codex"));
        let fixture = Arc::new(fixture);
        let task = tokio::spawn(async move {
            // All fixture state is immutable. Tool effects use the real volume's
            // atomic publication path; there is no fixture lock or request counter.
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    completed = connections.join_next(), if !connections.is_empty() => {
                        completed.ok_or("MCP connection set ended")???;
                    }
                    accepted = listener.accept() => {
                        let (socket, _) = accepted?;
                        let fixture = fixture.clone();
                        let host = host.clone();
                        let workspace = workspace.clone();
                        connections.spawn(async move {
                            let service = service_fn(move |request| {
                                exchange(request, fixture.clone(), host.clone(), workspace.clone())
                            });
                            hyper::server::conn::http1::Builder::new()
                                .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                                .await?;
                            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                        });
                    }
                }
            }
        });
        Ok((Self(Some(task)), settings))
    }

    pub(super) async fn close(mut self) -> TestResult {
        let task = self.0.take().ok_or("MCP server already closed")?;
        task.abort();
        match task.await {
            Err(error) if error.is_cancelled() => Ok(()),
            result => result?,
        }
    }
}

async fn exchange(
    request: Request<Incoming>,
    fixture: Arc<McpFixture>,
    host: Arc<Host>,
    workspace: WorkspaceRef,
) -> TestResult<Response<Full<Bytes>>> {
    if request.method() != hyper::Method::POST || request.uri().path() != "/mcp" {
        return Ok(Response::builder().status(405).body(Full::default())?);
    }
    let bytes = request.into_body().collect().await?.to_bytes();
    let request: Value = serde_json::from_slice(&bytes)?;
    let result = match request["method"].as_str() {
        Some("initialize") => json!({
            "protocolVersion":acyclic_harness::mcp::PROTOCOL_VERSION,
            "capabilities":{"tools":{}}, "serverInfo":{"name":"scenario", "version":"1"},
        }),
        Some("notifications/initialized") if request.get("id").is_none() => {
            return Ok(Response::builder().status(202).body(Full::default())?);
        }
        Some("tools/list") => {
            let cursor = match request["params"].get("cursor") {
                None | Some(Value::Null) => "",
                Some(Value::String(cursor)) => cursor,
                _ => return Err("invalid MCP fixture cursor".into()),
            };
            serde_json::to_value(fixture.pages.get(cursor).ok_or("unexpected MCP page")?)?
        }
        Some("tools/call") => {
            let params = &request["params"];
            let reply = fixture.reply(
                params["name"].as_str().ok_or("missing MCP tool name")?,
                &params["arguments"],
            )?;
            let mutations = reply
                .files
                .iter()
                .map(|(path, text)| WorkspaceMutation::PutFile {
                    path: format!("/{path}"),
                    bytes: text.as_bytes().to_vec().into(),
                })
                .collect::<Vec<_>>();
            if !mutations.is_empty() {
                let key = IdempotencyKey::new(format!("mcp-{}", blake3::hash(&bytes)))?;
                host.apply(&workspace, None, &mutations, &key).await?;
            }
            reply.result.clone()
        }
        _ => return Err("unexpected MCP fixture method".into()),
    };
    let body = serde_json::to_vec(&json!({"jsonrpc":"2.0", "id":request["id"], "result":result}))?;
    Ok(Response::builder()
        .header("Content-Type", "application/json")
        .body(Full::new(body.into()))?)
}
