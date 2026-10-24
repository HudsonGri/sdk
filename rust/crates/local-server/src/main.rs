//! One-command native stock package deployment on the SDK's local services.
use acyclic_actors::wire;
use acyclic_harness::{local::LocalHarnessConfig, package::harness::HarnessCode};
use acyclic_harness_providers::{Credentials, Registry as Models};
use acyclic_local_server::{Package, Registry, Scope, Server, client, harness::Harness};
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, sync::Arc};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(Box::pin(run()))
}
async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut workspace = PathBuf::from(".");
    let mut catalog = None;
    let mut model = None;
    let mut account = "local".to_owned();
    let mut namespace = "harness".to_owned();
    let mut listen: SocketAddr = "127.0.0.1:0".parse()?;
    let mut arguments = std::env::args().skip(1);
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--workspace" => workspace = value.into(),
            "--catalog" => catalog = Some(PathBuf::from(value)),
            "--model" => model = Some(value),
            "--account" => account = value,
            "--namespace" => namespace = value,
            "--listen" => listen = value.parse()?,
            _ => return Err(format!("unknown option {flag}").into()),
        }
    }
    // Never discover a native login, keychain or environment credential.
    let mut models = Models::new(Credentials::explicit(BTreeMap::new()))?;
    models.load_catalog(&catalog.ok_or("--catalog is required (explicit model HTTPS binding)")?)?;
    let provider = Arc::new(models.resolve(&model.ok_or("--model is required")?)?);
    let settings = tempfile::tempdir()?;
    let config = LocalHarnessConfig::new(&workspace, (provider.model().clone(), provider))
        .hermetic()
        .settings(
            acyclic_harness::native_settings::SettingsConfig::with_roots(
                workspace.clone(),
                settings.path().to_path_buf(),
                settings.path().join("codex"),
            ),
        );
    let local = Arc::new(HarnessCode::new()?.open_local(config).await?);
    let stream = local.task_runtime().task_host().workflow_stream();
    let filesystem = local.filesystem().filesystem().clone();
    let scope = Scope {
        account: account.into_bytes(),
        namespace,
    };
    let code = Arc::new(Harness::new(scope.clone(), local)?);
    let digest = code.digest();
    let mut registry = Registry::default();
    registry.package(code)?;
    let server = Server::bind(listen, stream, filesystem, registry).await?;
    wire::actors_service_client::ActorsServiceClient::new(
        client::channel(&server.endpoint()).await?,
    )
    .deploy_namespace(wire::DeploymentRequest {
        name: scope.namespace.clone(),
        account_id: scope.account,
        code_sha256: digest.to_vec(),
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1000,
            memory_bytes: 8 * 1024 * 1024,
            checkpoint_bytes: 8 * 1024 * 1024,
        }),
        idempotency_key: "sdk-local-install".into(),
        ..Default::default()
    })
    .await?;
    println!("{} namespace={}", server.endpoint(), scope.namespace);
    tokio::signal::ctrl_c().await?;
    server.shutdown().await
}
