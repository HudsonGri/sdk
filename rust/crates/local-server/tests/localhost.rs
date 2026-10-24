//! Canonical localhost service conformance and negative controls.
use acyclic_actors::wire;
use acyclic_local_server::{Registry, Scope, Server, Worker, client};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;
use tonic::{Code, Status};

struct Echo;
impl Worker for Echo {
    fn digest(&self) -> [u8; 32] {
        [7; 32]
    }
    fn fetch(
        &self,
        scope: Scope,
        request: wire::InvokeWorkerRequest,
    ) -> BoxProviderFuture<'_, Result<wire::InvokeActorResponse, Status>> {
        Box::pin(async move {
            let mut body = scope.account;
            body.extend(request.body);
            Ok(wire::InvokeActorResponse {
                status: 200,
                body,
                ..Default::default()
            })
        })
    }
}
#[tokio::test]
async fn canonical_worker_deploy_invoke_delete_is_account_scoped()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut registry = Registry::default();
    registry.worker(Arc::new(Echo))?;
    assert_eq!(
        registry.worker(Arc::new(Echo)).unwrap_err().code(),
        Code::AlreadyExists
    );
    let server = Server::start(
        Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        registry,
    )
    .await?;
    let mut api = wire::actors_service_client::ActorsServiceClient::new(
        client::channel(&server.endpoint()).await?,
    );
    for account in [vec![1], vec![2]] {
        let request = wire::DeploymentRequest {
            name: "worker".into(),
            account_id: account.clone(),
            code_sha256: vec![7; 32],
            worker_limits: Some(wire::WorkerLimits {
                cpu_millis: 10,
                memory_bytes: 1024,
                timeout_millis: 1000,
            }),
            idempotency_key: "install".into(),
            ..Default::default()
        };
        let first = api.deploy_worker(request.clone()).await?.into_inner();
        assert_eq!(first, api.deploy_worker(request).await?.into_inner());
        let body = api
            .invoke_worker(wire::InvokeWorkerRequest {
                worker_name: "worker".into(),
                account_id: account.clone(),
                method: "POST".into(),
                url: "http://localhost/".into(),
                body: vec![8],
                ..Default::default()
            })
            .await?
            .into_inner()
            .body;
        assert_eq!(body, [account, vec![8]].concat());
    }
    api.delete_worker(wire::DeleteWorkerRequest {
        worker_name: "worker".into(),
        account_id: vec![1],
        idempotency_key: "delete".into(),
    })
    .await?;
    let request = |account_id| wire::InvokeWorkerRequest {
        worker_name: "worker".into(),
        account_id,
        method: "GET".into(),
        url: "http://localhost/".into(),
        ..Default::default()
    };
    assert_eq!(
        api.invoke_worker(request(vec![1]))
            .await
            .err()
            .ok_or("old deployment unexpectedly resurrected")?
            .code(),
        Code::NotFound
    );
    assert_eq!(
        api.invoke_worker(request(vec![2])).await?.into_inner().body,
        vec![2]
    );
    assert_eq!(
        api.invoke_worker(request(vec![])).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    receipt_history(&mut api).await?;
    drop(api);
    server.shutdown().await
}

async fn receipt_history(
    api: &mut wire::actors_service_client::ActorsServiceClient<tonic::transport::Channel>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request = |account_id| wire::InvokeWorkerRequest {
        worker_name: "worker".into(),
        account_id,
        method: "GET".into(),
        url: "http://localhost/".into(),
        ..Default::default()
    };
    let install = wire::DeploymentRequest {
        name: "worker".into(),
        account_id: vec![1],
        code_sha256: vec![7; 32],
        worker_limits: Some(wire::WorkerLimits {
            cpu_millis: 10,
            memory_bytes: 1024,
            timeout_millis: 1000,
        }),
        idempotency_key: "install".into(),
        ..Default::default()
    };
    // Replaying the old install after deletion cannot resurrect a deployment.
    api.deploy_worker(install.clone()).await?;
    assert_eq!(
        api.invoke_worker(request(vec![1]))
            .await
            .err()
            .ok_or("old deployment unexpectedly resurrected")?
            .code(),
        Code::NotFound
    );
    let mut replacement = install.clone();
    replacement.idempotency_key = "replacement".into();
    api.deploy_worker(replacement).await?;
    // Replaying an old delete cannot retire the replacement at the same address.
    api.delete_worker(wire::DeleteWorkerRequest {
        worker_name: "worker".into(),
        account_id: vec![1],
        idempotency_key: "delete".into(),
    })
    .await?;
    assert_eq!(
        api.invoke_worker(request(vec![1])).await?.into_inner().body,
        vec![1]
    );
    let mut changed = install;
    changed
        .worker_limits
        .as_mut()
        .ok_or("missing limits")?
        .memory_bytes += 1;
    assert_eq!(
        api.deploy_worker(changed)
            .await
            .err()
            .ok_or("changed mutation unexpectedly admitted")?
            .code(),
        Code::AlreadyExists
    );
    Ok(())
}

#[tokio::test]
async fn credential_free_transport_cannot_escape_numeric_loopback()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for endpoint in [
        "http://example.org:1234",
        "http://localhost:1234",
        "https://127.0.0.1:1234",
        "http://192.0.2.1:1234",
        "http://user@127.0.0.1:1234",
        "http://127.0.0.1:1234/path",
        "http://127.0.0.1:1234/?route=external",
    ] {
        assert!(client::channel(endpoint).await.is_err());
        assert!(
            acyclic_stream::grpc::Client::connect_loopback(endpoint)
                .await
                .is_err()
        );
    }
    assert!(
        Server::bind(
            "0.0.0.0:0".parse()?,
            Arc::new(acyclic_stream::MemoryStream::default()),
            acyclic_fs::Fs::memory(),
            Registry::default()
        )
        .await
        .is_err()
    );
    Ok(())
}

#[path = "localhost/shutdown.rs"]
mod shutdown;
