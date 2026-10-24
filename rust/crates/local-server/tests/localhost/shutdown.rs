//! Shutdown fences admission before draining, retaining effect transports.
use acyclic_actors::wire;
use acyclic_local_server::{Fetch, Instance, Package, Registry, Scope, Server, client};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use tonic::Status;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Guest {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<oneshot::Receiver<()>>,
}
impl Fetch for Guest {
    fn fetch(
        &self,
        _: wire::InvokeActorRequest,
    ) -> BoxProviderFuture<'_, Result<wire::InvokeActorResponse, Status>> {
        Box::pin(async {
            Ok(wire::InvokeActorResponse {
                status: 200,
                ..Default::default()
            })
        })
    }
    fn shutdown(&self) -> BoxProviderFuture<'_, Result<(), Status>> {
        Box::pin(async move {
            self.entered
                .lock()
                .await
                .take()
                .ok_or_else(|| Status::internal("drain repeated"))?
                .send(())
                .map_err(|_| Status::internal("observer dropped"))?;
            (&mut *self.release.lock().await)
                .await
                .map_err(|_| Status::internal("release dropped"))
        })
    }
}
struct Code(Arc<Guest>);
impl Package for Code {
    fn digest(&self) -> [u8; 32] {
        [9; 32]
    }
    fn deploy(&self, _: Scope) -> BoxProviderFuture<'_, Result<Instance, Status>> {
        Box::pin(async { Ok(Instance::Fetch(self.0.clone())) })
    }
}
#[tokio::test]
async fn shutdown_fences_actor_admission_and_preserves_effect_services_until_drained() -> TestResult
{
    let (entered, draining) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let mut registry = Registry::default();
    registry.package(Arc::new(Code(Arc::new(Guest {
        entered: Mutex::new(Some(entered)),
        release: Mutex::new(released),
    }))))?;
    let server = Server::start(
        Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        registry,
    )
    .await?;
    let channel = client::channel(&server.endpoint()).await?;
    let mut actors = wire::actors_service_client::ActorsServiceClient::new(channel.clone());
    let mut fs = acyclic_fs_wire::wire::filesystem::v2::filesystem_service_client::FilesystemServiceClient::new(channel);
    actors
        .deploy_namespace(wire::DeploymentRequest {
            name: "scope".into(),
            account_id: vec![1],
            code_sha256: vec![9; 32],
            limits: Some(wire::ActorLimits {
                handler_timeout_millis: 1000,
                memory_bytes: 1024,
                checkpoint_bytes: 1024,
            }),
            idempotency_key: "install".into(),
            ..Default::default()
        })
        .await?;
    let stopping = tokio::spawn(server.shutdown());
    draining.await?;
    let request = wire::InvokeActorRequest {
        account_id: vec![1],
        namespace: Some("scope".into()),
        actor_id: "actor".into(),
        method: "GET".into(),
        url: "http://localhost/".into(),
        ..Default::default()
    };
    assert_eq!(
        actors.invoke_actor(request).await.unwrap_err().code(),
        tonic::Code::Unavailable,
        "negative control: an open connection cannot admit more work after the fence"
    );
    use acyclic_fs_wire::wire::filesystem::v2 as wire_fs;
    fs.create_workspace(wire_fs::CreateWorkspaceRequest {
        name: "draining-effect".into(),
        profile: wire_fs::FilesystemProfile::Portable as i32,
        operation: Some(wire_fs::OperationOptions {
            idempotency_key: vec![1; 16],
        }),
    })
    .await?;
    release.send(()).map_err(|()| "drain owner disappeared")?;
    drop(actors);
    drop(fs);
    stopping.await??;
    Ok(())
}
