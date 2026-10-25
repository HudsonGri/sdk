//! The admission lease outlives snapshot publication, including a first RPC.
use super::*;
use acyclic_harness::package::ActorPackage;
use futures::FutureExt;

#[tokio::test]
async fn every_accepted_service_socket_disables_packet_coalescing() -> std::io::Result<()> {
    use futures::StreamExt;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let connected = tokio::net::TcpStream::connect(listener.local_addr()?).await?;
    let mut sockets = incoming(listener);
    let accepted = sockets
        .next()
        .await
        .ok_or(std::io::ErrorKind::UnexpectedEof)??;
    assert!(accepted.nodelay()?);
    drop(connected);
    Ok(())
}

struct Uncalled;
impl ActorRuntime for Uncalled {
    fn install(
        &self,
        _: Arc<dyn ActorPackage>,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<()>> {
        Box::pin(async { Err(acyclic_harness::Error::Unsupported("uncalled".into())) })
    }
    fn place(
        &self,
        _: Placement,
        _: MachineIdentity,
        _: Value,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        Box::pin(async { Err(acyclic_harness::Error::Unsupported("uncalled".into())) })
    }
    fn deliver(
        &self,
        _: Placement,
        _: Event,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        Box::pin(async { Err(acyclic_harness::Error::Unsupported("uncalled".into())) })
    }
    fn recover(&self, _: Placement) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        Box::pin(async { Err(acyclic_harness::Error::Unsupported("uncalled".into())) })
    }
    fn cancel(
        &self,
        _: Placement,
        _: Event,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        Box::pin(async { Err(acyclic_harness::Error::Unsupported("uncalled".into())) })
    }
}

#[tokio::test]
async fn workflow_retirement_requires_every_admitted_snapshot_owner_to_release()
-> Result<(), Status> {
    let scope = Scope::new(vec![1], "scope".into())?;
    let actors = Actors {
        registry: Arc::new(Registry::default()),
        catalog: Arc::new(Mutex::new(Catalog::default())),
        request_admissions: Arc::new(tokio::sync::RwLock::new(())),
    };
    actors.catalog.lock().await.namespaces.insert(
        scope,
        Arc::new(Deployment {
            request: wire::DeploymentRequest::default(),
            revision: 1,
            runtime: Instance::Workflow(Arc::new(Uncalled)),
            actors: Arc::new(Mutex::new(BTreeMap::new())),
        }),
    );
    let first = actors.deployment(vec![1], Some("scope".into())).await?;
    let second = actors.deployment(vec![1], Some("scope".into())).await?;
    actors.catalog.lock().await.closing = true;
    assert!(
        actors
            .request_admissions
            .clone()
            .write_owned()
            .now_or_never()
            .is_none(),
        "negative control: omitting the production lease permits early retirement"
    );
    drop(first);
    assert!(
        actors
            .request_admissions
            .clone()
            .write_owned()
            .now_or_never()
            .is_none()
    );
    let denied = actors.deployment(vec![1], Some("scope".into())).await;
    assert!(matches!(denied, Err(status) if status.code() == tonic::Code::Unavailable));
    drop(second);
    assert!(
        actors
            .request_admissions
            .clone()
            .write_owned()
            .now_or_never()
            .is_some()
    );
    Ok(())
}

struct ParkedWorker {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<tokio::sync::oneshot::Receiver<()>>,
}
impl crate::Worker for ParkedWorker {
    fn digest(&self) -> [u8; 32] {
        [7; 32]
    }
    fn fetch(
        &self,
        _: Scope,
        _: wire::InvokeWorkerRequest,
    ) -> BoxProviderFuture<'_, Result<wire::InvokeActorResponse, Status>> {
        Box::pin(async move {
            self.entered
                .lock()
                .await
                .take()
                .ok_or_else(|| failure("entered twice"))?
                .send(())
                .map_err(|()| failure("observer dropped"))?;
            (&mut *self.release.lock().await).await.map_err(failure)?;
            Ok(wire::InvokeActorResponse::default())
        })
    }
}

#[tokio::test]
async fn worker_invocation_holds_the_shared_retirement_lease()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let mut registry = Registry::default();
    registry.worker(Arc::new(ParkedWorker {
        entered: Mutex::new(Some(entered)),
        release: Mutex::new(released),
    }))?;
    let actors = Actors {
        registry: Arc::new(registry),
        catalog: Arc::new(Mutex::new(Catalog::default())),
        request_admissions: Arc::new(tokio::sync::RwLock::new(())),
    };
    actors.catalog.lock().await.workers.insert(
        Scope::new(vec![1], "worker".into())?,
        wire::DeploymentRequest {
            code_sha256: vec![7; 32],
            ..Default::default()
        },
    );
    let invoking = actors.clone();
    let task = tokio::spawn(async move {
        invoking
            .invoke_worker(Request::new(wire::InvokeWorkerRequest {
                worker_name: "worker".into(),
                account_id: vec![1],
                method: "GET".into(),
                url: "http://localhost/".into(),
                ..Default::default()
            }))
            .await
    });
    observed.await?;
    actors.catalog.lock().await.closing = true;
    assert!(
        actors
            .request_admissions
            .clone()
            .write_owned()
            .now_or_never()
            .is_none(),
        "negative control: removing the invocation lease retires its transports early"
    );
    release.send(()).map_err(|()| "worker owner disappeared")?;
    task.await??;
    assert!(
        actors
            .request_admissions
            .clone()
            .write_owned()
            .now_or_never()
            .is_some()
    );
    Ok(())
}
