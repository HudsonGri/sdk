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

#[tokio::test]
async fn actor_inspection_preserves_progress_for_every_pair_of_reply_orders()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use acyclic_harness::{
        AgentId,
        workflow::{MachineCheckpoint, MachineStatus},
    };
    let storage =
        acyclic_harness::filesystem::MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
    let command = acyclic_harness::workflow::WorkflowCommand {
        operation_id: acyclic_harness::OperationId::new(),
        kind: "fixture.effect".into(),
        payload: storage
            .stage(
                acyclic_harness::OperationId::new(),
                "input.json",
                b"{}",
                "application/json",
                "input.json",
            )
            .await?,
    };
    let deployment = Deployment {
        request: wire::DeploymentRequest::default(),
        revision: 1,
        runtime: Instance::Workflow(Arc::new(Uncalled)),
        actors: Arc::new(Mutex::new(BTreeMap::new())),
    };
    let observations: Vec<_> = [
        (1, 0, 0, false, 0),
        (1, 1, 1, false, 1),
        (1, 1, 1, false, 0),
        (1, 2, 2, false, 1),
        (1, 2, 2, false, 0),
        (1, 2, 3, false, 0),
        (1, 2, 3, true, 0),
        (2, 2, 3, false, 0),
    ]
    .into_iter()
    .map(|(epoch, revision, consumed, cancelled, pending)| Snapshot {
        placement: Placement {
            actor: AgentId::from_bytes([1; 16]),
            node: "owner".into(),
            epoch,
        },
        checkpoint: MachineCheckpoint {
            machine: MachineIdentity {
                name: "fixture".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            revision,
            state: serde_json::json!({"revision":revision}),
        },
        consumed,
        status: if cancelled {
            MachineStatus::Failed {
                message: "actor cancelled".into(),
            }
        } else {
            MachineStatus::Suspended
        },
        pending: vec![command.clone(); pending],
        cancelled,
    })
    .collect();
    for (a, first) in observations.iter().enumerate() {
        for (b, second) in observations.iter().enumerate() {
            deployment.actors.lock().await.clear();
            Actors::remember_actor(&deployment, "actor".into(), first.clone()).await;
            Actors::remember_actor(&deployment, "actor".into(), second.clone()).await;
            assert_eq!(
                deployment.actors.lock().await.get("actor"),
                observations.get(a.max(b)),
                "reply order {a},{b}"
            );
        }
    }
    Ok(())
}
