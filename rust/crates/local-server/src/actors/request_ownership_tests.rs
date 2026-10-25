//! Admitted work survives loss of its RPC observer on every execution ABI.
use super::*;
use acyclic_harness::{
    AgentId, IdempotencyKey, OperationId,
    workflow::{MachineCheckpoint, MachineStatus},
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct ClockGuest;
impl Fetch for ClockGuest {
    fn fetch(
        &self,
        _: wire::InvokeActorRequest,
    ) -> BoxProviderFuture<'_, Result<wire::InvokeActorResponse, Status>> {
        Box::pin(async {
            let clock = acyclic_native_runtime::testing::CapturedClock::capture()
                .map(|clock| clock.unix_millis());
            Ok(wire::InvokeActorResponse {
                body: serde_json::to_vec(&clock).map_err(failure)?,
                ..Default::default()
            })
        })
    }
    fn shutdown(&self) -> BoxProviderFuture<'_, Result<(), Status>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn admitted_requests_preserve_the_original_native_schedule() -> TestResult {
    use acyclic_native_runtime::testing::Schedule;
    let actors = Actors {
        registry: Arc::new(Registry::default()),
        catalog: Arc::new(Mutex::new(Catalog::default())),
        request_admissions: Arc::new(tokio::sync::RwLock::new(())),
    };
    actors.catalog.lock().await.namespaces.insert(
        Scope::new(vec![1], "scope".into())?,
        Arc::new(Deployment {
            request: wire::DeploymentRequest::default(),
            revision: 1,
            runtime: Instance::Fetch(Arc::new(ClockGuest)),
            actors: Arc::new(Mutex::new(BTreeMap::new())),
        }),
    );
    let (schedule, mut events) = Schedule::seeded(71);
    schedule.set_unix_millis(100);
    let operation = schedule.scope(async {
        let response = actors
            .invoke_actor(Request::new(wire::InvokeActorRequest {
                actor_id: "actor".into(),
                account_id: vec![1],
                namespace: Some("scope".into()),
                method: "GET".into(),
                url: "http://localhost/".into(),
                ..Default::default()
            }))
            .await?
            .into_inner();
        schedule.drained().await;
        serde_json::from_slice::<Option<u64>>(&response.body).map_err(failure)
    });
    tokio::pin!(operation);
    let clock = loop {
        tokio::select! {
            biased;
            event = events.recv() => event.ok_or("lost native schedule boundary")?.release(),
            result = &mut operation => break result?,
        }
    };
    assert_eq!(
        clock,
        Some(100),
        "admitted work escaped its original native context"
    );
    Ok(())
}

struct Gate {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<oneshot::Receiver<()>>,
}
impl Gate {
    fn new() -> (Self, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        (
            Self {
                entered: Mutex::new(Some(entered)),
                release: Mutex::new(released),
            },
            observed,
            release,
        )
    }
    async fn wait(&self) -> Result<(), Status> {
        self.entered
            .lock()
            .await
            .take()
            .ok_or_else(|| failure("entered twice"))?
            .send(())
            .map_err(|()| failure("observer dropped"))?;
        (&mut *self.release.lock().await).await.map_err(failure)
    }
}

struct Workflow {
    gate: Gate,
    snapshot: Snapshot,
}
impl Workflow {
    fn run(&self) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        Box::pin(async move {
            self.gate
                .wait()
                .await
                .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
            Ok(self.snapshot.clone())
        })
    }
}
impl ActorRuntime for Workflow {
    fn install(
        &self,
        _: Arc<dyn acyclic_harness::package::ActorPackage>,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn place(
        &self,
        _: Placement,
        _: MachineIdentity,
        _: Value,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        self.run()
    }
    fn deliver(
        &self,
        _: Placement,
        _: Event,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        self.run()
    }
    fn recover(&self, _: Placement) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        self.run()
    }
    fn cancel(
        &self,
        _: Placement,
        _: Event,
    ) -> BoxProviderFuture<'_, acyclic_harness::Result<Snapshot>> {
        self.run()
    }
}

fn snapshot() -> Snapshot {
    Snapshot {
        placement: Placement {
            actor: AgentId::from_bytes([1; 16]),
            node: "owner".into(),
            epoch: 1,
        },
        checkpoint: MachineCheckpoint {
            machine: MachineIdentity {
                name: "fixture".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            revision: 1,
            state: Value::Null,
        },
        consumed: 1,
        status: MachineStatus::Suspended,
        pending: vec![],
        cancelled: false,
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Place,
    Deliver,
    Recover,
    Cancel,
    Checkpoint,
}

#[tokio::test]
async fn disconnected_workflow_requests_retain_the_lease_until_snapshot_publication() -> TestResult
{
    for operation in [
        Operation::Place,
        Operation::Deliver,
        Operation::Recover,
        Operation::Cancel,
        Operation::Checkpoint,
    ] {
        check_workflow(operation, true).await?;
    }
    // A checkpoint must use its existing admission during reply construction;
    // the closed catalog must not reject that already admitted response.
    check_workflow(Operation::Checkpoint, false).await?;
    Ok(())
}

async fn check_workflow(operation: Operation, disconnect: bool) -> TestResult {
    let (gate, entered, release) = Gate::new();
    let snapshot = snapshot();
    let deployment = Arc::new(Deployment {
        request: wire::DeploymentRequest {
            account_id: vec![1],
            name: "scope".into(),
            ..Default::default()
        },
        revision: 1,
        runtime: Instance::Workflow(Arc::new(Workflow {
            gate,
            snapshot: snapshot.clone(),
        })),
        actors: Arc::new(Mutex::new(BTreeMap::new())),
    });
    let id = snapshot.placement.actor.to_string();
    if matches!(operation, Operation::Checkpoint) {
        let mut previous = snapshot.clone();
        previous.consumed = 0;
        deployment.actors.lock().await.insert(id.clone(), previous);
    }
    let actors = Actors {
        registry: Arc::new(Registry::default()),
        catalog: Arc::new(Mutex::new(Catalog::default())),
        request_admissions: Arc::new(tokio::sync::RwLock::new(())),
    };
    actors
        .catalog
        .lock()
        .await
        .namespaces
        .insert(Scope::new(vec![1], "scope".into())?, deployment.clone());
    let invoking = actors.clone();
    let actor = id.clone();
    let placement = snapshot.placement.clone();
    let machine = snapshot.checkpoint.machine.clone();
    let task = tokio::spawn(async move {
        if matches!(operation, Operation::Checkpoint) {
            invoking
                .checkpoint_actor(Request::new(wire::CheckpointActorRequest {
                    actor_id: actor,
                    account_id: vec![1],
                    namespace: Some("scope".into()),
                    ..Default::default()
                }))
                .await?;
        } else {
            let event = Event {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new("input").map_err(runtime_error)?,
                input: Value::Null,
            };
            let call = match operation {
                Operation::Place => Call::Place {
                    placement,
                    machine,
                    input: Value::Null,
                },
                Operation::Deliver => Call::Deliver { placement, event },
                Operation::Recover => Call::Recover { placement },
                Operation::Cancel => Call::Cancel { placement, event },
                Operation::Checkpoint => return Err(failure("unexpected checkpoint")),
            };
            invoking
                .invoke_actor(Request::new(wire::InvokeActorRequest {
                    actor_id: actor,
                    account_id: vec![1],
                    namespace: Some("scope".into()),
                    method: "POST".into(),
                    url: "http://localhost/".into(),
                    body: serde_json::to_vec(&call).map_err(failure)?,
                    ..Default::default()
                }))
                .await?;
        }
        Ok::<_, Status>(())
    });
    entered.await?;
    let observer = if disconnect {
        task.abort();
        assert!(
            task.await
                .err()
                .ok_or("observer completed early")?
                .is_cancelled()
        );
        None
    } else {
        Some(task)
    };
    actors.catalog.lock().await.closing = true;
    let retiring = actors.request_admissions.clone().write_owned();
    tokio::pin!(retiring);
    assert!(
        futures::poll!(&mut retiring).is_pending(),
        "lost RPC observer released admitted work"
    );
    release
        .send(())
        .map_err(|()| "workflow disappeared with its observer")?;
    let _retired = retiring.await;
    assert_eq!(deployment.actors.lock().await.get(&id), Some(&snapshot));
    if let Some(observer) = observer {
        observer.await??;
    }
    Ok(())
}

struct Guest {
    gate: Gate,
    completed: Mutex<Option<oneshot::Sender<()>>>,
    stopped: Mutex<Option<oneshot::Sender<()>>>,
}

struct Code(Gate);
impl Package for Code {
    fn digest(&self) -> [u8; 32] {
        [1; 32]
    }
    fn deploy(&self, _: Scope) -> BoxProviderFuture<'_, Result<Instance, Status>> {
        Box::pin(async move {
            self.0.wait().await?;
            Ok(Instance::Workflow(Arc::new(Workflow {
                gate: Gate::new().0,
                snapshot: snapshot(),
            })))
        })
    }
}

#[tokio::test]
async fn disconnected_deployment_publishes_once_before_retirement() -> TestResult {
    let (gate, entered, release) = Gate::new();
    let mut registry = Registry::default();
    registry.package(Arc::new(Code(gate)))?;
    let actors = Actors {
        registry: Arc::new(registry),
        catalog: Arc::new(Mutex::new(Catalog::default())),
        request_admissions: Arc::new(tokio::sync::RwLock::new(())),
    };
    let request = wire::DeploymentRequest {
        account_id: vec![1],
        name: "scope".into(),
        code_sha256: vec![1; 32],
        idempotency_key: "deployment".into(),
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1000,
            memory_bytes: 1024,
            checkpoint_bytes: 1024,
        }),
        ..Default::default()
    };
    let invoking = actors.clone();
    let first = request.clone();
    let observer = tokio::spawn(async move { invoking.deploy(first, false).await });
    entered.await?;
    observer.abort();
    assert!(
        observer
            .await
            .err()
            .ok_or("observer completed early")?
            .is_cancelled()
    );
    release
        .send(())
        .map_err(|()| "deployment disappeared with its observer")?;
    let catalog = actors.catalog.lock().await;
    assert_eq!(catalog.namespaces.len(), 1);
    assert_eq!(catalog.receipts.len(), 1);
    drop(catalog);
    actors.deploy(request, false).await?;
    assert_eq!(actors.catalog.lock().await.namespaces.len(), 1);
    Ok(())
}
impl Fetch for Guest {
    fn fetch(
        &self,
        _: wire::InvokeActorRequest,
    ) -> BoxProviderFuture<'_, Result<wire::InvokeActorResponse, Status>> {
        Box::pin(async move {
            // The in-flight operation owns completion, so its destruction also
            // wakes the test. Keeping this sender in the fixture would hide
            // cancellation as an indefinitely outstanding completion.
            let complete = self
                .completed
                .lock()
                .await
                .take()
                .ok_or_else(|| failure("completed twice"))?;
            self.gate.wait().await?;
            complete
                .send(())
                .map_err(|()| failure("completion observer dropped"))?;
            Ok(wire::InvokeActorResponse::default())
        })
    }
    fn shutdown(&self) -> BoxProviderFuture<'_, Result<(), Status>> {
        Box::pin(async move {
            if let Some(stopped) = self.stopped.lock().await.take() {
                stopped
                    .send(())
                    .map_err(|()| failure("shutdown observer dropped"))?;
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn disconnected_fetch_requests_finish_on_the_original_guest() -> TestResult {
    let (gate, entered, release) = Gate::new();
    let (complete, completed) = oneshot::channel();
    let actors = Actors {
        registry: Arc::new(Registry::default()),
        catalog: Arc::new(Mutex::new(Catalog::default())),
        request_admissions: Arc::new(tokio::sync::RwLock::new(())),
    };
    actors.catalog.lock().await.namespaces.insert(
        Scope::new(vec![1], "scope".into())?,
        Arc::new(Deployment {
            request: wire::DeploymentRequest::default(),
            revision: 1,
            runtime: Instance::Fetch(Arc::new(Guest {
                gate,
                completed: Mutex::new(Some(complete)),
                stopped: Mutex::new(None),
            })),
            actors: Arc::new(Mutex::new(BTreeMap::new())),
        }),
    );
    let invoking = actors.clone();
    let observer = tokio::spawn(async move {
        invoking
            .invoke_actor(Request::new(wire::InvokeActorRequest {
                actor_id: "actor".into(),
                account_id: vec![1],
                namespace: Some("scope".into()),
                method: "GET".into(),
                url: "http://localhost/".into(),
                ..Default::default()
            }))
            .await
    });
    entered.await?;
    observer.abort();
    assert!(
        observer
            .await
            .err()
            .ok_or("observer completed early")?
            .is_cancelled()
    );
    let retiring = actors.request_admissions.clone().write_owned();
    tokio::pin!(retiring);
    assert!(
        futures::poll!(&mut retiring).is_pending(),
        "guest observer released the server lease"
    );
    release
        .send(())
        .map_err(|()| "guest disappeared with its observer")?;
    completed.await?;
    let _retired = retiring.await;
    Ok(())
}

#[tokio::test]
async fn guest_shutdown_precedes_lease_drain_and_transport_retirement() -> TestResult {
    let (gate, entered, release) = Gate::new();
    let (complete, completed) = oneshot::channel();
    let (stop_guest, mut guest_stopped) = oneshot::channel();
    let actors = Actors {
        registry: Arc::new(Registry::default()),
        catalog: Arc::new(Mutex::new(Catalog::default())),
        request_admissions: Arc::new(tokio::sync::RwLock::new(())),
    };
    actors.catalog.lock().await.namespaces.insert(
        Scope::new(vec![1], "scope".into())?,
        Arc::new(Deployment {
            request: wire::DeploymentRequest::default(),
            revision: 1,
            runtime: Instance::Fetch(Arc::new(Guest {
                gate,
                completed: Mutex::new(Some(complete)),
                stopped: Mutex::new(Some(stop_guest)),
            })),
            actors: Arc::new(Mutex::new(BTreeMap::new())),
        }),
    );
    let invoking = actors.clone();
    let observer = tokio::spawn(async move {
        invoking
            .invoke_actor(Request::new(wire::InvokeActorRequest {
                actor_id: "actor".into(),
                account_id: vec![1],
                namespace: Some("scope".into()),
                method: "GET".into(),
                url: "http://localhost/".into(),
                ..Default::default()
            }))
            .await
    });
    entered.await?;
    observer.abort();
    assert!(
        observer
            .await
            .err()
            .ok_or("observer completed early")?
            .is_cancelled()
    );
    let (stop_transport, mut transport_stopped) = oneshot::channel();
    let transport = acyclic_native_runtime::spawn(async { Ok(()) });
    let draining = Server::drain(actors.clone(), stop_transport, transport);
    tokio::pin!(draining);
    assert!(
        futures::poll!(&mut draining).is_pending(),
        "transport retired with an active guest request"
    );
    assert_eq!(
        guest_stopped.try_recv(),
        Ok(()),
        "lease drain blocked guest shutdown"
    );
    assert!(
        matches!(
            transport_stopped.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "effect transport retired before the request"
    );
    assert!(actors.catalog.lock().await.closing);
    release
        .send(())
        .map_err(|()| "guest disappeared during shutdown")?;
    draining.await?;
    completed.await?;
    assert!(matches!(
        transport_stopped.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
    Ok(())
}
