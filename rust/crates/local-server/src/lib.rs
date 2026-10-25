//! Credential-free SDK services bound exclusively to a numeric loopback address.
//!
//! Code is registered by digest by the embedding application. Namespace and
//! Worker deployment select those exact compiled artifacts; this is not a JS
//! interpreter, cloud emulator, quorum, or a harness-specific transport.

use acyclic_actors::wire;
use acyclic_harness::{
    package::{ActorRuntime, Event, Placement, Snapshot},
    workflow::MachineIdentity,
};
use acyclic_native_runtime::TaskHandle;
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, oneshot},
};
use tonic::{Request, Response, Status};

mod actors;
// Custom incoming streams bypass tonic Server's accepted-socket settings.
// All services share this one transport adapter and its immediate emission.
fn incoming(listener: TcpListener) -> tonic::transport::server::TcpIncoming {
    tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true))
}
pub mod client;
#[cfg(feature = "native-harness")]
pub mod harness;

/// Complete, validated namespace identity. No ambient account or namespace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Scope {
    /// Opaque account identity.
    pub account: Vec<u8>,
    /// Namespace or Worker deployment name.
    pub namespace: String,
}
impl Scope {
    fn new(account: Vec<u8>, namespace: String) -> Result<Self, Status> {
        acyclic_actors::addressing::AccountId::new(&account).map_err(invalid)?;
        acyclic_actors::addressing::NamespaceId::for_deployment(namespace.as_bytes())
            .map_err(invalid)?;
        Ok(Self { account, namespace })
    }
    /// Collision-free resource prefix derived from the full namespace identity.
    pub fn resource_prefix(&self) -> String {
        let mut hash = blake3::Hasher::new();
        hash.update(&(self.account.len() as u64).to_le_bytes());
        hash.update(&self.account);
        hash.update(self.namespace.as_bytes());
        format!("local/{}", hash.finalize().to_hex())
    }
}

/// A compiled Actors package factory. Each deployment owns a separate runtime.
pub trait Package: Send + Sync {
    /// Exact content digest used by the canonical deployment API.
    fn digest(&self) -> [u8; 32];
    /// Install the pinned package with namespace-scoped resource bindings.
    fn deploy(&self, scope: Scope) -> BoxProviderFuture<'_, Result<Instance, Status>>;
}

/// Runtime-selected execution ABI for a compiled package.
#[derive(Clone)]
pub enum Instance {
    /// The deterministic checkpoint/outbox package contract.
    Workflow(Arc<dyn ActorRuntime>),
    /// The guest's HTTP fetch entry point, including native stock task packages.
    Fetch(Arc<dyn Fetch>),
}
/// Generic fetch hook; the service never interprets application request bodies.
pub trait Fetch: Send + Sync {
    /// Execute one admitted request at its complete actor address.
    fn fetch(
        &self,
        request: wire::InvokeActorRequest,
    ) -> BoxProviderFuture<'_, Result<wire::InvokeActorResponse, Status>>;
    /// Stop new admissions and drain application-owned work before server exit.
    fn shutdown(&self) -> BoxProviderFuture<'_, Result<(), Status>>;
}

/// A compiled stateless Worker artifact, addressed through the canonical API.
pub trait Worker: Send + Sync {
    /// Exact artifact content digest.
    fn digest(&self) -> [u8; 32];
    /// Execute a request with the explicit deployment scope.
    fn fetch(
        &self,
        scope: Scope,
        request: wire::InvokeWorkerRequest,
    ) -> BoxProviderFuture<'_, Result<wire::InvokeActorResponse, Status>>;
}

/// Workflow package invocation body, carried by the public Actors Invoke API.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Call {
    /// Initial checkpoint with its exact handler identity.
    Place {
        /// Proposed owner and epoch.
        placement: Placement,
        /// Pinned handler in the compiled artifact.
        machine: MachineIdentity,
        /// Immutable initialization value.
        input: Value,
    },
    /// One idempotent inbox admission.
    Deliver {
        /// Owner attempting admission.
        placement: Placement,
        /// Immutable inbox record and retry identity.
        event: Event,
    },
    /// Reconcile already committed work.
    Recover {
        /// Owner whose committed history is reconstructed.
        placement: Placement,
    },
    /// Fence and drain before publishing cancellation.
    Cancel {
        /// Owner whose effects must be fenced.
        placement: Placement,
        /// Durable cancellation identity.
        event: Event,
    },
}
impl Call {
    fn placement(&self) -> &Placement {
        match self {
            Self::Place { placement, .. }
            | Self::Deliver { placement, .. }
            | Self::Recover { placement }
            | Self::Cancel { placement, .. } => placement,
        }
    }
    async fn apply(self, runtime: &dyn ActorRuntime) -> acyclic_harness::Result<Snapshot> {
        match self {
            Self::Place {
                placement,
                machine,
                input,
            } => runtime.place(placement, machine, input).await,
            Self::Deliver { placement, event } => runtime.deliver(placement, event).await,
            Self::Recover { placement } => runtime.recover(placement).await,
            Self::Cancel { placement, event } => runtime.cancel(placement, event).await,
        }
    }
}

struct Deployment {
    request: wire::DeploymentRequest,
    revision: u64,
    runtime: Instance,
    actors: Arc<Mutex<BTreeMap<String, Snapshot>>>,
}
struct DeploymentLease {
    deployment: Arc<Deployment>,
    _active: tokio::sync::OwnedRwLockReadGuard<()>,
}
impl std::ops::Deref for DeploymentLease {
    type Target = Deployment;
    fn deref(&self) -> &Self::Target {
        &self.deployment
    }
}
#[derive(Default)]
struct Catalog {
    closing: bool,
    namespaces: BTreeMap<Scope, Arc<Deployment>>,
    workers: BTreeMap<Scope, wire::DeploymentRequest>,
    receipts: BTreeMap<(Scope, &'static str, String), Receipt>,
}
struct Receipt {
    request: Vec<u8>,
    response: Vec<u8>,
}
impl Catalog {
    // Mutation and receipt publish under the same catalog lock. An old delete
    // or deployment cannot be applied again after an address has been reused.
    fn replay<R: prost::Message + Default>(
        &self,
        scope: &Scope,
        method: &'static str,
        key: &str,
        request: &[u8],
    ) -> Result<Option<R>, Status> {
        let Some(receipt) = self.receipts.get(&(scope.clone(), method, key.into())) else {
            return Ok(None);
        };
        if receipt.request != request {
            return Err(Status::already_exists(
                "idempotency key reused with a different mutation",
            ));
        }
        R::decode(receipt.response.as_slice())
            .map(Some)
            .map_err(failure)
    }
    fn remember<R: prost::Message>(
        &mut self,
        scope: Scope,
        method: &'static str,
        key: String,
        request: Vec<u8>,
        response: &R,
    ) {
        self.receipts.insert(
            (scope, method, key),
            Receipt {
                request,
                response: response.encode_to_vec(),
            },
        );
    }
}
/// Immutable artifact registry. Deployment cannot replace code behind a digest.
#[derive(Default)]
pub struct Registry {
    packages: BTreeMap<[u8; 32], Arc<dyn Package>>,
    workers: BTreeMap<[u8; 32], Arc<dyn Worker>>,
}
impl Registry {
    /// Register a compiled package, rejecting duplicate identities.
    pub fn package(&mut self, code: Arc<dyn Package>) -> Result<(), Status> {
        if self.packages.contains_key(&code.digest()) {
            return Err(Status::already_exists("package digest"));
        }
        self.packages.insert(code.digest(), code);
        Ok(())
    }
    /// Register a compiled Worker, rejecting duplicate identities.
    pub fn worker(&mut self, code: Arc<dyn Worker>) -> Result<(), Status> {
        if self.workers.contains_key(&code.digest()) {
            return Err(Status::already_exists("Worker digest"));
        }
        self.workers.insert(code.digest(), code);
        Ok(())
    }
}

#[derive(Clone)]
struct Actors {
    registry: Arc<Registry>,
    catalog: Arc<Mutex<Catalog>>,
    request_admissions: Arc<tokio::sync::RwLock<()>>,
}

/// Owns admission to the listener. Explicit shutdown, a dropped shutdown waiter,
/// and dropping the server all request the same independently owned drain.
pub struct Server {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    finished: oneshot::Receiver<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
}
impl Server {
    /// Bind one ephemeral loopback port and serve all three canonical services.
    pub async fn start<P, A, O>(
        stream: Arc<P>,
        filesystem: acyclic_fs::Fs<A, O>,
        registry: Registry,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        P: StreamProvider,
        A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
        O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
    {
        Self::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            stream,
            filesystem,
            registry,
        )
        .await
    }
    /// A non-loopback bind is rejected before a socket is created.
    pub async fn bind<P, A, O>(
        address: SocketAddr,
        stream: Arc<P>,
        filesystem: acyclic_fs::Fs<A, O>,
        registry: Registry,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        P: StreamProvider,
        A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
        O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
    {
        if !address.ip().is_loopback() {
            return Err("local server requires a loopback address".into());
        }
        let listener = TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let (transport_stop, transport_stopped) = oneshot::channel();
        let actors = Actors {
            registry: Arc::new(registry),
            catalog: Arc::new(Mutex::new(Catalog::default())),
            request_admissions: Arc::new(tokio::sync::RwLock::new(())),
        };
        let filesystem = acyclic_fs::FilesystemWireService::new(filesystem, Default::default())?;
        let task = acyclic_native_runtime::spawn(tonic::transport::Server::builder()
            .add_service(acyclic_stream::wire::stream_service_server::StreamServiceServer::new(acyclic_stream::grpc::Service::new(stream.clone())))
            .add_service(acyclic_stream::wire::stream_session_service_server::StreamSessionServiceServer::new(acyclic_stream::session::Service::new(
                stream, acyclic_stream::session::testing::authority(), "local".into(), Default::default())))
            .add_service(acyclic_fs_wire::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer::new(filesystem))
            .add_service(wire::actors_service_server::ActorsServiceServer::new(actors.clone()))
            .serve_with_incoming_shutdown(incoming(listener), async { let _ = transport_stopped.await; }));
        let (stop, stopped) = oneshot::channel();
        let (complete, finished) = oneshot::channel();
        // Install the retirement owner before publishing the server. The
        // request sender's destruction is itself the shutdown request, so no
        // destructor needs to allocate, spawn or abort in-flight package work.
        acyclic_native_runtime::spawn(async move {
            let _ = stopped.await;
            let result = Self::drain(actors, transport_stop, task).await;
            let _ = complete.send(result);
        });
        Ok(Self {
            address,
            stop: Some(stop),
            finished,
        })
    }
    /// Endpoint shared by Streams, filesystem and Actors.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }
    /// Stop admission and await all transport-owned work.
    pub async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.stop.take();
        self.finished.await?
    }
    async fn drain(
        actors: Actors,
        stop: oneshot::Sender<()>,
        task: TaskHandle<Result<(), tonic::transport::Error>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let guests: Vec<_> = {
            let mut catalog = actors.catalog.lock().await;
            catalog.closing = true;
            catalog.namespaces.values().cloned().collect()
        };
        // Wake guest-owned waiters before waiting for their request leases.
        // In particular a settlement request can wait for application shutdown
        // to establish that no further task admission will arrive. Both guest
        // shutdown and those requests retain the shared effect transports.
        let stopped = futures::future::join_all(guests.iter().filter_map(|deployment| {
            match &deployment.runtime {
                Instance::Fetch(fetch) => Some(fetch.shutdown()),
                Instance::Workflow(_) => None,
            }
        }))
        .await;
        // Every admitted RPC retains a lease through its response/snapshot.
        // The closed catalog makes the actor set complete, even when a first
        // placement was still in flight when shutdown was requested.
        let _settled = actors.request_admissions.write().await;
        let results = futures::future::join_all(guests.iter().map(|deployment| async move {
            match &deployment.runtime {
                Instance::Fetch(_) => Ok(()),
                Instance::Workflow(runtime) => {
                    let placements: Vec<_> = deployment
                        .actors
                        .lock()
                        .await
                        .values()
                        .map(|snapshot| snapshot.placement.clone())
                        .collect();
                    let results = futures::future::join_all(
                        placements
                            .into_iter()
                            .map(|placement| runtime.recover(placement)),
                    )
                    .await;
                    for result in results {
                        result.map_err(runtime_error)?;
                    }
                    Ok(())
                }
            }
        }))
        .await;
        drop(stop);
        task.await??;
        for result in stopped.into_iter().chain(results) {
            result?;
        }
        Ok(())
    }
}
fn invalid(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}
fn failure(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}
fn runtime_error(error: acyclic_harness::Error) -> Status {
    use acyclic_harness::Error;
    match error {
        Error::Conflict(message) => Status::aborted(message),
        Error::Invalid(message) => Status::invalid_argument(message),
        Error::Unauthorized(message) => Status::permission_denied(message),
        Error::NotFound(message) => Status::not_found(message),
        Error::Unsupported(message) => Status::unimplemented(message),
        error => failure(error),
    }
}
