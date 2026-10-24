//! Stock harness artifact hosting and its transport-only file client.
use super::*;
use acyclic_harness::{
    Error, OperationId, Result, client as files,
    local::LocalHarness,
    package::harness::CommandSubscription,
    task_volume::{Component, FileValue, Query, TaskVolume},
};
use futures::{
    FutureExt,
    future::{BoxFuture, LocalBoxFuture, Shared},
};
use tonic::transport::Channel;

/// One stock package installation with explicit native resource bindings.
/// A native workspace belongs to exactly one account/namespace installation.
pub struct Harness {
    scope: Scope,
    digest: [u8; 32],
    host: Arc<Host>,
}
struct Host {
    local: Mutex<Option<Arc<LocalHarness>>>,
    subscriptions: Mutex<BTreeMap<OperationId, Completion>>,
    changed: tokio::sync::Notify,
}
type Completion = Shared<BoxFuture<'static, Settlement>>;
#[derive(Clone, Serialize, Deserialize)]
enum Settlement {
    Complete,
    Indeterminate(OperationId),
    Failed(String),
}
impl Settlement {
    fn from_result(result: Result<()>) -> Self {
        match result {
            Ok(()) => Self::Complete,
            Err(Error::Indeterminate(operation)) => Self::Indeterminate(operation),
            Err(error) => Self::Failed(error.to_string()),
        }
    }
    fn into_result(self) -> Result<()> {
        match self {
            Self::Complete => Ok(()),
            Self::Indeterminate(operation) => Err(Error::Indeterminate(operation)),
            Self::Failed(message) => Err(storage(message)),
        }
    }
}
impl Harness {
    /// Bind an already assembled stock Actors package. No client can substitute
    /// providers, credentials or workspace paths during deployment/invocation.
    pub fn new(scope: Scope, local: Arc<LocalHarness>) -> Result<Self> {
        Scope::new(scope.account.clone(), scope.namespace.clone()).map_err(storage)?;
        let code = acyclic_harness::package::harness::HarnessCode::new()?;
        let digest = *blake3::hash(
            &serde_json::to_vec(&code.actors().collect::<Vec<_>>()).map_err(storage)?,
        )
        .as_bytes();
        Ok(Self {
            scope,
            digest,
            host: Arc::new(Host {
                local: Mutex::new(Some(local)),
                subscriptions: Mutex::new(BTreeMap::new()),
                changed: tokio::sync::Notify::new(),
            }),
        })
    }
}
impl Package for Harness {
    fn digest(&self) -> [u8; 32] {
        self.digest
    }
    fn deploy(&self, scope: Scope) -> BoxProviderFuture<'_, std::result::Result<Instance, Status>> {
        Box::pin(async move {
            if scope != self.scope {
                return Err(Status::permission_denied(
                    "native resource binding belongs to another namespace",
                ));
            }
            Ok(Instance::Fetch(self.host.clone()))
        })
    }
}
impl Host {
    async fn settled(&self, operation: OperationId) -> std::result::Result<Settlement, Status> {
        let completion = loop {
            let changed = self.changed.notified();
            if let Some(completion) = self.subscriptions.lock().await.get(&operation).cloned() {
                break completion;
            }
            if self.local.lock().await.is_none() {
                return Err(Status::unavailable("package stopped"));
            }
            changed.await;
        };
        Ok(completion.await)
    }
}
impl Fetch for Host {
    fn fetch(
        &self,
        request: wire::InvokeActorRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<wire::InvokeActorResponse, Status>> {
        Box::pin(async move {
            let operation = OperationId::parse(&request.actor_id).map_err(invalid)?;
            if request.method == "GET" && request.url == "http://localhost/settlement" {
                let settlement = self.settled(operation).await?;
                return Ok(wire::InvokeActorResponse {
                    status: 200,
                    body: serde_json::to_vec(&settlement).map_err(failure)?,
                    ..Default::default()
                });
            }
            // Admission and shutdown have one owner. Shutdown cannot retire the
            // providers while a request is between durable write and subscription.
            let local = self.local.lock().await;
            let local = local
                .as_ref()
                .ok_or_else(|| Status::unavailable("package stopped"))?;
            let body = match request.method.as_str() {
                "GET" => serde_json::to_vec(local.task_files().inner.path()).map_err(failure)?,
                "POST" => {
                    let text = std::str::from_utf8(&request.body).map_err(invalid)?;
                    let file = files::parse_local_request(text, operation).map_err(invalid)?;
                    let generation = local
                        .task_files()
                        .inner
                        .snapshot(&selection(operation).map_err(invalid)?, 4096)
                        .await
                        .map_err(failure)?
                        .generation;
                    files::admit_request(local, operation, &file, generation)
                        .await
                        .map_err(failure)?;
                    self.subscriptions
                        .lock()
                        .await
                        .entry(operation)
                        .or_insert_with(|| {
                            let subscription = CommandSubscription::start(
                                local.clone(),
                                &acyclic_stream::runtime::PlatformExecutor,
                                operation,
                                true,
                            );
                            async move { Settlement::from_result(subscription.await) }
                                .boxed()
                                .shared()
                        });
                    self.changed.notify_waiters();
                    Vec::new()
                }
                _ => return Err(invalid("package fetch supports GET and POST")),
            };
            Ok(wire::InvokeActorResponse {
                status: 200,
                headers: vec![],
                body,
            })
        })
    }
    fn shutdown(&self) -> BoxProviderFuture<'_, std::result::Result<(), Status>> {
        Box::pin(async move {
            let local = self.local.lock().await.take();
            self.changed.notify_waiters();
            let subscriptions = std::mem::take(&mut *self.subscriptions.lock().await);
            let results = futures::future::join_all(subscriptions.into_values()).await;
            let cleanup = match local {
                Some(local) => files::disconnect_local(local).await.map_err(failure),
                None => Ok(()),
            };
            for result in results {
                result.into_result().map_err(failure)?;
            }
            cleanup
        })
    }
}

/// Canonical Actors request adapter. It never executes the harness locally.
#[derive(Clone)]
struct Actor {
    client: wire::actors_service_client::ActorsServiceClient<Channel>,
    scope: Scope,
}
impl Actor {
    async fn invoke(
        &self,
        operation: OperationId,
        method: &str,
        url: &str,
        body: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let response = self
            .client
            .clone()
            .invoke_actor(wire::InvokeActorRequest {
                actor_id: operation.to_string(),
                namespace: Some(self.scope.namespace.clone()),
                account_id: self.scope.account.clone(),
                method: method.into(),
                url: url.into(),
                body,
                ..Default::default()
            })
            .await
            .map_err(storage)?
            .into_inner();
        if response.status != 200 {
            return Err(storage("package fetch failed"));
        }
        Ok(response.body)
    }
}
struct Admission {
    actor: Actor,
    volume: Arc<TaskVolume<acyclic_stream::grpc::Client>>,
}
impl files::RequestAdmission for Admission {
    fn submit(&self, operation: OperationId, file: FileValue) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.actor
                .invoke(
                    operation,
                    "POST",
                    "http://localhost/",
                    files::jsonl::request(&file)?.into_bytes(),
                )
                .await?;
            Ok(())
        })
    }
    fn settled(&self, operation: OperationId) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let bytes = self
                .actor
                .invoke(operation, "GET", "http://localhost/settlement", vec![])
                .await?;
            serde_json::from_slice::<Settlement>(&bytes)
                .map_err(storage)?
                .into_result()
        })
    }
    fn drain_cut(&self, operation: OperationId) -> LocalBoxFuture<'_, Result<u64>> {
        Box::pin(async move {
            Ok(self
                .volume
                .snapshot(&selection(operation)?, 4096)
                .await
                .map_err(storage)?
                .cursor
                .position)
        })
    }
}
fn selection(operation: OperationId) -> Result<Query> {
    Ok(Query::TaskSession {
        task: Component::new(
            acyclic_harness::runtime::task_id_for_admission(operation).to_string(),
        )?,
        session: Component::new(operation.to_string())?,
    })
}
/// Open the shared headless state machine over Streams and Actors transports.
/// Namespace deployment is a host operation, never implicit client code loading.
pub async fn connect(
    endpoint: &str,
    scope: Scope,
    operation: OperationId,
) -> Result<files::FileHeadlessClient> {
    Scope::new(scope.account.clone(), scope.namespace.clone()).map_err(storage)?;
    let actor = Actor {
        client: wire::actors_service_client::ActorsServiceClient::new(
            client::channel(endpoint).await?,
        ),
        scope,
    };
    let path = serde_json::from_slice(
        &actor
            .invoke(operation, "GET", "http://localhost/", vec![])
            .await?,
    )
    .map_err(storage)?;
    let volume = Arc::new(
        TaskVolume::open_existing(Arc::new(client::streams(endpoint).await?), path)
            .await
            .map_err(storage)?,
    );
    let query = files::ClientQuery::open(
        &volume,
        selection(operation)?,
        None,
        files::QueryViewLimits {
            files: 4096,
            segments: 4096,
            bytes: 4 * 1024 * 1024,
        },
    )
    .await?;
    Ok(files::FileHeadlessClient::deployed(
        operation,
        query,
        Box::new(Admission { actor, volume }),
    ))
}
fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn settlement_wire_preserves_unsaved_indeterminate_identity() -> Result<()> {
        let operation = OperationId::new();
        let encoded = serde_json::to_vec(&Settlement::from_result(Err(Error::Indeterminate(
            operation,
        ))))
        .map_err(storage)?;
        let restored = serde_json::from_slice::<Settlement>(&encoded)
            .map_err(storage)?
            .into_result();
        assert!(matches!(restored, Err(Error::Indeterminate(actual)) if actual == operation));
        // Stringifying the owner error loses the completion class.
        let erased = Settlement::Failed(Error::Indeterminate(operation).to_string()).into_result();
        assert!(!matches!(erased, Err(Error::Indeterminate(_))));
        Ok(())
    }
}
