//! Canonical Actors transport for the executable workflow package contract.
use super::*;
use acyclic_harness::{
    Error, Result,
    package::{ActorPackage, Manifest},
};
use tonic::transport::Channel;

/// Namespace-bound package client. Actor instance IDs are never global keys.
#[derive(Clone)]
pub struct Client {
    client: wire::actors_service_client::ActorsServiceClient<Channel>,
    scope: Scope,
    digest: [u8; 32],
    manifest: Manifest,
}
/// Connect only to a numeric loopback endpoint, with no credential callback.
pub async fn channel(endpoint: &str) -> Result<Channel> {
    acyclic_stream::grpc::loopback_endpoint(endpoint)
        .map_err(storage)?
        .connect()
        .await
        .map_err(storage)
}
/// Streams client for this loopback-only qualification server. The credential
/// is the SDK's public fixture capability, never a caller's login or secret.
pub async fn streams(endpoint: &str) -> Result<acyclic_stream::grpc::Client> {
    let stream = acyclic_stream::grpc::Client::connect_loopback(endpoint)
        .await
        .map_err(storage)?;
    stream
        .session_client()
        .map_err(storage)?
        .register_credential(0, acyclic_stream::session::testing::credential())
        .map_err(storage)?;
    Ok(stream)
}
impl Client {
    /// Select one compiled artifact and explicit account/namespace binding.
    pub async fn connect(
        endpoint: &str,
        scope: Scope,
        digest: [u8; 32],
        manifest: Manifest,
    ) -> Result<Self> {
        Scope::new(scope.account.clone(), scope.namespace.clone()).map_err(storage)?;
        Ok(Self {
            client: wire::actors_service_client::ActorsServiceClient::new(channel(endpoint).await?),
            scope,
            digest,
            manifest,
        })
    }
    /// Selects another namespace on the same transport and compiled package.
    pub fn for_scope(&self, scope: Scope) -> Result<Self> {
        let scope = Scope::new(scope.account, scope.namespace).map_err(storage)?;
        Ok(Self {
            scope,
            ..self.clone()
        })
    }
    async fn invoke(&self, call: Call) -> Result<Snapshot> {
        let mut client = self.client.clone();
        let response = client
            .invoke_actor(wire::InvokeActorRequest {
                actor_id: call.placement().actor.to_string(),
                namespace: Some(self.scope.namespace.clone()),
                account_id: self.scope.account.clone(),
                method: "POST".into(),
                url: "http://localhost/package".into(),
                body: serde_json::to_vec(&call).map_err(storage)?,
                ..Default::default()
            })
            .await
            .map_err(rpc_error)?
            .into_inner();
        if response.status != 200 {
            return Err(storage("package invocation failed"));
        }
        serde_json::from_slice(&response.body).map_err(storage)
    }
}
impl ActorRuntime for Client {
    fn install(&self, package: Arc<dyn ActorPackage>) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            if package.manifest() != &self.manifest {
                return Err(Error::Conflict("compiled package manifest differs".into()));
            }
            let mut client = self.client.clone();
            client
                .deploy_namespace(wire::DeploymentRequest {
                    name: self.scope.namespace.clone(),
                    account_id: self.scope.account.clone(),
                    code_sha256: self.digest.to_vec(),
                    limits: Some(wire::ActorLimits {
                        handler_timeout_millis: 1000,
                        memory_bytes: 8 * 1024 * 1024,
                        checkpoint_bytes: 8 * 1024 * 1024,
                    }),
                    idempotency_key: format!("package:{}", self.scope.namespace),
                    ..Default::default()
                })
                .await
                .map_err(storage)?;
            Ok(())
        })
    }
    fn place(
        &self,
        placement: Placement,
        machine: MachineIdentity,
        input: Value,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(self.invoke(Call::Place {
            placement,
            machine,
            input,
        }))
    }
    fn deliver(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(self.invoke(Call::Deliver { placement, event }))
    }
    fn recover(&self, placement: Placement) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(self.invoke(Call::Recover { placement }))
    }
    fn cancel(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(self.invoke(Call::Cancel { placement, event }))
    }
}
fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
fn rpc_error(error: Status) -> Error {
    let message = error.message().to_owned();
    match error.code() {
        tonic::Code::Aborted => Error::Conflict(message),
        tonic::Code::InvalidArgument => Error::Invalid(message),
        tonic::Code::PermissionDenied => Error::Unauthorized(message),
        tonic::Code::NotFound => Error::NotFound(message),
        tonic::Code::Unimplemented => Error::Unsupported(message),
        _ => storage(error),
    }
}
