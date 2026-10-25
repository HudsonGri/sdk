//! Deployed parity over actual SDK services, with no remote credentials.
use super::volume::FixtureVolume;
use super::*;
use acyclic_local_server::{Registry, Scope, Server, client};
use std::sync::OnceLock;
use tonic::Status;

struct Factory<P, A, O> {
    volume: Arc<OnceLock<FixtureVolume>>,
    providers: Providers<P, A, O>,
    #[cfg(test)]
    deployments: Arc<std::sync::atomic::AtomicUsize>,
}
impl<P, A, O> acyclic_local_server::Package for Factory<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn digest(&self) -> [u8; 32] {
        HarnessPackage::new(fixture_bindings()).identity.digest
    }
    fn deploy(
        &self,
        scope: Scope,
    ) -> BoxProviderFuture<'_, std::result::Result<acyclic_local_server::Instance, Status>> {
        Box::pin(async move {
            #[cfg(test)]
            self.deployments
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let mut bindings = fixture_bindings();
            bindings.streams = scope.resource_prefix();
            let volume = Arc::new(
                self.volume
                    .get()
                    .ok_or_else(|| Status::failed_precondition("server not bound"))?
                    .clone(),
            );
            let runtime = InProcessRuntime::new(
                self.providers.stream.clone(),
                bindings.clone(),
                Arc::new(FixturePolicy),
                Arc::new(FixtureEffects::new(
                    volume,
                    Arc::new(Providers {
                        stream: self.providers.stream.clone(),
                        filesystem: self.providers.filesystem.clone(),
                    }),
                    &bindings,
                )),
            );
            runtime
                .install(Arc::new(HarnessPackage::new(bindings)))
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(acyclic_local_server::Instance::Workflow(Arc::new(
                Supervised::new(runtime),
            )))
        })
    }
}

/// A running SDK localhost deployment, owned for the entire corpus traversal.
pub struct Localhost {
    server: Option<Server>,
    endpoint: String,
    client: client::Client,
    volume: FixtureVolume,
    #[cfg(test)]
    deployments: Option<Arc<std::sync::atomic::AtomicUsize>>,
}
impl Localhost {
    /// Starts one no-quorum server from SDK crates; no process or credential discovery.
    pub async fn start() -> TestResult<Self> {
        let providers = LocalProviders::local().await?;
        let stream = providers.stream;
        let filesystem = providers.filesystem;
        let volume = Arc::new(OnceLock::new());
        #[cfg(test)]
        let deployments = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.package(Arc::new(Factory {
            volume: volume.clone(),
            providers: Providers {
                stream: stream.clone(),
                filesystem: filesystem.clone(),
            },
            #[cfg(test)]
            deployments: deployments.clone(),
        }))?;
        let server = Server::start(stream, filesystem, registry).await?;
        let hosted = FixtureVolume::connect(&server.endpoint()).await?;
        volume
            .set(hosted.clone())
            .map_err(|_| "volume already assigned")?;
        let mut host = Self::connect_with_volume(&server.endpoint(), hosted).await?;
        host.server = Some(server);
        #[cfg(test)]
        {
            host.deployments = Some(deployments);
        }
        Ok(host)
    }
    /// Attaches through the same canonical Actors/fs clients to an SDK host
    /// that has registered the pinned conformance package.
    pub async fn connect(endpoint: &str) -> TestResult<Self> {
        Self::connect_with_volume(endpoint, FixtureVolume::connect(endpoint).await?).await
    }
    async fn connect_with_volume(endpoint: &str, volume: FixtureVolume) -> TestResult<Self> {
        let package = HarnessPackage::new(fixture_bindings());
        let client = client::Client::connect(
            endpoint,
            Scope {
                account: vec![1; 16],
                namespace: "conformance".into(),
            },
            package.identity.digest,
            package.manifest().clone(),
        )
        .await?;
        Ok(Self {
            server: None,
            endpoint: endpoint.to_owned(),
            client,
            volume,
            #[cfg(test)]
            deployments: None,
        })
    }
    #[cfg(test)]
    pub(super) fn deployment_count(&self) -> TestResult<usize> {
        Ok(self
            .deployments
            .as_ref()
            .ok_or("host has no deployment observer")?
            .load(std::sync::atomic::Ordering::Acquire))
    }
    /// Canonical shared service endpoint.
    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }
    /// Drain requests and release the listener.
    pub async fn shutdown(self) -> TestResult {
        if let Some(server) = self.server {
            server.shutdown().await?;
        }
        Ok(())
    }
}
impl Substrate for Localhost {
    fn capabilities(&self) -> BTreeSet<String> {
        InProcess::default().capabilities()
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(async move {
            Actors {
                runtime: self.client.clone(),
                volume: self.volume.clone(),
                bindings: fixture_bindings(),
            }
            .execute(recipe, paths)
            .await
        })
    }
}
