//! Deployed parity over actual SDK services, with no remote credentials.
use super::*;
use acyclic_fs_wire::wire::filesystem::v2 as fs;
use acyclic_harness::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
use acyclic_harness::resources::ProviderRef;
use acyclic_local_server::{Registry, Scope, Server, client};
use std::sync::OnceLock;
use tonic::{Status, transport::Channel};

type FsClient = fs::filesystem_service_client::FilesystemServiceClient<Channel>;

/// Immutable fixture publications using only the canonical filesystem API.
#[derive(Clone)]
pub struct HostedVolume {
    client: FsClient,
}
impl HostedVolume {
    /// Connect to the SDK's local filesystem service without bearer credentials.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        Ok(Self {
            client: FsClient::new(client::channel(endpoint).await?),
        })
    }
    async fn reference(&self, workspace: &fs::Workspace, bytes: &[u8]) -> Result<FileRef> {
        let reference = workspace
            .workspace
            .as_ref()
            .ok_or_else(|| codec("workspace reference missing"))?;
        let head = workspace
            .head
            .as_ref()
            .ok_or_else(|| codec("workspace head missing"))?;
        let volume = VolumeRef::new(
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            reference.name.clone(),
            VolumeClass::SessionShared,
            VolumeOwner::Session("conformance".into()),
            &mut None,
        )?;
        FileRef::new(
            volume,
            "payload.json".into(),
            hex::encode(&head.generation_id),
            FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
            "fixture.json".into(),
            &mut None,
        )
    }
    async fn read(&self, generation: fs::GenerationRef) -> std::result::Result<Vec<u8>, Status> {
        let mut client = self.client.clone();
        Ok(client
            .read(fs::ReadRequest {
                generation: Some(generation),
                path: "/payload.json".into(),
                range: None,
                maximum_bytes: 1_048_576,
            })
            .await?
            .into_inner()
            .contents)
    }
}
impl Volume for HostedVolume {
    fn put(&self, operation: OperationId, value: Value) -> BoxProviderFuture<'_, Result<FileRef>> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(&value).map_err(codec)?;
            let mut client = self.client.clone();
            let mut workspace = client
                .create_workspace(fs::CreateWorkspaceRequest {
                    name: format!("fixture-{operation}"),
                    profile: fs::FilesystemProfile::Portable as i32,
                    operation: Some(fs::OperationOptions {
                        idempotency_key: operation.into_bytes().to_vec(),
                    }),
                })
                .await
                .map_err(codec)?
                .into_inner()
                .workspace
                .ok_or_else(|| codec("missing workspace"))?;
            let reference = workspace
                .workspace
                .clone()
                .ok_or_else(|| codec("missing workspace identity"))?;
            let head = workspace
                .head
                .clone()
                .ok_or_else(|| codec("missing initial generation"))?;
            let mut hash = blake3::Hasher::new();
            hash.update(b"fixture-write:");
            hash.update(&operation.into_bytes());
            let id = hash
                .finalize()
                .as_bytes()
                .get(..16)
                .ok_or_else(|| codec("operation digest"))?
                .to_vec();
            let outcome = client
                .apply_transaction(fs::ApplyTransactionRequest {
                    base: Some(head),
                    mutations: vec![fs::Mutation {
                        mutation: Some(fs::mutation::Mutation::PutFile(fs::PutFile {
                            path: "/payload.json".into(),
                            contents: bytes.clone(),
                        })),
                    }],
                    operation: Some(fs::OperationOptions {
                        idempotency_key: id,
                    }),
                    maximum_conflicts: 8,
                })
                .await
                .map_err(codec)?
                .into_inner();
            if !matches!(
                fs::MutationStatus::try_from(outcome.status),
                Ok(fs::MutationStatus::Committed | fs::MutationStatus::AlreadyCommitted)
            ) {
                return Err(Error::Conflict("fixture publication conflict".into()));
            }
            workspace.workspace = Some(reference);
            workspace.head = outcome.generation;
            self.reference(&workspace, &bytes).await
        })
    }
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>> {
        Box::pin(async move {
            let mut client = self.client.clone();
            let workspace = client
                .open_workspace(fs::OpenWorkspaceRequest {
                    selector: Some(fs::open_workspace_request::Selector::Name(
                        file.volume().id().into(),
                    )),
                })
                .await
                .map_err(codec)?
                .into_inner()
                .workspace
                .ok_or_else(|| codec("missing workspace"))?;
            let bytes = self
                .read(fs::GenerationRef {
                    workspace: workspace.workspace,
                    generation_id: hex::decode(file.version()).map_err(codec)?,
                })
                .await
                .map_err(codec)?;
            if file.path() != "payload.json"
                || FileDescriptor::from_bytes(&bytes, "application/json", &mut None)?
                    != *file.descriptor()
            {
                return Err(Error::Conflict(
                    "immutable fixture reference differs".into(),
                ));
            }
            serde_json::from_slice(&bytes).map_err(codec)
        })
    }
    fn lookup(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<Option<FileRef>>> {
        Box::pin(async move {
            let mut client = self.client.clone();
            let response = match client
                .open_workspace(fs::OpenWorkspaceRequest {
                    selector: Some(fs::open_workspace_request::Selector::Name(format!(
                        "fixture-{operation}"
                    ))),
                })
                .await
            {
                Ok(response) => response.into_inner(),
                Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
                Err(error) => return Err(codec(error)),
            };
            let workspace = response
                .workspace
                .ok_or_else(|| codec("missing workspace"))?;
            let bytes = match self
                .read(
                    workspace
                        .head
                        .clone()
                        .ok_or_else(|| codec("missing head"))?,
                )
                .await
            {
                Ok(bytes) => bytes,
                Err(error) if error.code() == tonic::Code::NotFound => return Ok(None),
                Err(error) => return Err(codec(error)),
            };
            Ok(Some(self.reference(&workspace, &bytes).await?))
        })
    }
}

struct Factory<P, A, O> {
    endpoint: Arc<OnceLock<String>>,
    providers: Providers<P, A, O>,
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
            let mut bindings = fixture_bindings();
            bindings.streams = scope.resource_prefix();
            let volume = Arc::new(
                HostedVolume::connect(
                    self.endpoint
                        .get()
                        .ok_or_else(|| Status::failed_precondition("server not bound"))?,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
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
    server: Server,
}
impl Localhost {
    /// Starts one no-quorum server from SDK crates; no process or credential discovery.
    pub async fn start() -> TestResult<Self> {
        let directory = Arc::new(tempfile::tempdir()?);
        let stream = Arc::new(
            acyclic_stream::LocalStream::open_with_ownership_anchor(
                directory.path(),
                Default::default(),
                acyclic_native_runtime::OwnershipAnchor::new(directory.clone()),
            )
            .await?,
        );
        let filesystem = Fs::memory();
        let endpoint = Arc::new(OnceLock::new());
        let mut registry = Registry::default();
        registry.package(Arc::new(Factory {
            endpoint: endpoint.clone(),
            providers: Providers {
                stream: stream.clone(),
                filesystem: filesystem.clone(),
            },
        }))?;
        let server = Server::start(stream, filesystem, registry).await?;
        endpoint
            .set(server.endpoint())
            .map_err(|_| "endpoint already assigned")?;
        Ok(Self { server })
    }
    /// Canonical shared service endpoint.
    pub fn endpoint(&self) -> String {
        self.server.endpoint()
    }
    /// Drain requests and release the listener.
    pub async fn shutdown(self) -> TestResult {
        self.server.shutdown().await
    }
}
impl Substrate for Localhost {
    fn capabilities(&self) -> BTreeSet<String> {
        InProcess.capabilities()
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(async move {
            let package = HarnessPackage::new(fixture_bindings());
            let client = client::Client::connect(
                &self.endpoint(),
                Scope {
                    account: vec![1; 16],
                    namespace: format!("case-{}", AgentId::new()),
                },
                package.identity.digest,
                package.manifest().clone(),
            )
            .await?;
            Actors {
                runtime: client,
                volume: HostedVolume::connect(&self.endpoint()).await?,
                bindings: fixture_bindings(),
            }
            .execute(recipe, paths)
            .await
        })
    }
}
