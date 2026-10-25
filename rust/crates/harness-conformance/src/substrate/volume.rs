//! One immutable fixture publication path over direct or socket filesystem APIs.
use super::*;
use acyclic_fs_wire::wire::filesystem::v2 as fs;
use acyclic_harness::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
use acyclic_harness::resources::ProviderRef;
use acyclic_local_server::client;
use fs::filesystem_service_server::FilesystemService;
use tonic::{Status, transport::Channel};

type FsClient = fs::filesystem_service_client::FilesystemServiceClient<Channel>;

#[derive(Clone)]
enum Connection {
    Local(
        Arc<
            acyclic_fs::FilesystemWireService<
                acyclic_fs::MemoryAuthorityBackend,
                acyclic_fs::MemoryObjectBackend,
            >,
        >,
    ),
    Remote(FsClient),
}
impl Connection {
    async fn create_workspace(
        &mut self,
        request: fs::CreateWorkspaceRequest,
    ) -> std::result::Result<tonic::Response<fs::WorkspaceResponse>, Status> {
        match self {
            Self::Local(service) => service.create_workspace(tonic::Request::new(request)).await,
            Self::Remote(client) => client.create_workspace(request).await,
        }
    }
    async fn open_workspace(
        &mut self,
        request: fs::OpenWorkspaceRequest,
    ) -> std::result::Result<tonic::Response<fs::WorkspaceResponse>, Status> {
        match self {
            Self::Local(service) => service.open_workspace(tonic::Request::new(request)).await,
            Self::Remote(client) => client.open_workspace(request).await,
        }
    }
    async fn apply_transaction(
        &mut self,
        request: fs::ApplyTransactionRequest,
    ) -> std::result::Result<tonic::Response<fs::MutationResponse>, Status> {
        match self {
            Self::Local(service) => {
                service
                    .apply_transaction(tonic::Request::new(request))
                    .await
            }
            Self::Remote(client) => client.apply_transaction(request).await,
        }
    }
    async fn read(
        &mut self,
        request: fs::ReadRequest,
    ) -> std::result::Result<tonic::Response<fs::ReadResponse>, Status> {
        match self {
            Self::Local(service) => service.read(tonic::Request::new(request)).await,
            Self::Remote(client) => client.read(request).await,
        }
    }
}

/// Immutable fixture publications using only the canonical filesystem API.
#[derive(Clone)]
pub struct FixtureVolume {
    client: Connection,
}
impl FixtureVolume {
    /// Uses the same wire service in process, without a second storage algorithm.
    pub fn in_process(
        filesystem: Fs<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>,
    ) -> Result<Self> {
        Ok(Self {
            client: Connection::Local(Arc::new(
                acyclic_fs::FilesystemWireService::new(filesystem, Default::default())
                    .map_err(codec)?,
            )),
        })
    }
    /// Connect to the SDK's local filesystem service without bearer credentials.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        Ok(Self {
            client: Connection::Remote(FsClient::new(client::channel(endpoint).await?)),
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
impl Volume for FixtureVolume {
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
            let file = self.reference(&workspace, &bytes).await?;
            #[cfg(test)]
            acyclic_test_support::differential::record(FixturePublication(
                file.volume().id().into(),
            ));
            Ok(file)
        })
    }
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>> {
        Box::pin(async move {
            let mut client = self.client.clone();
            let mut workspace = client
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
            let generation = fs::GenerationRef {
                workspace: workspace.workspace.clone(),
                generation_id: hex::decode(file.version()).map_err(codec)?,
            };
            let bytes = self.read(generation.clone()).await.map_err(codec)?;
            // Opening resolves the workspace identity, not the requested cut.
            // The immutable read and binding must use that same pinned cut even
            // if another publication has advanced the workspace's current head.
            workspace.head = Some(generation);
            // The immutable descriptor is only part of the publication binding.
            // A different provider, owner or volume class must not resolve as
            // this fixture merely because it names the same workspace bytes.
            if file != self.reference(&workspace, &bytes).await? {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn both_connections_share_immutable_publications_and_reject_forgery() -> TestResult {
        let filesystem = Fs::memory();
        let local = FixtureVolume::in_process(filesystem.clone())?;
        let server = acyclic_local_server::Server::start(
            Arc::new(acyclic_stream::MemoryStream::default()),
            filesystem,
            acyclic_local_server::Registry::default(),
        )
        .await?;
        let remote = FixtureVolume::connect(&server.endpoint()).await?;
        for (writer, reader) in [(&local, &remote), (&remote, &local)] {
            let operation = OperationId::new();
            assert!(reader.lookup(operation).await?.is_none());
            let value = json!({"immutable": ["both", "connections"]});
            let file = writer.put(operation, value.clone()).await?;
            assert_eq!(reader.lookup(operation).await?, Some(file.clone()));
            assert_eq!(reader.get(file.clone()).await?, value);
            assert_eq!(writer.put(operation, value.clone()).await?, file);
            for adapter in [&local, &remote] {
                let forged = FileRef::new(
                    file.volume().clone(),
                    file.path().into(),
                    file.version().into(),
                    FileDescriptor::from_bytes(b"foreign", "application/json", &mut None)?,
                    "fixture.json".into(),
                    &mut None,
                )?;
                assert!(adapter.get(forged).await.is_err());
                for (provider, class, owner) in [
                    (
                        "foreign",
                        VolumeClass::SessionShared,
                        VolumeOwner::Session("conformance".into()),
                    ),
                    (
                        "acyclic",
                        VolumeClass::SessionShared,
                        VolumeOwner::Session("foreign".into()),
                    ),
                    (
                        "acyclic",
                        VolumeClass::Project,
                        VolumeOwner::Project("conformance".into()),
                    ),
                ] {
                    let foreign_volume = VolumeRef::new(
                        ProviderRef::new(
                            provider.into(),
                            "filesystem".into(),
                            "2".into(),
                            &mut None,
                        )?,
                        file.volume().id().into(),
                        class,
                        owner,
                        &mut None,
                    )?;
                    let forged = FileRef::new(
                        foreign_volume,
                        file.path().into(),
                        file.version().into(),
                        file.descriptor().clone(),
                        "fixture.json".into(),
                        &mut None,
                    )?;
                    assert!(adapter.get(forged).await.is_err());
                }
            }
            advance_head(writer, &file).await?;
            for adapter in [&local, &remote] {
                assert_eq!(adapter.get(file.clone()).await?, value);
            }
        }
        server.shutdown().await?;
        Ok(())
    }

    async fn advance_head(volume: &FixtureVolume, file: &FileRef) -> TestResult {
        let mut client = volume.client.clone();
        let workspace = client
            .open_workspace(fs::OpenWorkspaceRequest {
                selector: Some(fs::open_workspace_request::Selector::Name(
                    file.volume().id().into(),
                )),
            })
            .await?
            .into_inner()
            .workspace
            .ok_or("workspace missing")?;
        let outcome = client
            .apply_transaction(fs::ApplyTransactionRequest {
                base: workspace.head,
                mutations: vec![fs::Mutation {
                    mutation: Some(fs::mutation::Mutation::PutFile(fs::PutFile {
                        path: "/payload.json".into(),
                        contents: br#"{"later":true}"#.to_vec(),
                    })),
                }],
                operation: Some(fs::OperationOptions {
                    idempotency_key: OperationId::new().into_bytes().to_vec(),
                }),
                maximum_conflicts: 8,
            })
            .await?
            .into_inner();
        assert_eq!(outcome.status, fs::MutationStatus::Committed as i32);
        Ok(())
    }
}
