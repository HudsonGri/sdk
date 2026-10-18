use super::*;
use acyclic_fs::{
    AppendOutcome, AuthorityAllocation, AuthorityError, AuthorityGuard, AuthorityId,
    AuthorityStoreError, AuthorityWork, CreateAuthorityOutcome, DurableCommit,
    EmbeddedCapabilities, Epoch, GuardedAppend, Head, MemoryAuthorityBackend, OperationId,
    ReplayLimit, Sequence,
};
use std::sync::Mutex;
use tokio::sync::oneshot;

thread_local! {
    static RESTORE_CONFLICT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn restore_conflict() -> bool {
    RESTORE_CONFLICT.get()
}

struct Barrier {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

struct ScheduledAuthority<A> {
    inner: A,
    // Fixture schedule only: release the slot lock before any provider call/wait.
    barrier: Arc<Mutex<Option<Barrier>>>,
}

impl<A: AsyncAuthorityStore> AsyncAuthorityStore for ScheduledAuthority<A> {
    async fn create_authority<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> std::result::Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        self.inner
            .create_authority(authority, epoch, work, guard)
            .await
    }

    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> std::result::Result<Head, AuthorityStoreError> {
        self.inner.head(authority, work).await
    }

    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> std::result::Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        self.inner.replay(authority, after, limit, work).await
    }

    async fn compare_and_append<G: AuthorityGuard>(
        &self,
        request: GuardedAppend,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> std::result::Result<AppendOutcome, AuthorityError<G::Error>> {
        let barrier = self.barrier.lock().unwrap().take();
        if let Some(barrier) = barrier {
            barrier.entered.send(()).unwrap();
            barrier.release.await.unwrap();
        }
        self.inner.compare_and_append(request, work, guard).await
    }

    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> std::result::Result<Option<DurableCommit>, AuthorityStoreError> {
        self.inner.find_operation(authority, operation, work).await
    }
}

type Host<A> = FilesystemHost<ScheduledAuthority<A>, acyclic_fs::MemoryObjectBackend>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Batched,
    Raw,
    Measured,
    Native,
}

async fn publish<A: AsyncAuthorityStore>(
    host: &Host<A>,
    workspace: &WorkspaceRef,
    expected: Option<&GenerationRef>,
    mutations: &[WorkspaceMutation],
    key: &IdempotencyKey,
    mode: Mode,
) -> Result<GenerationRef> {
    if matches!(mode, Mode::Raw) {
        host.apply(workspace, expected, mutations, key).await
    } else {
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = match mode {
            Mode::Measured => Some(PreparationIo::from_budget(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )?),
            Mode::Native => Some(PreparationIo::from_native_work(
                &acyclic_fs::NativeMaterializationWork::new(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .unwrap(),
            )?),
            Mode::Batched | Mode::Raw => None,
        };
        let generation = host
            .apply_generation(
                workspace,
                expected,
                mutations,
                key.clone(),
                io.as_ref(),
                None,
            )?
            .await?;
        host.generation_ref_id(generation.id(), None)
    }
}

async fn schedule<A: AsyncAuthorityStore>(inner: A, mode: Mode, conditional: bool) -> Result<()> {
    let name = format!("publication-cut-{mode:?}-{conditional}");
    let barrier = Arc::new(Mutex::new(None));
    let authority = ScheduledAuthority {
        inner,
        barrier: barrier.clone(),
    };
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let filesystem = Fs::new(
        authority,
        acyclic_fs::LogicalObjectStore::new(Arc::new(objects), bucket),
        EmbeddedCapabilities::MEMORY,
    );
    let opened = filesystem
        .create_workspace(
            &name,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| map_error(failure.error))?
        .value;
    let initial = opened
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| map_error(failure.error))?
        .value;
    let provider = ProviderRef::new("cut".into(), "filesystem".into(), "1".into(), &mut None)?;
    let workspace = workspace_ref(provider.clone(), &name)?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let expected = host.generation_ref_id(initial.id(), None)?;
    let (entered, entering) = oneshot::channel();
    let (release, released) = oneshot::channel();
    *barrier.lock().unwrap() = Some(Barrier {
        entered,
        release: released,
    });
    let left = [
        WorkspaceMutation::CreateDirectory {
            path: "/shared".into(),
        },
        WorkspaceMutation::PutFile {
            path: "/shared/left".into(),
            bytes: b"left".as_slice().into(),
        },
    ];
    let right = [
        WorkspaceMutation::CreateDirectory {
            path: "/shared".into(),
        },
        WorkspaceMutation::PutFile {
            path: "/shared/right".into(),
            bytes: b"right".as_slice().into(),
        },
    ];
    let first_key = IdempotencyKey::new("left")?;
    let first = publish(
        &host,
        &workspace,
        conditional.then_some(&expected),
        &left,
        &first_key,
        mode,
    );
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("publication finished before its scheduled append: {result:?}"),
        entered = entering => entered.unwrap(),
    }
    let second = publish(
        &host,
        &workspace,
        None,
        &right,
        &IdempotencyKey::new("right")?,
        mode,
    )
    .await;
    release.send(()).unwrap();
    let first = first.await;
    second?;
    if conditional {
        assert!(
            matches!(
                first,
                Err(Error::Conflict(_))
                    | Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Conflict,
                        ..
                    })
            ),
            "explicit generation lost its fence: {first:?}"
        );
        assert!(
            host.read(&workspace, None, "/shared/left", 16, None)
                .await
                .is_err()
        );
        assert_eq!(
            host.read(&workspace, None, "/shared/right", 16, None)
                .await?
                .as_ref(),
            b"right"
        );
        return Ok(());
    }
    let committed = first?;
    assert_eq!(
        publish(&host, &workspace, None, &left, &first_key, mode).await?,
        committed,
        "retry lost the original operation identity"
    );
    for (path, expected) in [
        ("/shared/left", b"left".as_slice()),
        ("/shared/right", b"right".as_slice()),
    ] {
        assert_eq!(
            host.read(&workspace, None, path, 16, None).await?.as_ref(),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn unconditional_publication_retries_a_competing_directory_creation() -> Result<()> {
    for mode in [Mode::Batched, Mode::Raw, Mode::Measured, Mode::Native] {
        for conditional in [false, true] {
            let authority =
                MemoryAuthorityBackend::new(Arc::new(acyclic_stream::MemoryStream::default()));
            schedule(authority, mode, conditional).await?;
        }
    }
    Ok(())
}

#[cfg(feature = "grpc")]
#[tokio::test]
async fn http_publication_retries_the_same_competing_cut() {
    http_proof(false).await;
}

#[cfg(feature = "grpc")]
#[tokio::test]
#[should_panic(expected = "HTTP publication retry proof")]
async fn restoring_conflict_instead_of_restage_fails_the_same_http_proof() {
    http_proof(true).await;
}

#[cfg(feature = "grpc")]
async fn http_proof(restore: bool) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            RESTORE_CONFLICT.set(false);
        }
    }
    let _reset = Reset;
    RESTORE_CONFLICT.set(restore);
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let pem = certificate.cert.pem();
    let identity =
        tonic::transport::Identity::from_pem(&pem, certificate.signing_key.serialize_pem());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let incoming = futures::stream::try_unfold(listener, |listener| async {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    });
    let server = tokio::spawn(async move {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
            .unwrap()
            .add_service(
                acyclic_stream::wire::stream_service_server::StreamServiceServer::new(
                    acyclic_stream::grpc::Service::new(provider.clone()),
                ),
            )
            .add_service(
                acyclic_stream::wire::stream_session_service_server::StreamSessionServiceServer::new(
                    acyclic_stream::session::Service::new(
                        provider, "cut-proof".into(), acyclic_stream::session::Limits::default(),
                    ),
                ),
            )
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let client =
        acyclic_stream::grpc::Client::connect_eager_with_ca_certificate(endpoint, "cut-proof", pem)
            .await
            .unwrap();
    // Remote Streams exposes ordinary work; original native work is covered
    // above with MemoryStream, which implements the native provider contract.
    for mode in [Mode::Batched, Mode::Raw, Mode::Measured] {
        for conditional in [false, true] {
            // Each schedule owns a separate logical workspace and authority.
            let authority = acyclic_fs::StreamAuthorityStore::new(Arc::new(client.clone()));
            let result = schedule(authority, mode, conditional).await;
            assert!(result.is_ok(), "HTTP publication retry proof: {result:?}");
        }
    }
    server.abort();
}
