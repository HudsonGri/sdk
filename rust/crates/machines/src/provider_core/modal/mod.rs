//! Modal Sandbox V2 control plane and native task command router transport.
//! Requires an application and an already built immutable Modal image binding.
mod wire;
use super::{
    Allocation, ExecSession, NetworkPolicy, SandboxDriver, SandboxInstance, SandboxSpec,
    http::{Http, secret},
};
use crate::{ExecCommand, ExecOutput, ExecStream, MachineId, MachineState, ProviderError};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};
use tonic::{
    Request,
    metadata::MetadataMap,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

const CONTROL: &str = "modal.client.ModalClient";
const ROUTER: &str = "modal.task_command_router.TaskCommandRouter";

#[derive(Clone)]
struct Rpc {
    channel: Channel,
    metadata: MetadataMap,
}

impl Rpc {
    fn new(endpoint: &str, metadata: MetadataMap) -> Result<Self, ProviderError> {
        let http = Http::new(endpoint, reqwest::header::HeaderMap::new())?;
        let mut endpoint = Endpoint::from_shared(http.endpoint.to_string())
            .map_err(|_| {
                ProviderError::Invalid(
                    "Modal endpoint admission failed; recovery: configure an absolute gRPC URL"
                        .into(),
                )
            })?
            .connect_timeout(Duration::from_secs(5));
        if http.endpoint.scheme() == "https" {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .map_err(|_| ProviderError::Unavailable)?;
        }
        Ok(Self {
            channel: endpoint.connect_lazy(),
            metadata,
        })
    }

    fn request<T>(&self, message: T, timeout: Duration) -> Request<T> {
        let mut request = Request::new(message);
        *request.metadata_mut() = self.metadata.clone();
        request.set_timeout(timeout);
        request
    }

    async fn unary<I, O>(
        &self,
        service: &'static str,
        method: &'static str,
        message: I,
        timeout: Duration,
    ) -> Result<O, ProviderError>
    where
        I: Message + Default + Send + Sync + 'static,
        O: Message + Default + Send + Sync + 'static,
    {
        let mut client = tonic::client::Grpc::new(self.channel.clone());
        client
            .ready()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let path = format!("/{service}/{method}").parse().map_err(|_| ProviderError::Invalid("Modal RPC path construction failed; recovery: use the shipped adapter's method names".into()))?;
        client
            .unary(
                self.request(message, timeout),
                path,
                tonic_prost::ProstCodec::default(),
            )
            .await
            .map(tonic::Response::into_inner)
            .map_err(rpc_error)
    }

    async fn output(
        &self,
        task: String,
        exec: String,
        fd: i32,
        timeout: Duration,
    ) -> Result<ExecStream, ProviderError> {
        let mut client = tonic::client::Grpc::new(self.channel.clone());
        client
            .ready()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        let response: tonic::Response<tonic::Streaming<wire::TaskExecStdioReadResponse>> = client
            .server_streaming(
                self.request(
                    wire::TaskExecStdioReadRequest {
                        task_id: task,
                        exec_id: exec,
                        offset: 0,
                        file_descriptor: fd,
                    },
                    timeout,
                ),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/modal.task_command_router.TaskCommandRouter/TaskExecStdioRead",
                ),
                tonic_prost::ProstCodec::default(),
            )
            .await
            .map_err(rpc_error)?;
        Ok(response
            .into_inner()
            .map(|item| item.map_err(rpc_error))
            .try_filter_map(move |frame| async move {
                Ok(if frame.data.is_empty() {
                    None
                } else {
                    Some(if fd == 0 {
                        ExecOutput::Stdout(frame.data)
                    } else {
                        ExecOutput::Stderr(frame.data)
                    })
                })
            })
            .boxed())
    }
}

fn rpc_error(error: tonic::Status) -> ProviderError {
    match error.code() {
        tonic::Code::NotFound => ProviderError::NotFound("Modal resource".into()),
        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition => ProviderError::Conflict("Modal state conflict; recovery: inspect the sandbox before a new mutation".into()),
        tonic::Code::InvalidArgument | tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => ProviderError::Rejected("Modal request rejected; recovery: inspect app, image, network policy and credential configuration".into()),
        _ => ProviderError::Unavailable,
    }
}

#[derive(Clone)]
struct Guest {
    task: String,
    router: Rpc,
    sandbox_token: String,
}

/// Modal connection with explicit credentials; never reads account config files.
/// V2 Sandboxes have a 24-hour ceiling, retained as an admitted maximum age.
#[derive(Clone)]
pub struct Modal {
    control: Rpc,
    app: String,
    guests: Arc<Mutex<BTreeMap<String, Guest>>>,
}

impl Modal {
    /// Binds to the Modal control plane (normally `https://api.modal.com`).
    pub fn new(
        endpoint: &str,
        token_id: &str,
        token_secret: &str,
        app_id: &str,
    ) -> Result<Self, ProviderError> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-modal-token-id", secret(token_id)?);
        headers.insert("x-modal-token-secret", secret(token_secret)?);
        headers.insert(
            "x-modal-client-type",
            reqwest::header::HeaderValue::from_static("1"),
        );
        headers.insert(
            "x-modal-client-version",
            reqwest::header::HeaderValue::from_static("0.11.1"),
        );
        if app_id.is_empty() {
            return Err(ProviderError::Invalid(
                "Modal app admission failed; recovery: configure an existing app ID".into(),
            ));
        }
        Ok(Self {
            control: Rpc::new(endpoint, MetadataMap::from_headers(headers))?,
            app: app_id.into(),
            guests: Arc::default(),
        })
    }

    fn guest(&self, instance: &SandboxInstance) -> Result<Guest, ProviderError> {
        self.guests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&instance.id)
            .cloned()
            .ok_or_else(|| ProviderError::NotFound(instance.id.clone()))
    }

    fn control_for(&self, guest: &Guest) -> Result<Rpc, ProviderError> {
        let mut control = self.control.clone();
        if !guest.sandbox_token.is_empty() {
            let mut headers = control.metadata.clone().into_headers();
            headers.insert("x-modal-sandbox-token", secret(&guest.sandbox_token)?);
            control.metadata = MetadataMap::from_headers(headers);
        }
        Ok(control)
    }

    async fn raw_start(
        guest: &Guest,
        argv: Vec<String>,
        timeout: Duration,
        input: Option<Vec<u8>>,
    ) -> Result<(String, ExecStream), ProviderError> {
        let exec = format!("ex-{}", uuid::Uuid::new_v4().simple());
        let _: wire::Empty = guest
            .router
            .unary(
                ROUTER,
                "TaskExecStart",
                wire::TaskExecStartRequest {
                    task_id: guest.task.clone(),
                    exec_id: exec.clone(),
                    command_args: argv,
                    stdout_config: 1,
                    stderr_config: 1,
                    timeout_secs: Some(
                        u32::try_from(
                            timeout
                                .as_secs()
                                .saturating_add(u64::from(timeout.subsec_nanos() != 0)),
                        )
                        .unwrap_or(u32::MAX),
                    ),
                },
                Duration::from_secs(15),
            )
            .await?;
        // Attach both streams while pumping stdin. Some routers delay response
        // headers until the process emits bytes; awaiting those headers first
        // would deadlock a command which needs input before producing output.
        let attach = |fd| {
            let guest = guest.clone();
            let exec = exec.clone();
            stream::once(async move {
                guest
                    .router
                    .output(guest.task, exec, fd, timeout + Duration::from_secs(10))
                    .await
            })
            .try_flatten()
            .boxed()
        };
        let stdout = attach(0);
        let stderr = attach(1);
        let router = guest.router.clone();
        let wait = wire::TaskExecWaitRequest {
            task_id: guest.task.clone(),
            exec_id: exec.clone(),
        };
        let exit = stream::once(async move {
            let result: wire::TaskExecWaitResponse = router
                .unary(
                    ROUTER,
                    "TaskExecWait",
                    wait,
                    timeout + Duration::from_secs(10),
                )
                .await?;
            match result.exit_status {
                Some(wire::ExitStatus::Code(code)) => Ok(ExecOutput::ExitCode(code)),
                Some(wire::ExitStatus::Signal(signal)) => Ok(ExecOutput::ExitCode(128 + signal)),
                None => Err(ProviderError::Unavailable),
            }
        });
        let input_guest = guest.clone();
        let input_exec = exec.clone();
        let input = stream::once(async move {
            if let Some(data) = input {
                for (offset, bytes) in data.chunks(65_536).enumerate() {
                    let _: wire::Empty = input_guest
                        .router
                        .unary(
                            ROUTER,
                            "TaskExecStdinWrite",
                            wire::TaskExecStdinWriteRequest {
                                task_id: input_guest.task.clone(),
                                exec_id: input_exec.clone(),
                                offset: offset as u64 * 65_536,
                                data: bytes.to_vec(),
                                eof: false,
                            },
                            Duration::from_secs(10),
                        )
                        .await?;
                }
                let _: wire::Empty = input_guest
                    .router
                    .unary(
                        ROUTER,
                        "TaskExecStdinWrite",
                        wire::TaskExecStdinWriteRequest {
                            task_id: input_guest.task,
                            exec_id: input_exec,
                            offset: data.len() as u64,
                            data: vec![],
                            eof: true,
                        },
                        Duration::from_secs(10),
                    )
                    .await?;
            }
            Ok::<Option<ExecOutput>, ProviderError>(None)
        })
        .try_filter_map(|frame| async move { Ok(frame) });
        Ok((
            exec,
            stream::select(stream::select(stdout, stderr), input)
                .chain(exit)
                .boxed(),
        ))
    }

    fn owned(
        &self,
        instance: &SandboxInstance,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        let driver = self.clone();
        let instance = instance.clone();
        let request = command.clone();
        super::owned_exec(
            MachineId::from_digest(sha2::Sha256::digest(instance.id.as_bytes()).into()),
            &command,
            None,
            super::ConsoleHistory::default(),
            driver.start(&instance, request)?,
        )
    }
}

use sha2::Digest;

#[async_trait]
impl SandboxDriver for Modal {
    fn name(&self) -> &'static str {
        "modal"
    }
    fn supports_suspend(&self) -> bool {
        false
    }
    fn maximum_lifetime(&self) -> Option<Duration> {
        Some(Duration::from_secs(24 * 3600))
    }
    fn validate(&self, spec: &SandboxSpec) -> Result<(), ProviderError> {
        if !spec.env.is_empty() {
            return Err(ProviderError::Unsupported("Modal environment admission failed; recovery: register environment variables in the immutable image".into()));
        }
        spec.lifetime_seconds().map(|_| ())
    }

    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxInstance, ProviderError> {
        let network = match &spec.network {
            NetworkPolicy::BlockAll => wire::NetworkAccess {
                network_access_type: 2,
                allowed_domains: vec![],
            },
            NetworkPolicy::AllowDomains(domains) => wire::NetworkAccess {
                network_access_type: 3,
                allowed_domains: domains.clone(),
            },
        };
        let response: wire::SandboxCreateV2Response = self
            .control
            .unary(
                CONTROL,
                "SandboxCreateV2",
                wire::SandboxCreateV2Request {
                    app_id: self.app.clone(),
                    definition: Some(wire::Sandbox {
                        entrypoint_args: vec!["sleep".into(), "infinity".into()],
                        image_id: spec.image.clone(),
                        resources: Some(wire::Resources {
                            memory_mb: 1024,
                            milli_cpu: 1000,
                        }),
                        timeout_secs: spec.lifetime_seconds()?,
                        network_access: Some(network),
                        name: Some(spec.name.clone()),
                    }),
                },
                Duration::from_secs(30),
            )
            .await?;
        if response.sandbox_id.is_empty() || response.task_id.is_empty() {
            return Err(ProviderError::Unavailable);
        }
        let access = response
            .command_router_access
            .ok_or(ProviderError::Unavailable)?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            secret(&format!("Bearer {}", access.jwt))?,
        );
        let router = Rpc::new(&access.url, MetadataMap::from_headers(headers))?;
        self.guests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                response.sandbox_id.clone(),
                Guest {
                    task: response.task_id,
                    router,
                    sandbox_token: response.sandbox_token,
                },
            );
        Ok(SandboxInstance {
            endpoint: format!("modal://{}", response.sandbox_id),
            id: response.sandbox_id,
            allocation: Allocation {
                cpu: 1,
                memory_gib: 1,
                disk_gib: 0,
            },
        })
    }

    async fn state(&self, instance: &SandboxInstance) -> Result<MachineState, ProviderError> {
        let guest = self.guest(instance)?;
        let response: wire::SandboxWaitResponse = self
            .control_for(&guest)?
            .unary(
                CONTROL,
                "SandboxWaitV2",
                wire::SandboxWaitRequest {
                    sandbox_id: instance.id.clone(),
                    timeout: 0.0,
                },
                Duration::from_secs(5),
            )
            .await?;
        Ok(
            if response.result.is_some_and(|result| result.status != 0) {
                MachineState::Destroyed
            } else {
                MachineState::Running
            },
        )
    }

    async fn destroy(&self, instance: &SandboxInstance) -> Result<(), ProviderError> {
        let guest = self.guest(instance)?;
        let _: wire::Empty = self
            .control_for(&guest)?
            .unary(
                CONTROL,
                "SandboxTerminateV2",
                wire::SandboxRequest {
                    sandbox_id: instance.id.clone(),
                },
                Duration::from_secs(10),
            )
            .await?;
        self.guests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&instance.id);
        Ok(())
    }

    fn start(
        &self,
        instance: &SandboxInstance,
        command: ExecCommand,
    ) -> Result<ExecSession, ProviderError> {
        let guest = self.guest(instance)?;
        let maintenance_guest = guest.clone();
        let timeout = Duration::from_millis(u64::from(command.timeout_ms()));
        Ok(super::process_group_session(
            &command,
            move |argv| {
                Box::pin(async move {
                    Self::raw_start(&guest, argv, timeout, None)
                        .await
                        .map(|(_, output)| output)
                })
            },
            move |argv| {
                let guest = maintenance_guest.clone();
                Box::pin(async move {
                    Self::raw_start(&guest, argv, Duration::from_secs(10), None)
                        .await
                        .map(|(_, output)| output)
                })
            },
        ))
    }

    async fn read_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
    ) -> Result<Vec<u8>, ProviderError> {
        let command = ExecCommand::new(vec!["cat".into(), "--".into(), path.into()], 30_000)?;
        super::collect_success(self.owned(instance, command)?, "Modal file download").await
    }

    async fn write_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
        data: &[u8],
    ) -> Result<(), ProviderError> {
        let guest = self.guest(instance)?;
        let command = ExecCommand::new(
            vec![
                "sh".into(),
                "-c".into(),
                format!("umask 077; cat > {}", super::shell_quote(path)),
            ],
            30_000,
        )?;
        let maintenance_guest = guest.clone();
        let data = data.to_vec();
        let session = super::process_group_session(
            &command,
            move |argv| {
                Box::pin(async move {
                    Self::raw_start(&guest, argv, Duration::from_secs(30), Some(data))
                        .await
                        .map(|(_, output)| output)
                })
            },
            move |argv| {
                let guest = maintenance_guest.clone();
                Box::pin(async move {
                    Self::raw_start(&guest, argv, Duration::from_secs(10), None)
                        .await
                        .map(|(_, output)| output)
                })
            },
        );
        let output = super::owned_exec(
            MachineId::from_digest(sha2::Sha256::digest(instance.id.as_bytes()).into()),
            &command,
            None,
            super::ConsoleHistory::default(),
            session,
        )?;
        super::collect_success(output, "Modal file upload")
            .await
            .map(|_| ())
    }
}
