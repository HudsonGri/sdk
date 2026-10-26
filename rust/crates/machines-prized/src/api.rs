//! Thin typed HTTP client over the Prized Sandbox API.
//!
//! The control plane (default base [`crate::DEFAULT_API_URL`]) creates, reads, pauses,
//! resumes, forks, times out and deletes sandboxes; each sandbox's edge (`toolboxBase`) runs
//! commands.
//! Routes and bodies follow <https://prized.dev/docs/sandboxes-api>. Errors are
//! `{"error": "<code>"}` documents; the code decides the [`ProviderError`]. Wire structs keep
//! unknown fields in an `extra` map, since Prized adds fields without notice.

use std::{collections::BTreeMap, time::Duration};

use acyclic_machines::ProviderError;
use reqwest::{Method, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::field::Empty;

use crate::PrizedConfig;

/// Header carrying the caller's idempotency key on create and fork.
pub const IDEMPOTENCY_HEADER: &str = "Idempotency-Key";
/// Header that stops an edge request from resuming a paused sandbox.
pub const AUTO_RESUME_HEADER: &str = "Prized-Auto-Resume";

/// One Prized sandbox (`{sandbox}` in every control-plane answer).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sandbox {
    /// `sbx_` plus 16 hex digits.
    pub id: String,
    /// `starting`, `running`, `pausing`, `paused`, `resizing`, `deleting`, `deleted`, `error`.
    #[serde(default)]
    pub state: Option<String>,
    /// Template the sandbox was created from.
    #[serde(default)]
    pub template: Option<String>,
    /// Template build it runs, kept for life.
    #[serde(default)]
    pub template_build: Option<String>,
    /// Shape.
    #[serde(default)]
    pub resources: Option<Resources>,
    /// String labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Idle minutes before automatic pause; `None` when off.
    #[serde(default)]
    pub auto_pause_min: Option<u32>,
    /// Paused minutes before automatic delete; `None` when off.
    #[serde(default)]
    pub auto_delete_min: Option<u32>,
    /// When its run time ends and Prized deletes it (RFC 3339); `None` while paused, when the
    /// clock is stopped.
    #[serde(default)]
    pub expires_at: Option<String>,
    /// Why it is in `error`.
    #[serde(default)]
    pub error_reason: Option<String>,
    /// Edge base URL for this sandbox's commands and files.
    #[serde(default)]
    pub toolbox_base: Option<String>,
    /// RFC 3339 creation time.
    #[serde(default)]
    pub created_at: Option<String>,
    /// Fields this client does not model.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Sandbox shape. Any field left `None` takes the template's default.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resources {
    /// vCPUs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcpu: Option<u32>,
    /// Memory in MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
    /// Disk in GiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_gb: Option<u64>,
}

/// Body of `POST /sandboxes`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandboxRequest {
    /// Template id from `GET /sandbox-templates`.
    pub template: String,
    /// Shape; the template's defaults when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources>,
    /// Environment for every command; stored encrypted and never returned.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub env_vars: BTreeMap<String, String>,
    /// Labels.
    pub labels: BTreeMap<String, String>,
    /// Idle minutes before automatic pause.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_pause_min: Option<u32>,
    /// Minutes of running before Prized deletes the sandbox; the clock stops while it is paused
    /// and starts again at every resume. The workspace's default (24 hours on a paid plan)
    /// when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_runtime_min: Option<u32>,
}

/// Body of `POST /sandboxes/{id}/fork`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ForkRequest {
    /// Children to make, 1 to 256, in one call. Each child's run time is the parent's
    /// `maxRuntimeMin` from the fork on a paid workspace; on the free tier it keeps the
    /// parent's deletion time.
    pub count: u32,
    /// Labels for every child, replacing the parent's.
    pub labels: BTreeMap<String, String>,
}

/// The fork record of `POST /sandboxes/{id}/fork`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fork {
    /// `frk_` plus 16 hex digits.
    pub id: String,
    /// The forked sandbox.
    #[serde(default)]
    pub parent_id: Option<String>,
    /// Children, in fork order.
    #[serde(default)]
    pub sandbox_ids: Vec<String>,
    /// Fields this client does not model.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct SandboxDoc {
    sandbox: Sandbox,
}

#[derive(Deserialize)]
struct SandboxList {
    sandboxes: Vec<Sandbox>,
}

#[derive(Deserialize)]
struct ForkDoc {
    fork: Fork,
}

/// Body of `POST /sandboxes/{id}/timeout`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimeoutRequest<'a> {
    expires_at: &'a str,
}

/// One template from `GET /sandbox-templates`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct Template {
    /// Name a create passes.
    pub id: String,
    /// Fields this client does not model.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Deserialize)]
struct TemplateList {
    templates: Vec<Template>,
}

/// Body of the edge `POST /process/execute`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ExecuteRequest {
    /// Command line, run with `bash -c` as the user `prized`.
    pub command: String,
    /// Seconds before the command and its process group are killed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<f64>,
}

/// Answer of the edge `POST /process/execute`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResponse {
    /// Standard output (the first 4 MiB).
    #[serde(default)]
    pub stdout: String,
    /// Standard error (the first 4 MiB).
    #[serde(default)]
    pub stderr: String,
    /// Exit code; `-1` when the deadline killed it.
    pub exit_code: i32,
    /// Whether the deadline killed the command.
    #[serde(default)]
    pub timed_out: bool,
}

/// Why an edge request failed. Unlike the control plane, it matters whether the command may
/// have run: Prized says so per error code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EdgeError {
    /// No answer: the command may have run.
    Transport,
    /// An error answer.
    Status {
        /// HTTP status.
        status: u16,
        /// Prized error code.
        code: String,
    },
}

impl EdgeError {
    /// Whether the command may have run despite the error: no answer at all,
    /// `502 sandbox_unreachable`, or `410 sandbox_deleted`, which a command that was running
    /// when its sandbox was deleted answers (it is stopped with the sandbox).
    #[must_use]
    pub fn may_have_run(&self) -> bool {
        match self {
            Self::Transport => true,
            Self::Status { status, code } => matches!(
                (*status, code.as_str()),
                (502, "sandbox_unreachable") | (410, "sandbox_deleted")
            ),
        }
    }
}

/// Authenticated client bound to one Prized API base URL.
#[derive(Clone)]
pub struct PrizedApi {
    http: reqwest::Client,
    base: String,
    api_key: String,
    edge_hosts: Vec<String>,
}

impl std::fmt::Debug for PrizedApi {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrizedApi")
            .field("base", &self.base)
            .field("edge_hosts", &self.edge_hosts)
            .finish_non_exhaustive()
    }
}

impl PrizedApi {
    /// Builds a client from the provider configuration.
    ///
    /// # Errors
    /// Returns [`ProviderError::Invalid`] when the API key is empty or the HTTP client cannot
    /// be built.
    pub fn new(config: &PrizedConfig) -> Result<Self, ProviderError> {
        if config.api_key.trim().is_empty() {
            return Err(ProviderError::Invalid("Prized API key is empty".into()));
        }
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| ProviderError::Invalid(format!("HTTP client: {error}")))?;
        Ok(Self {
            http,
            base: config.api_url.trim_end_matches('/').to_owned(),
            api_key: config.api_key.clone(),
            edge_hosts: config.edge_hosts.clone(),
        })
    }

    /// Base URL the client was bound to, without a trailing slash.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.api_key)
    }

    /// Sends one control-plane call, `op` naming it in its `acyclic.machines.prized.call`
    /// span. Returns the decoded body and whether Prized answered `200` to a create or fork,
    /// which is an exact idempotent replay of an earlier request.
    #[tracing::instrument(
        name = "acyclic.machines.prized.call",
        level = "info",
        skip_all,
        fields(op = op, http.status = Empty, outcome = Empty, error.kind = Empty)
    )]
    async fn send<T: for<'de> Deserialize<'de>>(
        &self,
        op: &'static str,
        builder: RequestBuilder,
    ) -> Result<(T, StatusCode), ProviderError> {
        let result = async {
            let response = builder
                .send()
                .await
                .map_err(|error| transport_error(&error))?;
            let status = response.status();
            tracing::Span::current().record("http.status", status.as_u16());
            let body = response
                .bytes()
                .await
                .map_err(|error| transport_error(&error))?;
            if !status.is_success() {
                return Err(status_error(status, &body));
            }
            serde_json::from_slice(&body)
                .map(|value| (value, status))
                .map_err(|error| {
                    tracing::warn!(
                        op,
                        body_len = body.len(),
                        "Prized answer did not match the expected shape"
                    );
                    ProviderError::Rejected(format!("Prized answer shape: {error}"))
                })
        }
        .await;
        let span = tracing::Span::current();
        match &result {
            Ok(_) => span.record("outcome", "ok"),
            Err(error) => span
                .record("outcome", "err")
                .record("error.kind", error.kind()),
        };
        result
    }

    async fn sandbox(
        &self,
        op: &'static str,
        builder: RequestBuilder,
    ) -> Result<Sandbox, ProviderError> {
        self.send::<SandboxDoc>(op, builder)
            .await
            .map(|(doc, _)| doc.sandbox)
    }

    /// `GET /sandbox-templates`.
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`].
    pub async fn templates(&self) -> Result<Vec<Template>, ProviderError> {
        self.send::<TemplateList>("templates", self.request(Method::GET, "/sandbox-templates"))
            .await
            .map(|(list, _)| list.templates)
    }

    /// `POST /sandboxes?wait=running` under `idempotency_key`. Returns the sandbox and whether
    /// this was a replay of an earlier create with the same key and body (`200`, not `201`).
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`]; a key reused with
    /// another body is [`ProviderError::Conflict`].
    pub async fn create(
        &self,
        body: &CreateSandboxRequest,
        idempotency_key: &str,
    ) -> Result<(Sandbox, bool), ProviderError> {
        let builder = self
            .request(Method::POST, "/sandboxes?wait=running")
            .header(IDEMPOTENCY_HEADER, idempotency_key)
            .json(body);
        self.send::<SandboxDoc>("create", builder)
            .await
            .map(|(doc, status)| (doc.sandbox, status == StatusCode::OK))
    }

    /// `GET /sandboxes/{id}`; a deleted sandbox is [`ProviderError::NotFound`].
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`].
    pub async fn get(&self, id: &str) -> Result<Sandbox, ProviderError> {
        self.sandbox(
            "get",
            self.request(Method::GET, &format!("/sandboxes/{id}")),
        )
        .await
    }

    /// `GET /sandboxes?label=key=value...`: every sandbox in the workspace that is not deleted
    /// and carries all of `labels` (every sandbox when `labels` is empty), newest first.
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`].
    pub async fn list(&self, labels: &[(&str, &str)]) -> Result<Vec<Sandbox>, ProviderError> {
        let selectors: Vec<(&str, String)> = labels
            .iter()
            .map(|(name, value)| ("label", format!("{name}={value}")))
            .collect();
        let builder = self.request(Method::GET, "/sandboxes").query(&selectors);
        self.send::<SandboxList>("list", builder)
            .await
            .map(|(list, _)| list.sandboxes)
    }

    /// `POST /sandboxes/{id}/pause?wait=paused`.
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`].
    pub async fn pause(&self, id: &str) -> Result<Sandbox, ProviderError> {
        let path = format!("/sandboxes/{id}/pause?wait=paused");
        self.sandbox("pause", self.request(Method::POST, &path))
            .await
    }

    /// `POST /sandboxes/{id}/resume?wait=running`. Memory and processes carry on.
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`].
    pub async fn resume(&self, id: &str) -> Result<Sandbox, ProviderError> {
        let path = format!("/sandboxes/{id}/resume?wait=running");
        self.sandbox("resume", self.request(Method::POST, &path))
            .await
    }

    /// `POST /sandboxes/{id}/fork` under `idempotency_key`: one call makes every child, each a
    /// live copy of the parent's memory, processes and disk. A running parent is copied as it
    /// is at the fork; a paused one stays paused and is copied at its pause point. Returns the
    /// fork, children in fork order, and whether this was a replay of an earlier fork with the
    /// same key and body. Prized answers a replay before any parent check, so it works after
    /// the parent moved on.
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`].
    pub async fn fork(
        &self,
        id: &str,
        body: &ForkRequest,
        idempotency_key: &str,
    ) -> Result<(Fork, bool), ProviderError> {
        let builder = self
            .request(Method::POST, &format!("/sandboxes/{id}/fork"))
            .header(IDEMPOTENCY_HEADER, idempotency_key)
            .json(body);
        self.send::<ForkDoc>("fork", builder)
            .await
            .map(|(doc, status)| (doc.fork, status == StatusCode::OK))
    }

    /// `POST /sandboxes/{id}/timeout` with `{expiresAt}`: sets when a running sandbox's run time
    /// ends, earlier or later (at most 7 days from now; on the free tier, only earlier).
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`]; a paused sandbox is
    /// [`ProviderError::Conflict`] (`409 sandbox_not_running`).
    pub async fn timeout(&self, id: &str, expires_at: &str) -> Result<Sandbox, ProviderError> {
        let builder = self
            .request(Method::POST, &format!("/sandboxes/{id}/timeout"))
            .json(&TimeoutRequest { expires_at });
        self.sandbox("timeout", builder).await
    }

    /// `DELETE /sandboxes/{id}`; again on a deleting sandbox is a no-op.
    ///
    /// # Errors
    /// Maps transport failures and error answers to [`ProviderError`].
    pub async fn delete(&self, id: &str) -> Result<(), ProviderError> {
        self.sandbox(
            "delete",
            self.request(Method::DELETE, &format!("/sandboxes/{id}")),
        )
        .await
        .map(|_| ())
    }

    /// Edge `POST {toolbox_base}/process/execute`, with `Prized-Auto-Resume: false` so a
    /// command never wakes a paused machine behind the caller's back. `deadline` bounds the
    /// whole request, command included.
    ///
    /// # Errors
    /// Returns an [`EdgeError`] saying whether the command may have run; a `toolbox_base`
    /// that fails [`Self::check_edge`] is refused before anything is sent.
    #[tracing::instrument(
        name = "acyclic.machines.prized.exec",
        level = "info",
        skip_all,
        fields(http.status = Empty, outcome = Empty, error.kind = Empty)
    )]
    pub async fn execute(
        &self,
        toolbox_base: &str,
        body: &ExecuteRequest,
        deadline: Duration,
    ) -> Result<Result<ExecuteResponse, EdgeError>, ProviderError> {
        let url = self.check_edge(toolbox_base)?;
        let outcome = async {
            let response = self
                .http
                .post(format!(
                    "{}/process/execute",
                    url.as_str().trim_end_matches('/')
                ))
                .bearer_auth(&self.api_key)
                .header(AUTO_RESUME_HEADER, "false")
                .timeout(deadline)
                .json(body)
                .send()
                .await
                .map_err(|_| EdgeError::Transport)?;
            let status = response.status();
            tracing::Span::current().record("http.status", status.as_u16());
            let bytes = response.bytes().await.map_err(|_| EdgeError::Transport)?;
            if status.is_success() {
                serde_json::from_slice(&bytes).map_err(|_| EdgeError::Transport)
            } else {
                Err(EdgeError::Status {
                    status: status.as_u16(),
                    code: error_code(&bytes),
                })
            }
        }
        .await;
        let span = tracing::Span::current();
        match &outcome {
            Ok(_) => span.record("outcome", "ok"),
            Err(EdgeError::Transport) => span
                .record("outcome", "err")
                .record("error.kind", "transport"),
            Err(EdgeError::Status { .. }) => {
                span.record("outcome", "err").record("error.kind", "status")
            }
        };
        Ok(outcome)
    }

    /// Checks that the API key may go to `toolbox_base`: no userinfo, and either the API host
    /// itself (same scheme and port) or, over HTTPS, one of
    /// [`crate::PrizedConfig::edge_hosts`] (default `term.prized.dev`). The address comes from
    /// a server answer, so it is never trusted blindly.
    ///
    /// # Errors
    /// Returns [`ProviderError::Rejected`] for any other URL.
    pub fn check_edge(&self, toolbox_base: &str) -> Result<reqwest::Url, ProviderError> {
        let rejected = || {
            ProviderError::Rejected(format!(
                "edge URL {toolbox_base:?} is not a Prized edge for {}",
                self.base
            ))
        };
        let base = reqwest::Url::parse(&self.base).map_err(|_| rejected())?;
        let url = reqwest::Url::parse(toolbox_base).map_err(|_| rejected())?;
        let (Some(base_host), Some(host)) = (base.host_str(), url.host_str()) else {
            return Err(rejected());
        };
        let userinfo = !url.username().is_empty() || url.password().is_some();
        let same_origin = host.eq_ignore_ascii_case(base_host)
            && url.scheme() == base.scheme()
            && url.port_or_known_default() == base.port_or_known_default();
        let listed = url.scheme() == "https"
            && self
                .edge_hosts
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(host));
        if !userinfo && (same_origin || listed) {
            Ok(url)
        } else {
            Err(rejected())
        }
    }
}

fn transport_error(error: &reqwest::Error) -> ProviderError {
    tracing::warn!(
        timeout = error.is_timeout(),
        connect = error.is_connect(),
        "Prized request failed at the transport level"
    );
    ProviderError::Unavailable
}

/// The `error` code of a Prized error document, or `""`.
fn error_code(body: &[u8]) -> String {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("error")?.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Maps a control-plane error answer to the closest provider-boundary failure.
///
/// Refusals after which nothing was made (balance, capacity on the parent's machine, a parent
/// that cannot fork, quota, a held workspace) are [`ProviderError::Rejected`], so the caller
/// may retry under the same key; `410 sandbox_deleted` (a pause or resume of a sandbox being
/// deleted) is [`ProviderError::NotFound`]; `503 sandbox_capacity` and other server errors are
/// [`ProviderError::Unavailable`].
fn status_error(status: StatusCode, body: &[u8]) -> ProviderError {
    let code = error_code(body);
    let detail = String::from_utf8_lossy(body)
        .chars()
        .take(512)
        .collect::<String>();
    match (status, code.as_str()) {
        (StatusCode::NOT_FOUND, "template_not_found") => ProviderError::Unsupported(detail),
        (StatusCode::NOT_FOUND | StatusCode::GONE, _) => ProviderError::NotFound(detail),
        (
            StatusCode::CONFLICT,
            "idempotency_conflict" | "parent_not_running" | "sandbox_not_running",
        ) => ProviderError::Conflict(detail),
        (StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY, _) => {
            ProviderError::Invalid(detail)
        }
        (StatusCode::TOO_MANY_REQUESTS, "sandbox_quota_exceeded") => {
            ProviderError::Rejected(detail)
        }
        (StatusCode::TOO_MANY_REQUESTS, _) => ProviderError::Unavailable,
        _ if status.is_server_error() => ProviderError::Unavailable,
        _ => ProviderError::Rejected(format!("{status}: {detail}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(base: &str) -> PrizedApi {
        let mut config = PrizedConfig::new("dcp_test");
        config.api_url = base.to_owned();
        PrizedApi::new(&config).unwrap()
    }

    #[test]
    fn the_key_goes_only_to_prized_edges() {
        let api = client("https://api.prized.dev/api/v1");
        for allowed in [
            "https://term.prized.dev/v1/sandbox/sbx_2f3e54075a930b46",
            "https://api.prized.dev/edge/sbx_2f3e54075a930b46",
        ] {
            assert!(api.check_edge(allowed).is_ok(), "{allowed}");
        }
        for refused in [
            "http://term.prized.dev/v1/sandbox/sbx_2f3e54075a930b46",
            "https://term.prized.dev.evil.example/v1/sandbox/x",
            "https://user@term.prized.dev/v1/sandbox/x",
            "https://api.prized.dev:8443/edge/x",
            "https://evil.example/v1/sandbox/x",
            "not a url",
        ] {
            assert!(api.check_edge(refused).is_err(), "{refused}");
        }
        let local = client("http://127.0.0.1:4100/api/v1");
        assert!(local.check_edge("http://127.0.0.1:4100/edge/sbx_1").is_ok());
        assert!(
            local
                .check_edge("http://127.0.0.1:4101/edge/sbx_1")
                .is_err()
        );
    }

    #[test]
    fn error_codes_map_to_stable_failures() {
        let map = |status: u16, code: &str| {
            status_error(
                StatusCode::from_u16(status).unwrap(),
                format!("{{\"error\":\"{code}\"}}").as_bytes(),
            )
            .kind()
        };
        assert_eq!(map(404, "not_found"), "not_found");
        assert_eq!(map(404, "template_not_found"), "unsupported");
        assert_eq!(map(409, "idempotency_conflict"), "conflict");
        assert_eq!(map(409, "parent_not_running"), "conflict");
        assert_eq!(map(409, "fork_capacity"), "rejected");
        assert_eq!(map(409, "fork_unsupported_host"), "rejected");
        assert_eq!(map(409, "parent_not_forkable"), "rejected");
        assert_eq!(map(409, "sandbox_not_running"), "conflict");
        assert_eq!(map(410, "sandbox_deleted"), "not_found");
        assert_eq!(map(409, "insufficient_balance"), "rejected");
        assert_eq!(map(422, "invalid_count"), "invalid");
        assert_eq!(map(429, "sandbox_quota_exceeded"), "rejected");
        assert_eq!(map(429, "rate_limited"), "unavailable");
        assert_eq!(map(503, "sandbox_capacity"), "unavailable");
        assert_eq!(map(401, "unauthorized"), "rejected");
    }

    #[test]
    fn edge_errors_say_whether_the_command_may_have_run() {
        let status = |status: u16, code: &str| EdgeError::Status {
            status,
            code: code.to_owned(),
        };
        assert!(EdgeError::Transport.may_have_run());
        assert!(status(502, "sandbox_unreachable").may_have_run());
        assert!(status(410, "sandbox_deleted").may_have_run());
        assert!(!status(503, "sandbox_not_ready").may_have_run());
        assert!(!status(409, "sandbox_not_running").may_have_run());
        assert!(!status(502, "resolve_failed").may_have_run());
    }

    #[test]
    fn bodies_match_the_documented_shapes() {
        let body = CreateSandboxRequest {
            template: "prized-base-v1".into(),
            resources: Some(Resources {
                vcpu: Some(1),
                memory_mb: Some(1024),
                disk_gb: None,
            }),
            labels: BTreeMap::from([("run".into(), "1".into())]),
            auto_pause_min: Some(1),
            ..CreateSandboxRequest::default()
        };
        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            serde_json::json!({
                "template": "prized-base-v1",
                "resources": {"vcpu": 1, "memoryMb": 1024},
                "labels": {"run": "1"},
                "autoPauseMin": 1
            })
        );
        let sandbox: Sandbox = serde_json::from_value(serde_json::json!({
            "id": "sbx_2f3e54075a930b46", "state": "running", "template": "prized-agent-v1",
            "resources": {"vcpu": 8, "memoryMb": 16384, "diskGb": 40},
            "expiresAt": "2026-10-21T17:23:51.406Z", "errorReason": null,
            "labels": {"run": "pr-1874"}, "autoPauseMin": null, "autoDeleteMin": null,
            "toolboxBase": "https://term.prized.dev/v1/sandbox/sbx_2f3e54075a930b46",
            "createdAt": "2026-10-21T09:23:51.438Z", "lastActiveAt": "2026-10-21T09:25:48.979Z"
        }))
        .unwrap();
        assert_eq!(sandbox.resources.and_then(|r| r.memory_mb), Some(16_384));
        assert!(sandbox.extra.contains_key("lastActiveAt"));
    }
}
