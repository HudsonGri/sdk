#![doc = include_str!("../README.md")]
#![deny(unsafe_code)]

pub mod api;
pub mod map;
pub mod ops;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test-only stand-in for the Prized API; a broken fake should fail the test loudly"
)]
mod fake;
#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "tests index collections whose shape they just built or asserted"
)]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{Mutex, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use acyclic_machines::{
    Capability, CheckpointId, CheckpointObservation, CompatibilityPolicy, CreateMachine, EventPage,
    ExecCommand, ExecOutput, ExecStream, ForkFidelity, IdempotencyKey, Image, ImageQualification,
    MAX_EVENT_PAGE_SIZE, MAX_OUTPUT_READ_BYTES, MAX_PAGE_SIZE, MachineContract, MachineId,
    MachineObservation, MachineOutput, MachinePage, MachineState, MachinesProvider,
    MutationOutcome, OperationId, OperationObservation, OperationPhase, OperationStream,
    ProviderAssurance, ProviderError, SuspensionPolicy, UsageReceipt,
};
use async_trait::async_trait;
use futures::StreamExt as _;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use api::{
    CreateSandboxRequest, EdgeError, ExecuteRequest, ExecuteResponse, ForkRequest, PrizedApi,
    Sandbox,
};
use ops::{Admission, OperationRegistry};

/// Default Prized control-plane base URL.
pub const DEFAULT_API_URL: &str = "https://api.prized.dev/api/v1";
/// Host that serves sandbox edges (`toolboxBase`) for the default API.
pub const DEFAULT_EDGE_HOST: &str = "term.prized.dev";
/// Most children one Prized fork call makes.
pub const MAX_FORK_COUNT: u32 = 256;
/// What every Prized template offers: memory checkpoints (a paused live copy), live fork of a
/// machine (memory, processes and disk, many children in one call), and pause that keeps
/// memory.
pub const CAPABILITIES: [Capability; 3] = [
    Capability::LiveCheckpoint,
    Capability::LiveFork,
    Capability::SuspendResume,
];

/// Most extra labels a provider adds to every sandbox ([`PrizedConfig::labels`]), leaving
/// room in Prized's 32 for the provider's own.
pub const MAX_EXTRA_LABELS: usize = 8;

/// Slack on top of a guest command's own deadline for the edge's wake wait and the network.
const EDGE_GRACE: Duration = Duration::from_secs(30);
/// A `MaxAge` machine this close to its end is not woken: Prized counts run time in minutes,
/// and the end must still be in the future, by Prized's clock, when the wake pins it.
const WAKE_MARGIN_MS: u64 = 60_000;

/// Provider configuration.
#[derive(Clone)]
pub struct PrizedConfig {
    /// Prized API key (`dcp_...`) with Sandboxes: Write.
    pub api_key: String,
    /// Control-plane base URL; defaults to [`DEFAULT_API_URL`].
    pub api_url: String,
    /// Hosts, besides the API host, that may receive the key as a sandbox edge
    /// (`toolboxBase`); defaults to [`DEFAULT_EDGE_HOST`].
    pub edge_hosts: Vec<String>,
    /// Template booted for an image digest not in `templates`; `None` makes such images
    /// unsupported.
    pub default_template: Option<String>,
    /// Registered image digests (lowercase hex) mapped to Prized template ids.
    pub templates: BTreeMap<String, String>,
    /// Network-policy digests (lowercase hex) the operator commits to Prized's one network
    /// policy: outbound internet, nothing inbound, no traffic between sandboxes. A request
    /// committing to any other digest is refused.
    pub network_policies: BTreeSet<String>,
    /// Shape of every sandbox; `None` takes the template's default.
    pub resources: Option<api::Resources>,
    /// Tenant recorded in the `acyclic.tenant` label (and idempotency keys) of every sandbox:
    /// 1 to 48 of `A-Z a-z 0-9 . _ -`.
    pub tenant: Option<String>,
    /// Extra labels on every sandbox, for the operator's own filters and cleanup: at most
    /// [`MAX_EXTRA_LABELS`], none under `acyclic.`.
    pub labels: BTreeMap<String, String>,
    /// Interval between state polls.
    pub poll_interval: Duration,
    /// Longest wait for a sandbox to settle before reporting indeterminate.
    pub ready_timeout: Duration,
    /// Per-request HTTP timeout on the control plane.
    pub request_timeout: Duration,
}

impl PrizedConfig {
    /// Configuration with defaults for everything but the key.
    #[must_use]
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            api_url: DEFAULT_API_URL.to_owned(),
            edge_hosts: vec![DEFAULT_EDGE_HOST.to_owned()],
            default_template: None,
            templates: BTreeMap::new(),
            network_policies: BTreeSet::new(),
            resources: None,
            tenant: None,
            labels: BTreeMap::new(),
            poll_interval: Duration::from_millis(250),
            ready_timeout: Duration::from_secs(180),
            request_timeout: Duration::from_secs(60),
        }
    }

    /// Reads `PRIZED_API_KEY` (required), `PRIZED_API_URL`, `PRIZED_TEMPLATE`,
    /// `PRIZED_TENANT`, and `PRIZED_LABELS` (`key=value,key=value`). Network policies are
    /// registered in code with
    /// [`Self::register_network_policy`].
    ///
    /// # Errors
    /// Returns [`ProviderError::Invalid`] when `PRIZED_API_KEY` is unset or empty.
    pub fn from_env() -> Result<Self, ProviderError> {
        let read = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let api_key = read("PRIZED_API_KEY")
            .ok_or_else(|| ProviderError::Invalid("PRIZED_API_KEY is not set".into()))?;
        let mut config = Self::new(api_key);
        if let Some(url) = read("PRIZED_API_URL") {
            config.api_url = url;
        }
        config.default_template = read("PRIZED_TEMPLATE");
        config.tenant = read("PRIZED_TENANT");
        if let Some(labels) = read("PRIZED_LABELS") {
            for pair in labels.split(',').filter(|pair| !pair.trim().is_empty()) {
                let (name, value) = pair.split_once('=').ok_or_else(|| {
                    ProviderError::Invalid("PRIZED_LABELS must be key=value,key=value".into())
                })?;
                config
                    .labels
                    .insert(name.trim().to_owned(), value.trim().to_owned());
            }
        }
        Ok(config)
    }

    /// Registers the template that backs one image digest.
    pub fn register_template(&mut self, digest: [u8; 32], template: impl Into<String>) {
        self.templates.insert(map::hex(&digest), template.into());
    }

    /// Commits `digest` to Prized's network policy (outbound internet, nothing inbound, no
    /// traffic between sandboxes). Prized has no per-sandbox egress lists, so register only a
    /// digest whose policy that is.
    pub fn register_network_policy(&mut self, digest: [u8; 32]) {
        self.network_policies.insert(map::hex(&digest));
    }
}

impl std::fmt::Debug for PrizedConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrizedConfig")
            .field("api_key", &"<redacted>")
            .field("api_url", &self.api_url)
            .field("edge_hosts", &self.edge_hosts)
            .field("default_template", &self.default_template)
            .field("templates", &self.templates)
            .field("network_policies", &self.network_policies)
            .field("resources", &self.resources)
            .field("tenant", &self.tenant)
            .field("labels", &self.labels)
            .finish_non_exhaustive()
    }
}

/// Canonical intent of one mutation, digested for idempotency-key rebinding checks.
#[derive(Clone, Debug, Serialize)]
enum Intent {
    Create(CreateMachine),
    Checkpoint(MachineId),
    Fork(CheckpointId, u32),
    ForkMachine(MachineId, u32),
    Suspend(MachineId),
    Wake(MachineId),
    DestroyMachine(MachineId),
    DestroyCheckpoint(CheckpointId),
}

/// One fork call and how to observe its children.
struct ForkPlan<'a> {
    source: &'a str,
    count: u32,
    labels: BTreeMap<String, String>,
    contract: &'a MachineContract,
    checkpoint: Option<CheckpointId>,
}

/// Machines provider over Prized Sandboxes.
pub struct PrizedProvider {
    config: PrizedConfig,
    api: PrizedApi,
    ops: OperationRegistry,
    boot_env: Mutex<BTreeMap<IdempotencyKey, BTreeMap<String, String>>>,
    templates: Mutex<Option<BTreeSet<String>>>,
}

/// Staged boot environments hold credentials; only their count is ever rendered.
impl std::fmt::Debug for PrizedProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let staged = self
            .boot_env
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        formatter
            .debug_struct("PrizedProvider")
            .field("config", &self.config)
            .field("api", &self.api)
            .field("ops", &self.ops)
            .field("staged_boot_envs", &staged)
            .finish_non_exhaustive()
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl PrizedProvider {
    /// Binds a provider to one Prized workspace (the API key's).
    ///
    /// # Errors
    /// Returns [`ProviderError::Invalid`] for an empty key, a malformed tenant, or a client
    /// that cannot be built.
    pub fn new(config: PrizedConfig) -> Result<Self, ProviderError> {
        if let Some(tenant) = &config.tenant {
            let allowed = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-');
            if tenant.is_empty() || tenant.len() > 48 || !tenant.bytes().all(allowed) {
                return Err(ProviderError::Invalid(
                    "tenant must be 1 to 48 of A-Z a-z 0-9 . _ -".into(),
                ));
            }
        }
        let reserved = |name: &str| name.starts_with("acyclic.");
        if config.labels.len() > MAX_EXTRA_LABELS
            || config.labels.iter().any(|(name, value)| {
                name.is_empty()
                    || name.len() > 64
                    || reserved(name)
                    || value.chars().count() > map::MAX_LABEL_VALUE
            })
        {
            return Err(ProviderError::Invalid(format!(
                "at most {MAX_EXTRA_LABELS} extra labels, keys of 1 to 64 characters outside acyclic.*"
            )));
        }
        let api = PrizedApi::new(&config)?;
        Ok(Self {
            config,
            api,
            ops: OperationRegistry::default(),
            boot_env: Mutex::default(),
            templates: Mutex::default(),
        })
    }

    /// Stages the environment (`envVars`) for the create under `key`; the create clears it.
    /// Prized stores it encrypted and never returns it; fork children inherit it.
    pub fn stage_boot_env(&self, key: IdempotencyKey, env: BTreeMap<String, String>) {
        lock(&self.boot_env).insert(key, env);
    }

    /// Configuration in use.
    #[must_use]
    pub fn config(&self) -> &PrizedConfig {
        &self.config
    }

    /// Raw Prized client, for calls outside the Machines contract.
    #[must_use]
    pub fn api(&self) -> &PrizedApi {
        &self.api
    }

    /// Operation registry in use.
    #[must_use]
    pub fn registry(&self) -> &OperationRegistry {
        &self.ops
    }

    /// Opaque compatibility revision of this provider build.
    #[must_use]
    pub fn revision() -> [u8; 32] {
        Sha256::digest(b"acyclic-machines-prized-v1").into()
    }

    fn tenant(&self) -> Option<&str> {
        self.config.tenant.as_deref()
    }

    /// Lists the sandboxes this provider manages for its tenant that also carry `labels`.
    /// Prized filters by label; the tenant check here also drops sandboxes of a provider
    /// configured with no tenant, which no label selector can express.
    async fn list_owned(&self, labels: &[(&str, &str)]) -> Result<Vec<Sandbox>, ProviderError> {
        let tenant = self.tenant();
        let mut selector = vec![(map::LABEL_MANAGED, "true")];
        if let Some(tenant) = tenant {
            selector.push((map::LABEL_TENANT, tenant));
        }
        selector.extend_from_slice(labels);
        Ok(self
            .api
            .list(&selector)
            .await?
            .into_iter()
            .filter(|sandbox| map::owned_by(sandbox, tenant))
            .collect())
    }

    /// Prized `Idempotency-Key` for a mutation under `key`, scoped to the tenant.
    fn http_key(&self, key: IdempotencyKey) -> String {
        match self.tenant() {
            Some(tenant) => format!("acyclic/{tenant}/{key}"),
            None => format!("acyclic/{key}"),
        }
    }

    fn resolve_template(&self, image: &Image) -> Result<String, ProviderError> {
        match image {
            Image::Checkpoint(_) => Err(ProviderError::Unsupported(
                "Prized restores a checkpoint only by forking it; use fork".into(),
            )),
            Image::ManagedOci(digest) | Image::Custom(digest) => {
                let key = map::hex(&digest.as_bytes());
                self.config
                    .templates
                    .get(&key)
                    .or(self.config.default_template.as_ref())
                    .cloned()
                    .ok_or_else(|| {
                        ProviderError::Unsupported(format!(
                            "image digest {key} is not registered as a Prized template and no default template is configured"
                        ))
                    })
            }
        }
    }

    /// Requires `template` among the workspace's templates (read once and cached).
    async fn check_template(&self, template: &str) -> Result<(), ProviderError> {
        let cached = lock(&self.templates).clone();
        let known = if let Some(known) = cached {
            known
        } else {
            let fetched: BTreeSet<String> = self
                .api
                .templates()
                .await?
                .into_iter()
                .map(|value| value.id)
                .collect();
            *lock(&self.templates) = Some(fetched.clone());
            fetched
        };
        if known.contains(template) {
            Ok(())
        } else {
            Err(ProviderError::Unsupported(format!(
                "Prized has no template {template:?}"
            )))
        }
    }

    /// The contract a create request gets, or why Prized cannot honour it. Needs no request.
    fn contract(&self, request: &CreateMachine) -> Result<MachineContract, ProviderError> {
        if request.budgets.spend_micros != 0 || request.budgets.concurrency != 0 {
            return Err(ProviderError::Unsupported(
                "Prized enforces spend and concurrency limits per workspace, not per machine; leave budgets zero".into(),
            ));
        }
        map::max_runtime_minutes(request.expiration)?;
        map::auto_pause_minutes(request.suspension)?;
        let digest = map::hex(&request.network_policy_digest);
        if !self.config.network_policies.contains(&digest) {
            return Err(ProviderError::Unsupported(format!(
                "network policy {digest} is not registered; Prized offers one policy (outbound internet, nothing inbound)"
            )));
        }
        let capabilities = BTreeSet::from(CAPABILITIES);
        if let CompatibilityPolicy::Require(required) = &request.compatibility
            && (required.is_empty() || !required.is_subset(&capabilities))
        {
            return Err(ProviderError::Unsupported(format!(
                "Prized offers only {capabilities:?}"
            )));
        }
        Ok(MachineContract {
            image: request.image.clone(),
            capabilities,
            compatibility: request.compatibility.clone(),
            compatibility_revision: Self::revision(),
            suspension: request.suspension,
            expiration: request.expiration,
            network_policy_digest: request.network_policy_digest,
            budgets: request.budgets,
        })
    }

    fn labels(
        &self,
        key: IdempotencyKey,
        kind: &str,
        contract: &MachineContract,
        lineage: &[(&str, String)],
    ) -> Result<BTreeMap<String, String>, ProviderError> {
        let mut labels = self.config.labels.clone();
        labels.extend(map::labels(key, kind, self.tenant(), contract)?);
        labels.extend(
            lineage
                .iter()
                .map(|(name, value)| ((*name).to_owned(), value.clone())),
        );
        Ok(labels)
    }

    /// Records a sandbox observation, filling gaps from what the registry already knows.
    fn observe(
        &self,
        sandbox: &Sandbox,
        fallback: Option<&MachineContract>,
        last_checkpoint: Option<CheckpointId>,
    ) -> Result<MachineObservation, ProviderError> {
        // Another tenant's sandbox, one this provider did not make, or a checkpoint's paused
        // copy is reported as absent, so no read path exposes it as a machine.
        if !map::owned_by(sandbox, self.tenant()) || map::kind(sandbox) == map::KIND_CHECKPOINT {
            return Err(ProviderError::NotFound(sandbox.id.clone()));
        }
        let known = self.ops.machine(map::machine_id(&sandbox.id)?);
        let fallback = fallback.or(known.as_ref().map(|value| &value.contract));
        let last_checkpoint =
            last_checkpoint.or(known.as_ref().and_then(|value| value.last_checkpoint));
        let now = now_unix_ms();
        let mut observation = map::sandbox_to_observation(sandbox, fallback, last_checkpoint, now)?;
        if let Some(known) = known {
            observation.changed_at_unix_ms = if known.state == observation.state {
                known.changed_at_unix_ms
            } else {
                now
            };
        }
        self.ops.observe_machine(observation.clone(), now);
        Ok(observation)
    }

    /// Reads a machine. Prized answers 404 once a sandbox is deleted, so a known machine
    /// that is gone is reported destroyed.
    async fn fetch_machine(&self, machine: MachineId) -> Result<MachineObservation, ProviderError> {
        let sandbox_id = map::machine_sandbox(machine)?;
        match self.api.get(&sandbox_id).await {
            Ok(sandbox) => self.observe(&sandbox, None, None),
            Err(ProviderError::NotFound(_)) => self
                .ops
                .mark_destroyed(machine, now_unix_ms())
                .ok_or_else(|| ProviderError::NotFound(machine.to_string())),
            Err(error) => Err(error),
        }
    }

    /// Polls from `sandbox` until its state settles.
    async fn settle(
        &self,
        mut sandbox: Sandbox,
        key: IdempotencyKey,
    ) -> Result<Sandbox, ProviderError> {
        let deadline = tokio::time::Instant::now() + self.config.ready_timeout;
        loop {
            if map::is_settled(sandbox.state.as_deref()) {
                if map::machine_state(sandbox.state.as_deref()) == MachineState::Failed {
                    tracing::error!(sandbox_id = sandbox.id.as_str(), reason = ?sandbox.error_reason, "Prized sandbox failed");
                    return Err(ProviderError::Failed);
                }
                return Ok(sandbox);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ProviderError::Indeterminate(key));
            }
            tokio::time::sleep(self.config.poll_interval).await;
            sandbox = self
                .read_until(&sandbox.id, deadline, key)
                .await?
                .ok_or_else(|| ProviderError::NotFound(sandbox.id.clone()))?;
        }
    }

    /// Reads a sandbox inside a wait loop. A throttled or failed read (`429`, `5xx`, no
    /// answer) asked nothing of the sandbox, so it is retried until `deadline` instead of
    /// failing the mutation being waited on. `None` once the sandbox is gone.
    async fn read_until(
        &self,
        sandbox_id: &str,
        deadline: tokio::time::Instant,
        key: IdempotencyKey,
    ) -> Result<Option<Sandbox>, ProviderError> {
        loop {
            match self.api.get(sandbox_id).await {
                Ok(sandbox) => return Ok(Some(sandbox)),
                Err(ProviderError::NotFound(_)) => return Ok(None),
                Err(ProviderError::Unavailable) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(self.config.poll_interval).await;
                }
                Err(ProviderError::Unavailable) => return Err(ProviderError::Indeterminate(key)),
                Err(error) => return Err(error),
            }
        }
    }

    /// Reads `sandbox_id` until it settles, and requires it running.
    async fn running(
        &self,
        sandbox_id: &str,
        key: IdempotencyKey,
    ) -> Result<Sandbox, ProviderError> {
        let deadline = tokio::time::Instant::now() + self.config.ready_timeout;
        let first = self
            .read_until(sandbox_id, deadline, key)
            .await?
            .ok_or_else(|| ProviderError::NotFound(sandbox_id.to_owned()))?;
        let settled = self.settle(first, key).await?;
        if map::machine_state(settled.state.as_deref()) == MachineState::Running {
            Ok(settled)
        } else {
            Err(ProviderError::Failed)
        }
    }

    /// Reads `sandbox_id` until Prized answers 404, which it does once a delete has finished.
    async fn wait_gone(&self, sandbox_id: &str, key: IdempotencyKey) -> Result<(), ProviderError> {
        let deadline = tokio::time::Instant::now() + self.config.ready_timeout;
        loop {
            match self.read_until(sandbox_id, deadline, key).await? {
                None => return Ok(()),
                Some(_) if tokio::time::Instant::now() >= deadline => {
                    return Err(ProviderError::Indeterminate(key));
                }
                Some(_) => tokio::time::sleep(self.config.poll_interval).await,
            }
        }
    }

    /// Brings a checkpoint's copy to `paused`, pausing it if a replayed checkpoint left it
    /// running.
    async fn hold_paused(
        &self,
        sandbox_id: &str,
        key: IdempotencyKey,
    ) -> Result<(), ProviderError> {
        let current = self.settle(self.api.get(sandbox_id).await?, key).await?;
        match map::machine_state(current.state.as_deref()) {
            MachineState::Suspended => return Ok(()),
            MachineState::Running => {}
            _ => {
                return Err(ProviderError::Conflict(format!(
                    "checkpoint sandbox {sandbox_id} is {:?}",
                    current.state
                )));
            }
        }
        let settled = self.settle(self.api.pause(sandbox_id).await?, key).await?;
        if map::machine_state(settled.state.as_deref()) == MachineState::Suspended {
            Ok(())
        } else {
            Err(ProviderError::Failed)
        }
    }

    /// Keeps a woken machine's `MaxAge` exact. Prized gives a sandbox a fresh run time at every
    /// resume, so after a wake its end is set back to the machine's creation plus its age
    /// whenever Prized's is later. Only ever earlier, which the free tier also allows.
    async fn pin_deadline(&self, sandbox: &Sandbox, deadline_ms: u64) -> Result<(), ProviderError> {
        let ends = sandbox
            .expires_at
            .as_deref()
            .and_then(map::parse_rfc3339_ms);
        if ends.is_some_and(|ends| ends <= deadline_ms) {
            return Ok(());
        }
        self.api
            .timeout(&sandbox.id, &map::format_rfc3339_ms(deadline_ms))
            .await
            .map(|_| ())
    }

    /// Deletes, newest first and best-effort, the sandboxes a failed operation made. Any that
    /// cannot be deleted stay recorded, so a later `cancel` retries them.
    async fn roll_back(&self, operation: OperationId) {
        let mut remaining = Vec::new();
        for sandbox in self.ops.take_created(operation).into_iter().rev() {
            match self.api.delete(&sandbox).await {
                Ok(()) | Err(ProviderError::NotFound(_)) => {}
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = sandbox,
                        error.kind = error.kind(),
                        "rollback delete failed"
                    );
                    remaining.push(sandbox);
                }
            }
        }
        remaining.reverse();
        self.ops.restore_created(operation, remaining);
    }

    /// Records a sandbox the operation made, or (`adopted`) one Prized replayed from an
    /// earlier attempt under the same key, which cancellation and rollback never delete: it
    /// may already have been reported as that key's outcome before a restart. When the
    /// operation was cancelled while the request was in flight, a sandbox it made is deleted
    /// here and the mutation ends as cancelled.
    async fn claim(
        &self,
        operation: OperationId,
        sandbox_id: &str,
        adopted: bool,
    ) -> Result<(), ProviderError> {
        if self.ops.bind(operation, sandbox_id, adopted) {
            return Ok(());
        }
        if !adopted && let Err(error) = self.api.delete(sandbox_id).await {
            tracing::warn!(
                sandbox_id,
                error.kind = error.kind(),
                "delete of a sandbox made after cancellation failed"
            );
        }
        Err(ProviderError::Cancelled)
    }

    /// Runs one mutation under `key`: replays a prior identical success, rejects a rebound
    /// key, and records the outcome. A refusal before any Prized side effect releases the key
    /// so a corrected request can reuse it; a definite failure deletes what it made.
    async fn apply<F>(
        &self,
        key: IdempotencyKey,
        intent: &Intent,
        action: F,
    ) -> Result<MutationOutcome, ProviderError>
    where
        F: AsyncFnOnce(OperationId) -> Result<MutationOutcome, ProviderError>,
    {
        let operation = match self.ops.admit(key, intent)? {
            Admission::Replay(outcome) => return Ok(*outcome),
            Admission::Fresh(operation) => operation,
        };
        match action(operation).await {
            Ok(outcome) => {
                self.ops.complete(operation, outcome.clone());
                match self.ops.inspect(operation).map(|value| value.phase) {
                    Some(OperationPhase::Cancelled) => Err(ProviderError::Cancelled),
                    _ => Ok(outcome),
                }
            }
            Err(error) => {
                let touched = self
                    .ops
                    .record(operation)
                    .is_some_and(|record| !record.created.is_empty() || !record.targets.is_empty());
                let refused = matches!(
                    error,
                    ProviderError::Invalid(_)
                        | ProviderError::Unsupported(_)
                        | ProviderError::NotFound(_)
                        | ProviderError::Conflict(_)
                        | ProviderError::Rejected(_)
                );
                if !touched && refused {
                    self.ops.forget(operation);
                } else {
                    self.ops.fail(operation, &error);
                    if !matches!(
                        error,
                        ProviderError::Indeterminate(_)
                            | ProviderError::Unavailable
                            | ProviderError::Cancelled
                    ) {
                        self.roll_back(operation).await;
                    }
                }
                Err(error)
            }
        }
    }

    /// One Prized fork call making every child at once, then waiting for each to run. A
    /// replayed answer (an earlier attempt under the same key) adopts its children.
    async fn fork_children(
        &self,
        operation: OperationId,
        key: IdempotencyKey,
        plan: ForkPlan<'_>,
    ) -> Result<Vec<MachineObservation>, ProviderError> {
        let body = ForkRequest {
            count: plan.count,
            labels: plan.labels,
        };
        let (fork, replayed) = self
            .api
            .fork(plan.source, &body, &self.http_key(key))
            .await?;
        for id in &fork.sandbox_ids {
            self.claim(operation, id, replayed).await?;
        }
        if fork.sandbox_ids.len() != usize::try_from(plan.count).unwrap_or(usize::MAX) {
            return Err(ProviderError::Rejected(format!(
                "Prized answered {} children for a fork of {}",
                fork.sandbox_ids.len(),
                plan.count
            )));
        }
        let settled =
            futures::future::join_all(fork.sandbox_ids.iter().map(|id| self.running(id, key)))
                .await;
        settled
            .into_iter()
            .map(|sandbox| self.observe(&sandbox?, Some(plan.contract), plan.checkpoint))
            .collect()
    }

    fn checkpoint_observation(
        &self,
        sandbox: &Sandbox,
    ) -> Result<CheckpointObservation, ProviderError> {
        if !map::owned_by(sandbox, self.tenant()) || map::kind(sandbox) != map::KIND_CHECKPOINT {
            return Err(ProviderError::NotFound(sandbox.id.clone()));
        }
        let unreadable =
            || ProviderError::Rejected(format!("checkpoint {} has unreadable labels", sandbox.id));
        let source = sandbox
            .labels
            .get(map::LABEL_PARENT)
            .and_then(|value| MachineId::parse(value).ok())
            .ok_or_else(unreadable)?;
        let contract = map::contract_from_labels(&sandbox.labels).ok_or_else(unreadable)?;
        let state = map::machine_state(sandbox.state.as_deref());
        Ok(CheckpointObservation {
            id: map::checkpoint_id(&sandbox.id)?,
            source,
            contract,
            forkable: !matches!(
                state,
                MachineState::Destroying | MachineState::Destroyed | MachineState::Failed
            ),
            created_at_unix_ms: sandbox
                .created_at
                .as_deref()
                .and_then(map::parse_rfc3339_ms)
                .unwrap_or_else(now_unix_ms),
        })
    }

    async fn recover_fork(
        &self,
        key: IdempotencyKey,
        child: &Sandbox,
    ) -> Result<MutationOutcome, ProviderError> {
        let indeterminate = |_| ProviderError::Indeterminate(key);
        let live = map::kind(child) == map::KIND_LIVE_FORK;
        let label = |name: &str| {
            child
                .labels
                .get(name)
                .cloned()
                .ok_or(ProviderError::Indeterminate(key))
        };
        let count = label(map::LABEL_COUNT)?
            .parse::<u32>()
            .map_err(|_| ProviderError::Indeterminate(key))?;
        let (parent, source) = if live {
            let parent = MachineId::parse(&label(map::LABEL_PARENT)?)
                .map_err(|_| ProviderError::Indeterminate(key))?;
            (
                Some(parent),
                map::machine_sandbox(parent).map_err(indeterminate)?,
            )
        } else {
            let checkpoint = CheckpointId::parse(&label(map::LABEL_CHECKPOINT)?)
                .map_err(|_| ProviderError::Indeterminate(key))?;
            (
                None,
                map::checkpoint_sandbox(checkpoint).map_err(indeterminate)?,
            )
        };
        // The exact request again: Prized answers it from its idempotency record, children in
        // fork order, even after the parent moved on. A fork's labels replace the parent's on
        // every child, so a child's labels are the request's.
        let body = ForkRequest {
            count,
            labels: child.labels.clone(),
        };
        let (fork, replayed) = self
            .api
            .fork(&source, &body, &self.http_key(key))
            .await
            .map_err(indeterminate)?;
        if !replayed {
            tracing::error!(
                fork_id = fork.id.as_str(),
                "recovery fork was not a replay; deleting its children"
            );
            for id in &fork.sandbox_ids {
                let _ = self.api.delete(id).await;
            }
            return Err(ProviderError::Indeterminate(key));
        }
        let mut children = Vec::with_capacity(fork.sandbox_ids.len());
        for id in &fork.sandbox_ids {
            let sandbox = self.api.get(id).await.map_err(indeterminate)?;
            children.push(self.observe(&sandbox, None, None)?);
        }
        match parent {
            Some(source) => Ok(MutationOutcome::MachineForked {
                source,
                fidelity: ForkFidelity::MemoryAndDisk,
                children,
            }),
            None => Ok(MutationOutcome::Forked(children)),
        }
    }

    async fn recover_by_label(
        &self,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let key_text = key.to_string();
        let sandboxes = self.list_owned(&[(map::LABEL_KEY, &key_text)]).await?;
        let Some(first) = sandboxes.first() else {
            return Err(ProviderError::NotFound(key_text));
        };
        match (map::kind(first), sandboxes.as_slice()) {
            (map::KIND_CREATE, [only]) => {
                Ok(MutationOutcome::Created(self.observe(only, None, None)?))
            }
            (map::KIND_CHECKPOINT, [only]) => Ok(MutationOutcome::Checkpointed(
                self.checkpoint_observation(only)?,
            )),
            (map::KIND_LIVE_FORK | map::KIND_FORK, _) => self.recover_fork(key, first).await,
            (map::KIND_CREATE | map::KIND_CHECKPOINT, _) => Err(ProviderError::Indeterminate(key)),
            (other, _) => Err(ProviderError::Rejected(format!(
                "sandbox under key {key} has unknown kind {other:?}"
            ))),
        }
    }
}

fn require_capability(
    machine: &MachineObservation,
    capability: Capability,
) -> Result<(), ProviderError> {
    if machine.contract.capabilities.contains(&capability) {
        Ok(())
    } else {
        Err(ProviderError::Unsupported(format!(
            "machine contract does not declare {capability:?}"
        )))
    }
}

/// An edge refusal after which nothing ran.
fn edge_refusal(error: &EdgeError) -> ProviderError {
    match error {
        EdgeError::Transport => ProviderError::Unavailable,
        EdgeError::Status { status, code } => match status {
            404 => ProviderError::NotFound(code.clone()),
            409 => ProviderError::Conflict(code.clone()),
            400 | 413 => ProviderError::Invalid(code.clone()),
            429 | 500..=599 => ProviderError::Unavailable,
            _ => ProviderError::Rejected(format!("{status} {code}")),
        },
    }
}

/// Guest output as SDK frames: merged output in chunks of at most 64 KiB, then the exit code.
/// A command the deadline killed did run; like a gRPC deadline, it ends indeterminate.
fn exec_frames(machine: MachineId, answer: ExecuteResponse) -> ExecStream {
    let chunk = usize::try_from(MAX_OUTPUT_READ_BYTES).unwrap_or(usize::MAX);
    let mut output = answer.stdout.into_bytes();
    output.extend(answer.stderr.into_bytes());
    let mut frames: Vec<Result<ExecOutput, ProviderError>> = output
        .chunks(chunk)
        .map(|data| Ok(ExecOutput::Data(data.to_vec())))
        .collect();
    frames.push(if answer.timed_out {
        Err(ProviderError::CommandIndeterminate(machine))
    } else {
        Ok(ExecOutput::ExitCode(answer.exit_code))
    });
    futures::stream::iter(frames).boxed()
}

fn usage_receipt(
    machine: MachineId,
    sandbox: &Sandbox,
    interval: (u64, u64),
    lifetime: (u64, u64),
) -> Result<UsageReceipt, ProviderError> {
    #[derive(Serialize)]
    struct Payload<'a> {
        provider: &'static str,
        basis: &'static str,
        provisional: bool,
        sandbox_id: &'a str,
        start_unix_ms: u64,
        end_unix_ms: u64,
        overlap_ms: u64,
        resources: Option<api::Resources>,
    }
    const MIB: u128 = 1 << 20;
    const GIB: u128 = 1 << 30;
    let (start_unix_ms, end_unix_ms) = interval;
    let overlap_ms = end_unix_ms
        .min(lifetime.1)
        .saturating_sub(start_unix_ms.max(lifetime.0));
    let overlap = u128::from(overlap_ms);
    let shape = sandbox.resources.unwrap_or_default();
    let saturate = |value: u128| u64::try_from(value).unwrap_or(u64::MAX);
    let receipt = serde_json::to_vec(&Payload {
        provider: "prized",
        basis: "allocation",
        provisional: true,
        sandbox_id: &sandbox.id,
        start_unix_ms,
        end_unix_ms,
        overlap_ms,
        resources: sandbox.resources,
    })
    .map_err(|error| ProviderError::Invalid(format!("usage receipt cannot be encoded: {error}")))?;
    Ok(UsageReceipt {
        machine,
        start_unix_ms,
        end_unix_ms,
        elastic_cpu_ns: saturate(u128::from(shape.vcpu.unwrap_or(0)) * overlap * 1_000_000),
        dedicated_cpu_ns: 0,
        private_resident_byte_seconds: saturate(
            u128::from(shape.memory_mb.unwrap_or(0)) * MIB * overlap / 1_000,
        ),
        durable_private_bytes: if overlap_ms == 0 {
            0
        } else {
            saturate(u128::from(shape.disk_gb.unwrap_or(0)) * GIB)
        },
        // Prized issues no signed lineage receipt; this commits to the estimate itself.
        lineage_receipt_sha256: Sha256::digest(&receipt).into(),
        egress_bytes: 0,
        receipt,
    })
}

#[async_trait]
impl MachinesProvider for PrizedProvider {
    /// Prized runs in the customer's own Prized workspace, each sandbox its own Firecracker
    /// microVM; `CustomerHosted` is the closest of the three declared classes. It is not an
    /// Acyclic-operated managed service.
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::CustomerHosted
    }

    /// Runs `argv` once through the sandbox edge (`POST /process/execute`), without shell
    /// evaluation of its words and with stderr merged into stdout. Prized returns output when
    /// the command ends, so the stream yields it then, in 64 KiB frames. A running sandbox
    /// takes commands at once, so nothing is sent first. Never wakes a suspended machine.
    async fn exec(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        let sandbox_id = map::machine_sandbox(machine)?;
        let sandbox = self.api.get(&sandbox_id).await?;
        if self.observe(&sandbox, None, None)?.state != MachineState::Running {
            return Err(ProviderError::Conflict(
                "guest commands run only on a running machine; wake it first".into(),
            ));
        }
        let edge = sandbox
            .toolbox_base
            .as_deref()
            .ok_or_else(|| ProviderError::Rejected("sandbox reports no edge address".into()))?;
        let request = ExecuteRequest {
            command: map::exec_command_line(command.argv()),
            timeout: Some(f64::from(command.timeout_ms()) / 1_000.0),
        };
        let deadline = Duration::from_millis(u64::from(command.timeout_ms())) + EDGE_GRACE;
        match self.api.execute(edge, &request, deadline).await? {
            Ok(answer) => Ok(exec_frames(machine, answer)),
            Err(error) if error.may_have_run() => Err(ProviderError::CommandIndeterminate(machine)),
            Err(error) => Err(edge_refusal(&error)),
        }
    }

    async fn read_output(
        &self,
        _machine: MachineId,
        _offset: u64,
        _limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        Err(ProviderError::Unsupported(
            "Prized keeps no retained console; read files through exec".into(),
        ))
    }

    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        let template = self.resolve_template(&image)?;
        self.check_template(&template).await?;
        Ok(ImageQualification {
            image,
            capabilities: BTreeSet::from(CAPABILITIES),
            compatibility_revision: Self::revision(),
        })
    }

    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let intent = Intent::Create(request.clone());
        let outcome = self
            .apply(key, &intent, async |operation| {
                let template = self.resolve_template(&request.image)?;
                let contract = self.contract(&request)?;
                self.check_template(&template).await?;
                let body = CreateSandboxRequest {
                    template,
                    resources: self.config.resources,
                    env_vars: lock(&self.boot_env).get(&key).cloned().unwrap_or_default(),
                    labels: self.labels(key, map::KIND_CREATE, &contract, &[])?,
                    auto_pause_min: map::auto_pause_minutes(contract.suspension)?,
                    max_runtime_min: map::max_runtime_minutes(contract.expiration)?,
                };
                let (created, replayed) = self.api.create(&body, &self.http_key(key)).await?;
                self.claim(operation, &created.id, replayed).await?;
                let settled = self.settle(created, key).await?;
                let observation = self.observe(&settled, Some(&contract), None)?;
                if observation.state != MachineState::Running {
                    return Err(ProviderError::Failed);
                }
                Ok(MutationOutcome::Created(observation))
            })
            .await;
        // The environment is only for the create request; keeping it would retain secrets.
        lock(&self.boot_env).remove(&key);
        outcome
    }

    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        self.fetch_machine(machine).await
    }

    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "machine page limit must be 1..=256".into(),
            ));
        }
        let limit = usize::try_from(limit)
            .map_err(|_| ProviderError::Invalid("invalid page limit".into()))?;
        let mut merged: BTreeMap<MachineId, MachineObservation> = self
            .ops
            .machines()
            .into_iter()
            .map(|value| (value.id, value))
            .collect();
        for sandbox in self.list_owned(&[]).await? {
            if map::kind(&sandbox) == map::KIND_CHECKPOINT {
                continue;
            }
            match self.observe(&sandbox, None, None) {
                Ok(observation) => {
                    merged.insert(observation.id, observation);
                }
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = sandbox.id.as_str(),
                        error.kind = error.kind(),
                        "skipping sandbox that cannot be observed"
                    );
                }
            }
        }
        let mut values: Vec<MachineObservation> = merged
            .into_values()
            .filter(|value| after.is_none_or(|cursor| value.id > cursor))
            .take(limit + 1)
            .collect();
        let next = if values.len() > limit {
            values.pop();
            values.last().map(|value| value.id)
        } else {
            None
        };
        Ok(MachinePage {
            machines: values,
            next,
        })
    }

    /// Forks the running machine once into a copy and pauses the copy at once: the paused copy
    /// holds the machine's memory, processes and disk, bills storage only, and lives on the
    /// source's host, independent of the source. Its run-time clock is stopped while it is
    /// paused, so it is kept until destroyed (on the free tier, 30 days after its last use).
    /// The pause is sent while the copy is still starting and lands as its boot ends, so the
    /// copy runs for about a second past the fork; a workload that holds still meanwhile is
    /// captured exactly.
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::Checkpoint(machine), async |operation| {
            let source = self.fetch_machine(machine).await?;
            require_capability(&source, Capability::LiveCheckpoint)?;
            if source.state != MachineState::Running {
                return Err(ProviderError::Conflict(
                    "only a running machine can be checkpointed".into(),
                ));
            }
            let sandbox_id = map::machine_sandbox(machine)?;
            let labels = self.labels(
                key,
                map::KIND_CHECKPOINT,
                &source.contract,
                &[(map::LABEL_PARENT, machine.to_string())],
            )?;
            let body = ForkRequest { count: 1, labels };
            let (fork, replayed) = self
                .api
                .fork(&sandbox_id, &body, &self.http_key(key))
                .await?;
            self.ops.bind_target(operation, &sandbox_id);
            let [child] = fork.sandbox_ids.as_slice() else {
                return Err(ProviderError::Rejected(
                    "Prized answered the wrong children for a checkpoint".into(),
                ));
            };
            self.claim(operation, child, replayed).await?;
            if replayed {
                self.hold_paused(child, key).await?;
            } else {
                let settled = self.settle(self.api.pause(child).await?, key).await?;
                if map::machine_state(settled.state.as_deref()) != MachineState::Suspended {
                    return Err(ProviderError::Failed);
                }
            }
            let checkpoint = self.checkpoint_observation(&self.api.get(child).await?)?;
            self.ops.remember_checkpoint(checkpoint.clone());
            let now = now_unix_ms();
            let mut updated = source;
            updated.last_checkpoint = Some(checkpoint.id);
            updated.changed_at_unix_ms = now;
            self.ops.observe_machine(updated, now);
            Ok(MutationOutcome::Checkpointed(checkpoint))
        })
        .await
    }

    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        let known = self.ops.checkpoint(checkpoint);
        let sandbox_id = map::checkpoint_sandbox(checkpoint)?;
        match self.api.get(&sandbox_id).await {
            Ok(sandbox) => {
                let mut observation = self.checkpoint_observation(&sandbox)?;
                if known.is_some_and(|known| !known.forkable) {
                    observation.forkable = false;
                }
                self.ops.remember_checkpoint(observation.clone());
                Ok(observation)
            }
            Err(ProviderError::NotFound(_)) => known
                .map(|mut value| {
                    value.forkable = false;
                    value
                })
                .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string())),
            Err(error) => Err(error),
        }
    }

    /// Forks the checkpoint's paused copy into `count` children in one call. Prized copies a
    /// paused sandbox at its pause point and leaves it paused, so every fork of a checkpoint
    /// starts from the same memory, processes and disk, and the copy never runs.
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count = count.get();
        if count > MAX_FORK_COUNT {
            return Err(ProviderError::Unsupported(
                "Prized forks at most 256 children per call".into(),
            ));
        }
        self.apply(key, &Intent::Fork(checkpoint, count), async |operation| {
            let source = self.inspect_checkpoint(checkpoint).await?;
            if !source.forkable {
                return Err(ProviderError::Conflict(
                    "checkpoint no longer accepts forks".into(),
                ));
            }
            let sandbox_id = map::checkpoint_sandbox(checkpoint)?;
            // A copy that is not paused was resumed outside the provider, so its memory has
            // moved on from the checkpoint.
            let copy = self.settle(self.api.get(&sandbox_id).await?, key).await?;
            if map::machine_state(copy.state.as_deref()) != MachineState::Suspended {
                return Err(ProviderError::Conflict(
                    "the checkpoint's paused copy was resumed outside the provider; take a new checkpoint".into(),
                ));
            }
            let mut contract = source.contract;
            contract.image = Image::Checkpoint(checkpoint);
            let lineage = [
                (map::LABEL_CHECKPOINT, checkpoint.to_string()),
                (map::LABEL_COUNT, count.to_string()),
            ];
            let labels = self.labels(key, map::KIND_FORK, &contract, &lineage)?;
            self.ops.bind_target(operation, &sandbox_id);
            let plan = ForkPlan {
                source: &sandbox_id,
                count,
                labels,
                contract: &contract,
                checkpoint: Some(checkpoint),
            };
            let children = self.fork_children(operation, key, plan).await?;
            Ok(MutationOutcome::Forked(children))
        })
        .await
    }

    /// One Prized fork call makes every child: each resumes from the running source's memory,
    /// processes and disk at the same instant. There is no fan-out tree and no per-child call.
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count = count.get();
        if count > MAX_FORK_COUNT {
            return Err(ProviderError::Unsupported(
                "Prized forks at most 256 children per call".into(),
            ));
        }
        self.apply(
            key,
            &Intent::ForkMachine(machine, count),
            async |operation| {
                let parent = self.fetch_machine(machine).await?;
                let fidelity = parent.contract.fork_fidelity().ok_or_else(|| {
                    ProviderError::Unsupported("machine contract does not declare live fork".into())
                })?;
                if parent.state != MachineState::Running {
                    return Err(ProviderError::Conflict(
                        "only a running machine can be forked".into(),
                    ));
                }
                let sandbox_id = map::machine_sandbox(machine)?;
                let lineage = [
                    (map::LABEL_PARENT, machine.to_string()),
                    (map::LABEL_COUNT, count.to_string()),
                ];
                let labels = self.labels(key, map::KIND_LIVE_FORK, &parent.contract, &lineage)?;
                let plan = ForkPlan {
                    source: &sandbox_id,
                    count,
                    labels,
                    contract: &parent.contract,
                    checkpoint: None,
                };
                let children = self.fork_children(operation, key, plan).await?;
                Ok(MutationOutcome::MachineForked {
                    source: machine,
                    fidelity,
                    children,
                })
            },
        )
        .await
    }

    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::Suspend(machine), async |operation| {
            let current = self.fetch_machine(machine).await?;
            require_capability(&current, Capability::SuspendResume)?;
            match current.state {
                MachineState::Suspended => return Ok(MutationOutcome::Suspended(machine)),
                MachineState::Running => {}
                _ => {
                    return Err(ProviderError::Conflict(
                        "only a running machine can be suspended".into(),
                    ));
                }
            }
            let sandbox_id = map::machine_sandbox(machine)?;
            self.ops.bind_target(operation, &sandbox_id);
            let settled = self.settle(self.api.pause(&sandbox_id).await?, key).await?;
            if self.observe(&settled, None, None)?.state != MachineState::Suspended {
                return Err(ProviderError::Failed);
            }
            Ok(MutationOutcome::Suspended(machine))
        })
        .await
    }

    /// Resumes the sandbox with its memory and processes. Prized restarts a resumed sandbox's
    /// run time, so a `MaxAge` machine's end is pinned back to its creation plus its age as
    /// soon as the resume is accepted; if that fails, the machine is paused again and the wake
    /// fails. A machine within a minute of its age (Prized's clock stops while it is
    /// suspended) is not woken. A wake of a running `MaxAge` machine pins its end too, which
    /// repairs a resume made outside the provider.
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::Wake(machine), async |operation| {
            let current = self.fetch_machine(machine).await?;
            require_capability(&current, Capability::SuspendResume)?;
            let sandbox_id = map::machine_sandbox(machine)?;
            let deadline =
                map::deadline_unix_ms(current.contract.expiration, current.created_at_unix_ms);
            match current.state {
                MachineState::Running => {
                    if let Some(deadline) = deadline {
                        if deadline <= now_unix_ms() {
                            return Err(ProviderError::Conflict(
                                "the machine runs past its MaxAge after a resume made outside the provider; destroy it".into(),
                            ));
                        }
                        let sandbox = self.api.get(&sandbox_id).await?;
                        self.pin_deadline(&sandbox, deadline).await?;
                    }
                    return Ok(MutationOutcome::Woken(machine));
                }
                MachineState::Suspended => {}
                _ => {
                    return Err(ProviderError::Conflict(
                        "only a suspended machine can be woken".into(),
                    ));
                }
            }
            if deadline.is_some_and(|deadline| deadline <= now_unix_ms() + WAKE_MARGIN_MS) {
                return Err(ProviderError::Conflict(
                    "the machine reached its MaxAge while suspended; destroy it".into(),
                ));
            }
            self.ops.bind_target(operation, &sandbox_id);
            let resumed = self.api.resume(&sandbox_id).await?;
            if let Some(deadline) = deadline
                && let Err(error) = self.pin_deadline(&resumed, deadline).await
            {
                // Unpinned, the machine would run past its MaxAge: leave it suspended instead.
                if let Err(pause) = self.api.pause(&sandbox_id).await {
                    tracing::warn!(
                        sandbox_id,
                        error.kind = pause.kind(),
                        "pause after a failed MaxAge pin failed; a wake of the running machine pins it"
                    );
                }
                return Err(error);
            }
            let settled = self.settle(resumed, key).await?;
            if self.observe(&settled, None, None)?.state != MachineState::Running {
                return Err(ProviderError::Failed);
            }
            Ok(MutationOutcome::Woken(machine))
        })
        .await
    }

    /// Prized sets `autoPauseMin` at creation and has no route to change it.
    async fn set_suspension_policy(
        &self,
        _machine: MachineId,
        _policy: SuspensionPolicy,
        _key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        Err(ProviderError::Unsupported(
            "Prized fixes a sandbox's automatic pause at creation".into(),
        ))
    }

    /// Deletes the sandbox and waits until Prized no longer lists it, so its quota is free.
    /// Children of a live fork are independent once running and never block it.
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &Intent::DestroyMachine(machine), async |operation| {
            let current = self.fetch_machine(machine).await?;
            if current.state == MachineState::Destroyed {
                return Ok(MutationOutcome::MachineDestroyed(machine));
            }
            let sandbox_id = map::machine_sandbox(machine)?;
            self.ops.bind_target(operation, &sandbox_id);
            match self.api.delete(&sandbox_id).await {
                Ok(()) | Err(ProviderError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
            self.wait_gone(&sandbox_id, key).await?;
            self.ops.mark_destroyed(machine, now_unix_ms());
            Ok(MutationOutcome::MachineDestroyed(machine))
        })
        .await
    }

    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &Intent::DestroyCheckpoint(checkpoint),
            async |operation| {
                // Retain an observation so `inspect_checkpoint` keeps answering after deletion.
                self.inspect_checkpoint(checkpoint).await?;
                let sandbox_id = map::checkpoint_sandbox(checkpoint)?;
                self.ops.bind_target(operation, &sandbox_id);
                match self.api.delete(&sandbox_id).await {
                    Ok(()) | Err(ProviderError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
                self.wait_gone(&sandbox_id, key).await?;
                self.ops.mark_checkpoint_destroyed(checkpoint);
                Ok(MutationOutcome::CheckpointDestroyed(checkpoint))
            },
        )
        .await
    }

    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "event page limit must be 1..=1024".into(),
            ));
        }
        // One poll per call: refreshing the observation appends a state event on change.
        self.fetch_machine(machine).await?;
        let limit = usize::try_from(limit)
            .map_err(|_| ProviderError::Invalid("invalid event limit".into()))?;
        let (events, next_sequence) = self.ops.events(machine, after_sequence, limit);
        Ok(EventPage {
            events,
            next_sequence,
        })
    }

    /// Allocation-based estimate, marked provisional: Prized meters usage per workspace, not
    /// per sandbox through the API. It is an upper bound (a paused sandbox bills storage only).
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        if start_unix_ms >= end_unix_ms {
            return Err(ProviderError::Invalid(
                "usage interval must be non-empty".into(),
            ));
        }
        let sandbox = self.api.get(&map::machine_sandbox(machine)?).await?;
        let observation = self.observe(&sandbox, None, None)?;
        usage_receipt(
            machine,
            &sandbox,
            (start_unix_ms, end_unix_ms),
            (observation.created_at_unix_ms, now_unix_ms()),
        )
    }

    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        if let Some(record) = self.ops.record_for_key(key) {
            return match (record.phase, record.outcome) {
                (OperationPhase::Succeeded, Some(outcome)) => Ok(outcome),
                (OperationPhase::Failed, _) => Err(ProviderError::Failed),
                (OperationPhase::Cancelled, _) => Err(ProviderError::Cancelled),
                _ => Err(ProviderError::Indeterminate(key)),
            };
        }
        self.recover_by_label(key).await
    }

    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        self.ops
            .record_for_key(key)
            .map(|record| record.id)
            .ok_or_else(|| ProviderError::NotFound(key.to_string()))
    }

    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.ops
            .inspect(operation)
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))
    }

    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        let (observation, sandboxes) = self
            .ops
            .cancel(operation)
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
        let mut remaining = Vec::new();
        for sandbox in sandboxes.into_iter().rev() {
            match self.api.delete(&sandbox).await {
                Ok(()) | Err(ProviderError::NotFound(_)) => {}
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = sandbox,
                        error.kind = error.kind(),
                        "delete of a cancelled operation's sandbox failed; cancel again to retry"
                    );
                    remaining.push(sandbox);
                }
            }
        }
        remaining.reverse();
        self.ops.restore_created(operation, remaining);
        Ok(observation)
    }

    /// Mutations complete inline, so the stream yields the current observation and, while it
    /// is still pending, one more once it leaves `Pending` or the ready timeout elapses.
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        let current = self.inspect_operation(operation).await?;
        if current.phase != OperationPhase::Pending {
            return Ok(futures::stream::once(async move { Ok(current) }).boxed());
        }
        let last = self
            .ops
            .watch(
                operation,
                self.config.poll_interval,
                self.config.ready_timeout,
            )
            .await;
        Ok(futures::stream::iter([Ok(current), last]).boxed())
    }
}
