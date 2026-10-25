//! Hosting-contract adapter. External hosts implement `ActorRuntime` and `Volume`.
use crate::{CaseObservation, TestResult};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs};
use acyclic_harness::{
    AgentId, Error, IdempotencyKey, OperationId, Result,
    conversation::FileRef,
    package::{
        ActorPackage, ActorRuntime, Bindings, EffectHost, Event, Manifest, Placement,
        VolumePlacement,
        in_process::{AdmissionAction, AdmissionPolicy, InProcessRuntime, effect_result_operation},
    },
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use acyclic_stream::{BoxProviderFuture, ProviderPlatform, StreamProvider};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

#[cfg(test)]
mod effect_tests;
#[cfg(test)]
mod install_tests;
/// SDK single-node deployment using canonical Streams, fs and Actors services.
pub mod localhost;
mod supervised;
/// Shared immutable publications through the canonical filesystem service.
pub mod volume;
pub use supervised::Supervised;

/// Explicit host capabilities. Native fixture drivers require `local-driver:<name>`.
pub trait Substrate: ProviderPlatform {
    /// Returns explicitly implemented fixture capabilities.
    fn capabilities(&self) -> BTreeSet<String>;
    /// Runs expectation-free fixture input and returns raw observations.
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>>;
}

/// Public fs binding for immutable fixture and result publications.
pub trait Volume: ProviderPlatform {
    /// Publishes immutable JSON under a stable operation identity.
    fn put(&self, operation: OperationId, value: Value) -> BoxProviderFuture<'_, Result<FileRef>>;
    /// Loads and verifies an immutable reference.
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>>;
    /// Reconciles an earlier publication by stable operation identity.
    fn lookup(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<Option<FileRef>>>;
}

#[cfg(test)]
#[derive(Debug)]
struct FixturePublication(String);

fn codec(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value> {
    value
        .get(name)
        .ok_or_else(|| Error::Invalid(format!("missing scenario field {name}")))
}

const FIXTURE_SOURCES: &[(&str, &str)] = &[
    ("policy_owner", include_str!("policy_owner.rs")),
    ("substrate", include_str!("substrate.rs")),
    ("fixture_volume", include_str!("substrate/volume.rs")),
    ("dispatch", include_str!("lib.rs")),
    ("execution_adapter", include_str!("execution.rs")),
    (
        "executor_observation",
        include_str!("execution/observation.rs"),
    ),
    (
        "executor",
        include_str!("../../harness/tests/e2e/execution.rs"),
    ),
    (
        "scenario",
        include_str!("../../harness/tests/e2e/scenario.rs"),
    ),
    (
        "expectations",
        include_str!("../../harness/tests/e2e/expectations.rs"),
    ),
    (
        "discovery",
        include_str!("../../harness/tests/support/discovery.rs"),
    ),
    (
        "scenario_data",
        include_str!("../../harness/tests/support/scenario_data.rs"),
    ),
    (
        "ordered_matcher",
        include_str!("../../harness/tests/support/ordered_matcher.rs"),
    ),
    (
        "scenario_matchers",
        include_str!("../../harness/tests/support/scenario_matchers.rs"),
    ),
    (
        "composer",
        include_str!("../../harness/tests/support/composer.rs"),
    ),
    ("composer_observation", include_str!("composer.rs")),
];

fn fixture_digest(sources: &[(&str, &str)]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for (name, source) in sources {
        hasher.update(name.as_bytes());
        hasher.update(&[0]);
        hasher.update(blake3::hash(source.as_bytes()).as_bytes());
    }
    *hasher.finalize().as_bytes()
}

/// Pinned package shared by reference and external runtimes. The deterministic
/// handler emits a compute command; all executor I/O stays in the effect host.
#[derive(Clone)]
pub struct HarnessPackage {
    identity: MachineIdentity,
    manifest: Manifest,
    schema: Value,
}
impl HarnessPackage {
    /// Pins the fixture implementation and its explicit resource bindings.
    pub fn new(bindings: Bindings) -> Self {
        let identity = MachineIdentity {
            name: "harness.scenario".into(),
            version: "1".into(),
            digest: *PACKAGE_DIGEST.get_or_init(|| fixture_digest(FIXTURE_SOURCES)),
        };
        Self {
            manifest: Manifest {
                actors: vec![identity.clone()],
                effects: vec!["harness.scenario.compute".into()],
                bindings,
            },
            identity,
            schema: json!({"type":"object"}),
        }
    }
}

// Compiled source bytes never change during a host lifetime. This is artifact
// identity, independent of caller authority, placement or live resource policy.
static PACKAGE_DIGEST: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[tokio::test]
    async fn both_hosts_deploy_once_and_isolate_each_actor() -> TestResult {
        let localhost = Arc::new(localhost::Localhost::start().await?);
        let in_process = Arc::new(InProcess::default());
        let hosts: [Arc<dyn Substrate>; 2] = [in_process.clone(), localhost.clone()];
        for (kind, host) in hosts.into_iter().enumerate() {
            for answer in ["first actor", "second actor"] {
                let recipe = json!({"prompt":"Say hello", "turns":[answer]});
                let observed = host.execute(recipe, vec![]).await?;
                let deployments = if kind == 0 {
                    in_process
                        .deployments
                        .load(std::sync::atomic::Ordering::Acquire)
                } else {
                    localhost.deployment_count()?
                };
                assert_eq!(deployments, 1, "case redeployed its host package");
                let CaseObservation::Executor(observed) = observed else {
                    return Err("host returned a different driver".into());
                };
                let case = crate::scenario::Scenario::from_value(json!({
                    "prompt":"Say hello", "turns":[answer], "expect":{"text":answer}
                }))?;
                crate::observation::check(&case, &observed)?;
            }
        }
        Arc::into_inner(localhost)
            .ok_or("localhost still borrowed")?
            .shutdown()
            .await
    }

    #[tokio::test]
    async fn cases_reuse_the_host_provider_with_isolated_actor_storage() -> TestResult {
        let host = Arc::new(InProcess::default());
        let recipe = json!({"prompt":"Say hello", "turns":["Hello from mock LLM!"]});
        assert!(host.providers.get().is_none());
        let (first, first_publications): (_, Vec<FixturePublication>) =
            acyclic_test_support::differential::observe_async(host.execute(recipe.clone(), vec![]))
                .await;
        let first = first?;
        assert!(!first_publications.is_empty());
        let original = host
            .providers
            .get()
            .ok_or("case did not use the host-owned provider")?;
        let stream = original.stream.clone();
        let (second, second_publications): (_, Vec<FixturePublication>) =
            acyclic_test_support::differential::observe_async(host.execute(recipe.clone(), vec![]))
                .await;
        let second = second?;
        assert!(!second_publications.is_empty());
        let mut expected = recipe;
        expected
            .as_object_mut()
            .ok_or("recipe is not an object")?
            .insert("expect".into(), json!({"text":"Hello from mock LLM!"}));
        let case = crate::scenario::Scenario::from_value(expected)?;
        let (CaseObservation::Executor(first), CaseObservation::Executor(second)) =
            (&first, &second)
        else {
            return Err("executor fixture returned a different driver".into());
        };
        crate::observation::check(&case, first)?;
        crate::observation::check(&case, second)?;
        let request_volume = |observed: &crate::observation::Observation| {
            observed
                .events
                .iter()
                .find(|event| event.get("kind").and_then(Value::as_str) == Some("model_started"))
                .and_then(|event| event.get("request")?.get("volume")?.get("id")?.as_str())
                .map(str::to_owned)
        };
        let first_volume = request_volume(first).ok_or("missing first execution volume")?;
        let second_volume = request_volume(second).ok_or("missing second execution volume")?;
        assert_ne!(
            first_volume, second_volume,
            "cases shared an execution volume"
        );
        assert!(Arc::ptr_eq(
            &stream,
            &host.providers.get().ok_or("provider lost")?.stream
        ));
        assert!(InProcess::default().providers.get().is_none());
        let first_publications: std::collections::BTreeSet<_> = first_publications
            .into_iter()
            .map(|record| record.0)
            .collect();
        let second_publications: std::collections::BTreeSet<_> = second_publications
            .into_iter()
            .map(|record| record.0)
            .collect();
        assert_eq!(first_publications.len(), 2);
        assert_eq!(second_publications.len(), 2);
        assert!(
            first_publications.is_disjoint(&second_publications),
            "unrelated actors share a mutable fixture workspace"
        );
        Ok(())
    }

    #[test]
    fn pinned_parsing_and_discovery_changes_change_package_identity() -> TestResult {
        let expected = HarnessPackage::new(fixture_bindings()).identity.digest;
        for critical in [
            "fixture_volume",
            "scenario",
            "discovery",
            "dispatch",
            "composer",
            "executor_observation",
        ] {
            let position = FIXTURE_SOURCES
                .iter()
                .position(|(name, _)| *name == critical)
                .ok_or_else(|| format!("missing pinned fixture source {critical}"))?;
            let mut changed = FIXTURE_SOURCES.to_vec();
            let (name, source) = changed.get(position).copied().ok_or("missing source")?;
            let mutation = format!("{source}\n// changed implementation\n");
            *changed.get_mut(position).ok_or("missing mutation slot")? = (name, &mutation);
            assert_ne!(
                expected,
                fixture_digest(&changed),
                "unbound source {critical}"
            );
        }
        Ok(())
    }
}
impl ActorPackage for HarnessPackage {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn machines(&self) -> Vec<Arc<dyn ResumableMachine>> {
        vec![Arc::new(self.clone())]
    }
}
impl ResumableMachine for HarnessPackage {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        let command = field(input, "command")?;
        let _: WorkflowCommand = serde_json::from_value(command.clone()).map_err(codec)?;
        Ok(json!({"command":command,"started":false}))
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if field(state, "started")?.as_bool() == Some(false) && input == &json!({"start":true}) {
            return Ok(MachineTransition {
                state: json!({"started":true}),
                commands: vec![
                    serde_json::from_value(field(state, "command")?.clone()).map_err(codec)?,
                ],
                status: MachineStatus::Suspended,
            });
        }
        if field(state, "started")?.as_bool() != Some(true) {
            return Err(Error::Invalid("unexpected scenario input".into()));
        }
        let _: FileRef = serde_json::from_value(field(input, "output")?.clone()).map_err(codec)?;
        Ok(MachineTransition {
            state: input.clone(),
            commands: vec![],
            status: MachineStatus::Completed {
                value: input.clone(),
            },
        })
    }
}

/// Runner side of the package protocol: only the public Actors and fs contracts.
pub struct Actors<R, V> {
    runtime: R,
    volume: V,
    identity: MachineIdentity,
}
impl<R: ActorRuntime, V: Volume> Actors<R, V> {
    /// Installs the pinned package once before publishing this host binding.
    /// Cases can only create actors on this already installed deployment.
    pub async fn install(runtime: R, volume: V, bindings: Bindings) -> TestResult<Self> {
        let package = Arc::new(HarnessPackage::new(bindings));
        let identity = package.identity().clone();
        runtime.install(package).await?;
        Ok(Self {
            runtime,
            volume,
            identity,
        })
    }
}
impl<R: ActorRuntime, V: Volume> Substrate for Actors<R, V> {
    fn capabilities(&self) -> BTreeSet<String> {
        BTreeSet::from([
            "executor".into(),
            "composer".into(),
            "filesystem-policy".into(),
        ])
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(async move {
            let identity = self.identity.clone();
            let operation = OperationId::new();
            let placement = Placement {
                actor: AgentId::new(),
                node: "conformance".into(),
                epoch: 1,
            };
            let payload = self
                .volume
                .put(operation, json!({"recipe":recipe,"paths":paths}))
                .await?;
            let command = WorkflowCommand {
                operation_id: OperationId::new(),
                kind: "harness.scenario.compute".into(),
                payload,
            };
            let placed = self
                .runtime
                .place(
                    placement.clone(),
                    identity.clone(),
                    json!({"command":command}),
                )
                .await?;
            validate_snapshot(&placed, &placement, &identity, 0, "placement")?;
            if placed.status != MachineStatus::Suspended || !placed.pending.is_empty() {
                return Err("host changed initial scenario state".into());
            }
            let event = Event {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new(format!("start:{operation}"))?,
                input: json!({"start":true}),
            };
            let admitted = self
                .runtime
                .deliver(placement.clone(), event.clone())
                .await?;
            validate_snapshot(&admitted, &placement, &identity, 1, "admission")?;
            if admitted.pending != [command] || admitted.status != MachineStatus::Suspended {
                return Err("host lost scenario admission or outbox".into());
            }
            if self.runtime.deliver(placement.clone(), event).await? != admitted {
                return Err("host changed an identical admission retry".into());
            }
            let completed = self.runtime.recover(placement.clone()).await?;
            validate_snapshot(&completed, &placement, &identity, 2, "completion")?;
            if !completed.pending.is_empty()
                || !matches!(&completed.status, MachineStatus::Completed { value } if value == &completed.checkpoint.state)
            {
                return Err("host lost scenario completion observation".into());
            }
            if self.runtime.recover(placement).await? != completed {
                return Err("host changed a settled scenario on recovery".into());
            }
            let output: FileRef =
                serde_json::from_value(field(&completed.checkpoint.state, "output")?.clone())?;
            Ok(serde_json::from_value(self.volume.get(output).await?)?)
        })
    }
}

fn validate_snapshot(
    snapshot: &acyclic_harness::package::Snapshot,
    placement: &Placement,
    identity: &MachineIdentity,
    revision: u64,
    phase: &str,
) -> TestResult {
    if &snapshot.placement != placement
        || &snapshot.checkpoint.machine != identity
        || snapshot.checkpoint.revision != revision
        || snapshot.consumed != revision
        || snapshot.cancelled
    {
        return Err(format!("host lost scenario {phase} observation").into());
    }
    Ok(())
}

/// Fixture-only policy: no ambient credentials, network or user home access.
pub struct FixturePolicy;
impl AdmissionPolicy for FixturePolicy {
    fn authorize<'a>(
        &'a self,
        _: AdmissionAction,
        _: &'a Manifest,
        _: Option<&'a Placement>,
        _: Option<&'a Event>,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Provider-bound executor fixture installed by the effect host.
pub trait ExecutorFixture: ProviderPlatform {
    /// Runs a fixture against the host's real providers and returns observations.
    fn collect(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>>;
}

/// Explicit provider bundle for executor journals, files and context discovery.
pub struct Providers<P, A, O> {
    /// Runtime-resolved Streams provider.
    pub stream: Arc<P>,
    /// Runtime-resolved filesystem providers.
    pub filesystem: Fs<A, O>,
}

type LocalProviders = Providers<
    acyclic_stream::LocalStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

impl LocalProviders {
    async fn local() -> TestResult<Self> {
        let directory = Arc::new(tempfile::tempdir()?);
        let stream = Arc::new(
            acyclic_stream::LocalStream::open_with_ownership_anchor(
                directory.path(),
                Default::default(),
                acyclic_native_runtime::OwnershipAnchor::new(directory.clone()),
            )
            .await?,
        );
        Ok(Self {
            stream,
            filesystem: Fs::memory(),
        })
    }
}
impl<P, A, O> ExecutorFixture for Providers<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn collect(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(crate::collect_on(
            recipe,
            paths,
            self.filesystem.clone(),
            self.stream.clone(),
        ))
    }
}

/// Reference compute binding. Results are retained by command ID for retry;
/// fencing and dispatch share a lock, so a closed placement cannot start work.
pub struct FixtureEffects<V> {
    /// Resolved fixture volume, also used to reconcile completed commands.
    pub volume: Arc<V>,
    /// Harness executor bound to the same host's resolved public providers.
    pub executor: Arc<dyn ExecutorFixture>,
    resource_scope: String,
    placements: Mutex<std::collections::BTreeMap<(AgentId, u64), PlacementEffects>>,
}
#[derive(Default)]
struct EffectState {
    results: std::collections::BTreeMap<OperationId, Event>,
    fenced: bool,
}
type PlacementEffects = Arc<tokio::sync::Mutex<EffectState>>;
impl<V> FixtureEffects<V> {
    /// Creates an isolated fixture compute binding with explicit fs custody.
    pub fn new(volume: Arc<V>, executor: Arc<dyn ExecutorFixture>, bindings: &Bindings) -> Self {
        Self {
            volume,
            executor,
            resource_scope: bindings.streams.clone(),
            placements: Mutex::new(Default::default()),
        }
    }

    fn result_publication(&self, placement: &Placement, operation: OperationId) -> OperationId {
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness.conformance.effect-publication.v1\0");
        hash.update(&(self.resource_scope.len() as u64).to_le_bytes());
        hash.update(self.resource_scope.as_bytes());
        hash.update(&placement.actor.into_bytes());
        hash.update(&operation.into_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        OperationId::from_bytes(bytes)
    }

    fn placement(&self, placement: &Placement) -> Result<PlacementEffects> {
        let mut placements = self
            .placements
            .lock()
            .map_err(|_| Error::Storage("fixture placements poisoned".into()))?;
        Ok(placements
            .entry((placement.actor, placement.epoch))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(EffectState::default())))
            .clone())
    }
}
impl<V: Volume + 'static> EffectHost for FixtureEffects<V> {
    fn execute(
        &self,
        placement: Placement,
        command: WorkflowCommand,
    ) -> BoxProviderFuture<'_, Result<Event>> {
        Box::pin(async move {
            let owner = self.placement(&placement)?;
            let mut state = owner.lock().await;
            if state.fenced {
                return Err(Error::Conflict("fixture placement fenced".into()));
            }
            if let Some(event) = state.results.get(&command.operation_id) {
                return Ok(event.clone());
            }
            if command.kind != "harness.scenario.compute" {
                return Err(Error::Unsupported("unknown fixture effect".into()));
            }
            let result_operation = effect_result_operation(command.operation_id);
            let publication = self.result_publication(&placement, command.operation_id);
            let output = if let Some(output) = self.volume.lookup(publication).await? {
                output
            } else {
                let value = self.volume.get(command.payload).await?;
                let paths =
                    serde_json::from_value(field(&value, "paths")?.clone()).map_err(codec)?;
                let observed = self
                    .executor
                    .collect(field(&value, "recipe")?.clone(), paths)
                    .await
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                self.volume
                    .put(publication, serde_json::to_value(observed).map_err(codec)?)
                    .await?
            };
            let event = Event {
                operation_id: result_operation,
                idempotency_key: IdempotencyKey::new(format!("result:{result_operation}"))?,
                input: json!({"output":output}),
            };
            state.results.insert(command.operation_id, event.clone());
            Ok(event)
        })
    }
    fn fence(&self, placement: Placement) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            let owner = self.placement(&placement)?;
            owner.lock().await.fenced = true;
            Ok(())
        })
    }
}
impl<V: Volume + ?Sized> Volume for Arc<V> {
    fn put(&self, operation: OperationId, value: Value) -> BoxProviderFuture<'_, Result<FileRef>> {
        (**self).put(operation, value)
    }
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>> {
        (**self).get(file)
    }
    fn lookup(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<Option<FileRef>>> {
        (**self).lookup(operation)
    }
}

/// Credential-free resource names for isolated reference deployments.
pub fn fixture_bindings() -> Bindings {
    Bindings {
        streams: "conformance".into(),
        volume: "fixtures".into(),
        placement: VolumePlacement::Portable,
        model_https: "https://fixture.invalid".into(),
        model_secret: "unused-fixture".into(),
        compute: "scenario-executor".into(),
    }
}

/// Host-owned providers; each actor retains its own fixture and execution workspaces.
type LocalActorHost = Actors<Supervised<acyclic_stream::LocalStream>, Arc<volume::FixtureVolume>>;

/// One package deployment over host-owned providers and isolated actor storage.
#[derive(Default)]
pub struct InProcess {
    providers: tokio::sync::OnceCell<LocalProviders>,
    actors: tokio::sync::OnceCell<LocalActorHost>,
    #[cfg(test)]
    deployments: std::sync::atomic::AtomicUsize,
}
impl Substrate for InProcess {
    fn capabilities(&self) -> BTreeSet<String> {
        BTreeSet::from([
            "executor".into(),
            "composer".into(),
            "filesystem-policy".into(),
        ])
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(async move {
            let actors = self
                .actors
                .get_or_try_init(|| async {
                    let providers = self
                        .providers
                        .get_or_try_init(LocalProviders::local)
                        .await?;
                    let stream = providers.stream.clone();
                    let filesystem = providers.filesystem.clone();
                    let volume = Arc::new(volume::FixtureVolume::in_process(filesystem.clone())?);
                    let bindings = fixture_bindings();
                    let executor = Arc::new(Providers {
                        stream: stream.clone(),
                        filesystem,
                    });
                    let runtime = InProcessRuntime::new(
                        stream,
                        bindings.clone(),
                        Arc::new(FixturePolicy),
                        Arc::new(FixtureEffects::new(volume.clone(), executor, &bindings)),
                    );
                    #[cfg(test)]
                    self.deployments
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    Actors::install(Supervised::new(runtime), volume, bindings).await
                })
                .await?;
            actors.execute(recipe, paths).await
        })
    }
}

/// Direct executor baseline, retaining exactly the same fixture and oracles.
pub struct Local;
impl Substrate for Local {
    fn capabilities(&self) -> BTreeSet<String> {
        BTreeSet::from([
            "executor".into(),
            "composer".into(),
            "filesystem-policy".into(),
        ])
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(crate::collect_on(
            recipe,
            paths,
            Fs::memory(),
            Arc::new(acyclic_stream::MemoryStream::default()),
        ))
    }
}
