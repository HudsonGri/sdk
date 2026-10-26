//! Portable lifecycle implementation over a provider transport.

use super::{
    MutationIntent, NetworkPolicy, OperationRegistry, SandboxDriver, SandboxInstance, SandboxSpec,
    WorkspaceArchive, WorkspaceIo, allocation_receipt, expiration_deadline,
};
use crate::*;
use std::sync::{Mutex as StdMutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

/// Application bindings for a remote machine provider.
#[derive(Clone, Debug)]
pub struct RemoteMachinesConfig {
    /// Resource namespace for this provider instance. The journal is process-local;
    /// independent instances must not dispatch into one another's resource names.
    pub namespace: uuid::Uuid,
    /// Immutable digests mapped to provider image/template identifiers.
    pub images: BTreeMap<[u8; 32], String>,
    /// Network commitments mapped to the policy the adapter must enforce.
    pub networks: BTreeMap<[u8; 32], NetworkPolicy>,
    /// Persistent disk boundary used by portable disk checkpoints and forks.
    pub workspace: String,
}

impl Default for RemoteMachinesConfig {
    fn default() -> Self {
        Self {
            namespace: uuid::Uuid::new_v4(),
            images: BTreeMap::new(),
            networks: BTreeMap::new(),
            workspace: "/workspace".into(),
        }
    }
}

#[derive(Clone)]
struct Binding {
    instance: SandboxInstance,
    image: String,
}

#[derive(Clone)]
struct DiskCheckpoint {
    archive: WorkspaceArchive,
    image: String,
}

/// Machines lifecycle shared by sandbox transports.
///
/// Resource ownership, events, replay and disk checkpoints are process-local.
/// The adapter supplies the actual execution/isolation boundary. Keep this
/// provider instance alive while using its machine and checkpoint identities.
pub struct RemoteMachines<D> {
    driver: Arc<D>,
    config: RemoteMachinesConfig,
    ops: Arc<OperationRegistry>,
    bindings: StdMutex<BTreeMap<MachineId, Binding>>,
    checkpoints: StdMutex<BTreeMap<CheckpointId, DiskCheckpoint>>,
    history: super::ConsoleHistory,
    lifetimes: StdMutex<BTreeMap<MachineId, acyclic_native_runtime::WallAlarm>>,
}

pub(super) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| {
            u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
        })
}

impl<D: SandboxDriver> RemoteMachines<D> {
    /// Binds a transport to application-authorized images and network policies.
    ///
    /// # Errors
    /// Rejects workspace paths that cannot be archived independently.
    pub fn new(driver: D, config: RemoteMachinesConfig) -> Result<Self, ProviderError> {
        super::validate_workspace(&config.workspace)?;
        Ok(Self {
            driver: Arc::new(driver),
            config,
            ops: Arc::default(),
            bindings: StdMutex::default(),
            checkpoints: StdMutex::default(),
            history: super::ConsoleHistory::default(),
            lifetimes: StdMutex::default(),
        })
    }

    fn capabilities(&self) -> BTreeSet<Capability> {
        let mut capabilities = BTreeSet::from([Capability::DiskFork]);
        if self.driver.supports_suspend() {
            capabilities.insert(Capability::SuspendResume);
        }
        capabilities
    }

    fn revision(&self) -> [u8; 32] {
        Sha256::digest(format!("acyclic-machines-remote-v1/{}", self.driver.name())).into()
    }

    fn image(&self, image: &Image) -> Result<String, ProviderError> {
        let digest = match image {
            Image::Custom(digest) | Image::ManagedOci(digest) => digest.as_bytes(),
            Image::Checkpoint(id) => {
                return self
                    .checkpoints
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(id)
                    .map(|value| value.image.clone())
                    .ok_or_else(|| ProviderError::NotFound(id.to_string()));
            }
        };
        self.config.images.get(&digest).cloned().ok_or_else(|| ProviderError::Unsupported(
            format!("unregistered immutable image {}; recovery: register its provider image in RemoteMachinesConfig::images", hex::encode(digest))
        ))
    }

    fn binding(&self, machine: MachineId) -> Result<Binding, ProviderError> {
        self.bindings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&machine)
            .cloned()
            .ok_or_else(|| ProviderError::NotFound(machine.to_string()))
    }

    fn known(&self, machine: MachineId) -> Result<MachineObservation, ProviderError> {
        self.ops
            .machine(machine)
            .ok_or_else(|| ProviderError::NotFound(machine.to_string()))
    }

    async fn rollback(&self, operation: OperationId) {
        let mut remaining = Vec::new();
        for id in self.ops.take_created(operation).into_iter().rev() {
            let binding = self
                .bindings
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .find(|(_, binding)| binding.instance.id == id)
                .map(|(machine, binding)| (*machine, binding.clone()));
            if let Some((machine, binding)) = binding {
                match self.driver.destroy(&binding.instance).await {
                    Ok(()) | Err(ProviderError::NotFound(_)) => {
                        self.ops.mark_destroyed(machine, now_unix_ms());
                        self.lifetimes
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .remove(&machine);
                    }
                    Err(_) => remaining.push(id),
                }
            } else {
                remaining.push(id);
            }
        }
        remaining.reverse();
        self.ops.restore_created(operation, remaining);
    }

    async fn apply<F>(
        &self,
        key: IdempotencyKey,
        intent: &MutationIntent,
        action: F,
    ) -> Result<MutationOutcome, ProviderError>
    where
        F: AsyncFnOnce(OperationId) -> Result<MutationOutcome, ProviderError>,
    {
        self.ops
            .apply(key, intent, action, async |operation| {
                self.rollback(operation).await
            })
            .await
    }

    async fn create_one(
        &self,
        operation: OperationId,
        key: IdempotencyKey,
        index: u32,
        mut contract: MachineContract,
        image: String,
        archive: Option<&WorkspaceArchive>,
    ) -> Result<MachineObservation, ProviderError> {
        let now = now_unix_ms();
        if let Some(maximum) = self.driver.maximum_lifetime() {
            match contract.expiration {
                ExpirationPolicy::ProviderDefault => contract.expiration = ExpirationPolicy::MaxAge(maximum),
                ExpirationPolicy::Never => return Err(ProviderError::Unsupported("unlimited lifetime admission failed; recovery: select ProviderDefault or an explicit deadline within the provider ceiling".into())),
                ExpirationPolicy::MaxAge(age) if age > maximum => return Err(ProviderError::Unsupported("provider lifetime admission failed; recovery: request a maximum age within the provider ceiling".into())),
                ExpirationPolicy::AtUnixMs(deadline) if deadline.saturating_sub(now) > u64::try_from(maximum.as_millis()).unwrap_or(u64::MAX) => return Err(ProviderError::Unsupported("provider deadline admission failed; recovery: request a deadline within the provider lifetime ceiling".into())),
                _ => {},
            }
        }
        if contract.expiration == ExpirationPolicy::ProviderDefault {
            contract.expiration = ExpirationPolicy::Never;
        }
        let expiry = expiration_deadline(contract.expiration, now)?;
        let network = self.config.networks.get(&contract.network_policy_digest).cloned()
            .ok_or_else(|| ProviderError::Unsupported("network commitment is unregistered; recovery: register its policy in RemoteMachinesConfig::networks".into()))?;
        let name = format!("acyclic-{}-{key}-{index}", self.config.namespace);
        let spec = SandboxSpec {
            name: name.clone(),
            image: image.clone(),
            contract: contract.clone(),
            network,
            env: BTreeMap::new(),
        };
        self.driver.validate(&spec)?;
        // Record dispatch before awaiting the reply, so a lost reply never frees the key.
        self.ops.bind_target(operation, &name);
        let instance = self.driver.create(&spec).await?;
        let id = MachineId::from_digest(Sha256::digest(name.as_bytes()).into());
        self.bindings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                id,
                Binding {
                    instance: instance.clone(),
                    image,
                },
            );
        let mut observation = MachineObservation {
            id,
            state: MachineState::Starting,
            contract,
            endpoints: vec![Endpoint {
                name: "default".into(),
                uri: instance.endpoint.clone(),
            }],
            last_checkpoint: None,
            created_at_unix_ms: now,
            changed_at_unix_ms: now,
        };
        if !self
            .ops
            .bind_created_machine(operation, &instance.id, observation.clone())
        {
            self.driver.destroy(&instance).await?;
            self.bindings
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&id);
            return Err(ProviderError::Cancelled);
        }
        if let Some(archive) = archive {
            archive
                .restore(self, &id.to_string(), &self.config.workspace)
                .await?;
        }
        if expiry.is_some_and(|deadline| deadline <= now_unix_ms()) {
            return Err(ProviderError::Rejected("machine creation exceeded its expiration; recovery: create with a future deadline and a fresh idempotency key".into()));
        }
        if let Some(deadline) = expiry {
            let driver = Arc::clone(&self.driver);
            let ops = Arc::clone(&self.ops);
            let runtime = tokio::runtime::Handle::current();
            let instance = instance.clone();
            let alarm = acyclic_native_runtime::WallAlarm::register(deadline, Arc::new(move || {
                let (driver, ops, instance) = (Arc::clone(&driver), Arc::clone(&ops), instance.clone());
                runtime.spawn(async move {
                    match driver.destroy(&instance).await {
                        Ok(()) | Err(ProviderError::NotFound(_)) => ops.mark_destroyed(id, now_unix_ms()),
                        Err(error) => {
                            ops.observe_state(id, MachineState::Indeterminate, now_unix_ms());
                            tracing::error!(machine = %id, error.kind = error.kind(), "machine expiry destruction failed; recovery: call destroy_machine with a fresh idempotency key");
                        }
                    }
                });
            })).map_err(|error| ProviderError::Rejected(format!("machine expiry alarm failed: {error}; recovery: restore the OS wall-clock alarm service and create with a new idempotency key")))?;
            self.lifetimes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(id, alarm);
        }
        observation.state = MachineState::Running;
        observation.changed_at_unix_ms = now_unix_ms();
        if !self
            .ops
            .observe_pending_machine(operation, observation.clone())
        {
            self.lifetimes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&id);
            return Err(ProviderError::Cancelled);
        }
        Ok(observation)
    }

    fn exec_bound(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        let binding = self.binding(machine)?;
        let observation = self.known(machine)?;
        let expiry = expiration_deadline(
            observation.contract.expiration,
            observation.created_at_unix_ms,
        )?;
        let driver = Arc::clone(&self.driver);
        let request = command.clone();
        super::owned_exec(
            machine,
            &command,
            expiry,
            self.history.clone(),
            driver.start(&binding.instance, request)?,
        )
    }

    async fn capture(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<WorkspaceArchive, ProviderError> {
        WorkspaceArchive::capture(
            self,
            &machine.to_string(),
            &self.config.workspace,
            &format!("/tmp/acyclic-fork-{key}.tar"),
            5,
        )
        .await
    }
}

#[async_trait]
impl<D: SandboxDriver> WorkspaceIo for RemoteMachines<D> {
    async fn status(&self, resource: &str, command: String) -> Result<Option<i64>, ProviderError> {
        let machine = MachineId::parse(resource)
            .map_err(|error| ProviderError::Invalid(error.to_string()))?;
        let command = ExecCommand::new(vec!["sh".into(), "-c".into(), command], 60_000)?;
        let mut output = self.exec_bound(machine, command)?;
        while let Some(event) = output.next().await {
            if let ExecOutput::ExitCode(code) = event? {
                return Ok(Some(i64::from(code)));
            }
        }
        Err(ProviderError::CommandIndeterminate(machine))
    }

    async fn download(&self, resource: &str, path: &str) -> Result<Vec<u8>, ProviderError> {
        self.driver
            .read_file(
                &self
                    .binding(
                        MachineId::parse(resource)
                            .map_err(|error| ProviderError::Invalid(error.to_string()))?,
                    )?
                    .instance,
                path,
            )
            .await
    }

    async fn upload(&self, resource: &str, path: &str, data: &[u8]) -> Result<(), ProviderError> {
        self.driver
            .write_file(
                &self
                    .binding(
                        MachineId::parse(resource)
                            .map_err(|error| ProviderError::Invalid(error.to_string()))?,
                    )?
                    .instance,
                path,
                data,
            )
            .await
    }
}

#[async_trait]
impl<D: SandboxDriver> MachinesProvider for RemoteMachines<D> {
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::CustomerHosted
    }

    async fn exec(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        if self.known(machine)?.state != MachineState::Running {
            return Err(ProviderError::Conflict(
                "exec requires a running machine".into(),
            ));
        }
        self.exec_bound(machine, command)
    }

    async fn read_output(
        &self,
        machine: MachineId,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        self.known(machine)?;
        self.history.read(machine, offset, limit)
    }

    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        self.image(&image)?;
        Ok(ImageQualification {
            image,
            capabilities: self.capabilities(),
            compatibility_revision: self.revision(),
        })
    }

    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        self.apply(key, &MutationIntent::Create(request.clone()), async |operation| {
            if request.budgets != Budgets::default() {
                return Err(ProviderError::Unsupported("per-machine spend/concurrency enforcement is unavailable; recovery: leave budgets zero".into()));
            }
            let capabilities = self.capabilities();
            if let CompatibilityPolicy::Require(required) = &request.compatibility
                && (required.is_empty() || !required.is_subset(&capabilities)) {
                return Err(ProviderError::Unsupported(format!("requested capabilities exceed {capabilities:?}")));
            }
            let image = self.image(&request.image)?;
            expiration_deadline(request.expiration, now_unix_ms())?;
            let archive = if let Image::Checkpoint(id) = &request.image {
                self.checkpoints.lock().unwrap_or_else(PoisonError::into_inner).get(id).map(|value| value.archive.clone())
            } else { None };
            let contract = MachineContract { image: request.image, capabilities, compatibility: request.compatibility,
                compatibility_revision: self.revision(), suspension: SuspensionPolicy::Manual,
                expiration: request.expiration, network_policy_digest: request.network_policy_digest, budgets: request.budgets };
            self.create_one(operation, key, 0, contract, image, archive.as_ref()).await.map(MutationOutcome::Created)
        }).await
    }

    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        let current = self.known(machine)?;
        if !matches!(
            current.state,
            MachineState::Destroyed | MachineState::Starting
        ) {
            let state = match self.driver.state(&self.binding(machine)?.instance).await {
                Ok(state) => state,
                Err(ProviderError::NotFound(_)) => MachineState::Destroyed,
                Err(error) => return Err(error),
            };
            return self
                .ops
                .observe_state(machine, state, now_unix_ms())
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()));
        }
        Ok(current)
    }

    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if !(1..=MAX_PAGE_SIZE).contains(&limit) {
            return Err(ProviderError::Invalid(
                "machine page limit must be 1..=256".into(),
            ));
        }
        let mut machines = self
            .ops
            .machines()
            .into_iter()
            .filter(|m| after.is_none_or(|id| m.id > id))
            .take(limit as usize + 1)
            .collect::<Vec<_>>();
        let next = if machines.len() > limit as usize {
            machines.pop();
            machines.last().map(|m| m.id)
        } else {
            None
        };
        Ok(MachinePage { machines, next })
    }

    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &MutationIntent::Checkpoint(machine),
            async |operation| {
                let current = self.inspect_machine(machine).await?;
                self.ops.bind_target(operation, &machine.to_string());
                let archive = self.capture(machine, key).await?;
                let id = CheckpointId::from_digest(Sha256::digest(key.as_bytes()).into());
                let checkpoint = CheckpointObservation {
                    id,
                    source: machine,
                    contract: current.contract,
                    forkable: true,
                    created_at_unix_ms: now_unix_ms(),
                };
                self.checkpoints
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(
                        id,
                        DiskCheckpoint {
                            archive,
                            image: self.binding(machine)?.image,
                        },
                    );
                self.ops.remember_checkpoint(checkpoint.clone());
                let mut source = self.known(machine)?;
                source.last_checkpoint = Some(id);
                self.ops.observe_machine(source, now_unix_ms());
                Ok(MutationOutcome::Checkpointed(checkpoint))
            },
        )
        .await
    }

    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.ops
            .checkpoint(checkpoint)
            .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))
    }

    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &MutationIntent::Fork(checkpoint, count.get()),
            async |operation| {
                if count.get() > MAX_FORK_CHILDREN {
                    return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
                }
                let current = self.inspect_checkpoint(checkpoint).await?;
                if !current.forkable {
                    return Err(ProviderError::Conflict("checkpoint was destroyed".into()));
                }
                let disk = self
                    .checkpoints
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&checkpoint)
                    .cloned()
                    .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))?;
                let mut children = Vec::new();
                for index in 0..count.get() {
                    let mut child = self
                        .create_one(
                            operation,
                            key,
                            index,
                            current.contract.clone(),
                            disk.image.clone(),
                            Some(&disk.archive),
                        )
                        .await?;
                    child.last_checkpoint = Some(checkpoint);
                    self.ops.observe_machine(child.clone(), now_unix_ms());
                    children.push(child);
                }
                Ok(MutationOutcome::Forked(children))
            },
        )
        .await
    }

    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &MutationIntent::ForkMachine(machine, count.get()),
            async |operation| {
                if count.get() > MAX_FORK_CHILDREN {
                    return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
                }
                let current = self.inspect_machine(machine).await?;
                let image = self.binding(machine)?.image;
                let archive = self.capture(machine, key).await?;
                let mut children = Vec::new();
                for index in 0..count.get() {
                    children.push(
                        self.create_one(
                            operation,
                            key,
                            index,
                            current.contract.clone(),
                            image.clone(),
                            Some(&archive),
                        )
                        .await?,
                    );
                }
                Ok(MutationOutcome::MachineForked {
                    source: machine,
                    fidelity: ForkFidelity::DiskOnly,
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
        self.apply(key, &MutationIntent::Suspend(machine), async |operation| {
            let mut current = self.inspect_machine(machine).await?;
            if !current
                .contract
                .capabilities
                .contains(&Capability::SuspendResume)
            {
                return Err(ProviderError::Unsupported(
                    "memory-preserving suspend is unavailable".into(),
                ));
            }
            if current.state == MachineState::Suspended {
                return Ok(MutationOutcome::Suspended(machine));
            }
            if current.state != MachineState::Running {
                return Err(ProviderError::Conflict(
                    "suspend requires a running machine".into(),
                ));
            }
            self.ops.bind_target(operation, &machine.to_string());
            self.driver
                .suspend(&self.binding(machine)?.instance)
                .await?;
            current.state = MachineState::Suspended;
            current.changed_at_unix_ms = now_unix_ms();
            self.ops.observe_machine(current, now_unix_ms());
            Ok(MutationOutcome::Suspended(machine))
        })
        .await
    }

    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, &MutationIntent::Wake(machine), async |operation| {
            let mut current = self.known(machine)?;
            if !current
                .contract
                .capabilities
                .contains(&Capability::SuspendResume)
            {
                return Err(ProviderError::Unsupported(
                    "memory-preserving wake is unavailable".into(),
                ));
            }
            if current.state == MachineState::Running {
                return Ok(MutationOutcome::Woken(machine));
            }
            if current.state != MachineState::Suspended {
                return Err(ProviderError::Conflict(
                    "wake requires a suspended machine".into(),
                ));
            }
            self.ops.bind_target(operation, &machine.to_string());
            self.driver.wake(&self.binding(machine)?.instance).await?;
            current.state = MachineState::Running;
            current.changed_at_unix_ms = now_unix_ms();
            self.ops.observe_machine(current, now_unix_ms());
            Ok(MutationOutcome::Woken(machine))
        })
        .await
    }

    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &MutationIntent::Policy(machine, policy),
            async |operation| {
                let mut current = self.inspect_machine(machine).await?;
                if !current
                    .contract
                    .capabilities
                    .contains(&Capability::SuspendResume)
                {
                    return Err(ProviderError::Unsupported(
                        "suspension policy is unavailable".into(),
                    ));
                }
                self.ops.bind_target(operation, &machine.to_string());
                self.driver
                    .set_suspension_policy(&self.binding(machine)?.instance, policy)
                    .await?;
                current.contract.suspension = policy;
                self.ops.observe_machine(current, now_unix_ms());
                Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
            },
        )
        .await
    }

    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &MutationIntent::DestroyMachine(machine),
            async |operation| {
                let current = self.known(machine)?;
                if current.state != MachineState::Destroyed {
                    self.ops.bind_target(operation, &machine.to_string());
                    match self.driver.destroy(&self.binding(machine)?.instance).await {
                        Ok(()) | Err(ProviderError::NotFound(_)) => {}
                        Err(error) => return Err(error),
                    }
                    self.ops.mark_destroyed(machine, now_unix_ms());
                    self.lifetimes
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&machine);
                }
                Ok(MutationOutcome::MachineDestroyed(machine))
            },
        )
        .await
    }

    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            &MutationIntent::DestroyCheckpoint(checkpoint),
            async |_operation| {
                self.inspect_checkpoint(checkpoint).await?;
                self.checkpoints
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&checkpoint);
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
        if !(1..=MAX_EVENT_PAGE_SIZE).contains(&limit) {
            return Err(ProviderError::Invalid(
                "event page limit must be 1..=1024".into(),
            ));
        }
        self.inspect_machine(machine).await?;
        let (events, next_sequence) = self.ops.events(machine, after_sequence, limit as usize);
        Ok(EventPage {
            events,
            next_sequence,
        })
    }

    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        let current = self.known(machine)?;
        let end = if current.state == MachineState::Destroyed {
            current.changed_at_unix_ms
        } else {
            now_unix_ms()
        };
        allocation_receipt(
            self.driver.name(),
            machine,
            &self.binding(machine)?.instance.id,
            (start_unix_ms, end_unix_ms),
            (current.created_at_unix_ms, end),
            false,
            self.binding(machine)?.instance.allocation,
        )
    }

    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        let record = self
            .ops
            .record_for_key(key)
            .ok_or_else(|| ProviderError::NotFound(key.to_string()))?;
        match (record.phase, record.outcome) {
            (OperationPhase::Succeeded, Some(outcome)) => Ok(outcome),
            (OperationPhase::Cancelled, _) => Err(ProviderError::Cancelled),
            (OperationPhase::Failed, _) => Err(ProviderError::Failed),
            _ => Err(ProviderError::Indeterminate(key)),
        }
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
        let (observation, created) = self
            .ops
            .cancel(operation)
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))?;
        self.ops.restore_created(operation, created);
        self.rollback(operation).await;
        Ok(observation)
    }

    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        self.ops.watch(operation)
    }
}
