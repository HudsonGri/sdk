//! Fixed immutable owner metadata; only publication and its fence are modeled.
use super::*;
use crate::distributed::WorkLease;

impl<P: StreamProvider + 'static> Fixture<P> {
    pub(crate) fn model(
        stream: Arc<P>,
        coordinator: DistributedCoordinator<P>,
        lease: &WorkLease,
    ) -> Result<(Self, TaskJournalOwner<P>)> {
        let backend = Fs::memory();
        let provider = ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?;
        let files = FilesystemHost::new(backend.clone(), &provider, None)?;
        let storage = VolumeRef::new(
            provider,
            "model".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?;
        let authority = lease.operation.owner.authority().clone();
        let issuer = AuthorityIssuer::new("test-runtime", [9; 32], authority.clone());
        let scope = RuntimeScope::new(
            Capabilities::new([
                "operation:declare".to_owned(),
                "operation:observe".to_owned(),
                "operation:cancel".to_owned(),
                storage.capability(VolumeOperation::Read, &mut None)?,
                storage.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Limits::default(),
        )?;
        let signed = issuer.issue("owner", scope.grants(), None, None, &mut None)?;
        let verifier = issuer.verifier(&mut None)?;
        // These production adapters are bound but never resolve a workspace:
        // the Started event contains no content references or payload writes.
        let reader = FilesystemContentVerifier::new(
            files.clone(),
            &verifier,
            &signed,
            scope.limits().file_bytes,
            None,
        )?;
        let publisher = FilesystemContentPublisher::new(
            files.clone(),
            &storage,
            &verifier,
            &signed,
            scope.limits().file_bytes,
            None,
        )?;
        let host = Arc::new(CoordinatorTaskHost::new(
            coordinator,
            stream.clone(),
            publisher.clone(),
            reader.clone(),
            authority,
            signed,
            verifier,
            scope,
            TaskRegistry::default(),
            MachineRegistry::default(),
            Arc::new(SystemUnixMillisClock),
        )?);
        let task = ComponentIdentity {
            name: "example.task".into(),
            version: "1".into(),
            digest: [2; 32],
        };
        let machine = MachineIdentity {
            name: "example.machine".into(),
            version: "1".into(),
            digest: [3; 32],
        };
        let fixture = Self {
            stream,
            backend,
            files,
            issuer,
            host,
            destination: storage.clone(),
            storage,
            reader,
            publisher,
            task,
            machine: machine.clone(),
            schema: json!(true),
        };
        let admission = fixture.admission(1, json!(0));
        assert_eq!(admission.operation_id, lease.operation.operation_id);
        let owner = TaskJournalOwner {
            host: fixture.host.clone(),
            fence: (&lease.reservation).into(),
            workflow: crate::workflow::WorkflowAdmission {
                operation_id: admission.operation_id,
                request_digest: [1; 32],
                initial: crate::workflow::MachineCheckpoint {
                    machine,
                    revision: 0,
                    state: json!(0),
                },
            },
            admission: Arc::new(admission),
            input_agent: None,
            created: None,
            read_proof: None,
            tool_output_schema: None,
            invocation_cancellation: None,
            root_binding: std::sync::OnceLock::new(),
            root_choice: std::sync::OnceLock::new(),
            root_gate: Mutex::new(()),
            native_custody: None,
        };
        Ok((fixture, owner))
    }

    pub(crate) async fn model_peer(&self, spec: OperationSpec) -> Result<bool> {
        self.host
            .coordinator
            .lock()
            .await
            .declare_operation(
                &self.host.owner,
                &self.host.owner_scope,
                &self.host.verifier,
                spec,
                IdempotencyKey::new("peer")?,
                None,
                &mut None,
            )
            .await
    }
}
