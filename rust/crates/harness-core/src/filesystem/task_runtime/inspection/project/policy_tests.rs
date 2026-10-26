use super::*;
use crate::conversation::VolumeOperation;
use crate::durable_host::tests::fixture::Fixture;
use crate::runtime::TaskSpawner;
use crate::source_authorization::ProjectSourceAuthorization;
use crate::{Capabilities, core::Scope};
use acyclic_fs::access::{
    Action, Capability, Conditions, Effect, Generations, Grant, Policy, ResourcePattern,
};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend, WorkBudget};
use acyclic_stream::MemoryStream;
use serde_json::json;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type Reader = ProjectSourceReader<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;

struct Source {
    fixture: Fixture,
    project: VolumeRef,
    generation: GenerationRef,
    public: FileRef,
    secret: FileRef,
    parent: Capability,
    volume: String,
    other_volumes: Vec<String>,
    policy: Policy,
}

fn grant(volume: Option<String>, path: &str, actions: &[Action]) -> Grant {
    Grant {
        actions: actions.iter().copied().collect(),
        resource: ResourcePattern {
            lineage_root: None,
            volume,
            generations: Generations::All,
            path: path.into(),
        },
    }
}

impl Source {
    async fn new(name: &str) -> TestResult<Self> {
        let provider = crate::resources::ProviderRef::new(
            "fixture".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let project = VolumeRef::new(
            provider,
            "project".into(),
            crate::conversation::VolumeClass::Project,
            crate::conversation::VolumeOwner::Project("source".into()),
            &mut None,
        )?;
        let parent = Capability {
            layers: vec![vec![grant(None, "**", &Action::ALL)]],
            expires_at: None,
        };
        let read = project.capability(VolumeOperation::Read, &mut None)?;
        let write = project.capability(VolumeOperation::Write, &mut None)?;
        let label = parent.to_scope_label()?;
        let fixture = Fixture::new(
            Arc::new(MemoryStream::default()),
            Fs::memory(),
            json!({"type": "integer"}),
            Some(crate::scheduler::SessionLimits {
                active_tasks: 1,
                total_tasks: 1,
                depth: 0,
                model_steps: 1,
            }),
            &[&read, &write, &label],
        )
        .await?;
        fixture.files.create_volume(&project).await?;
        let boot = fixture.issuer.issue(
            "seed",
            &Capabilities::new([read, write]),
            Some(crate::AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let verifier = fixture.issuer.verifier(&mut None)?;
        let write = crate::conversation::ContentGrant::verify(
            &verifier,
            &boot,
            &project,
            VolumeOperation::Write,
            4096,
            &mut None,
        )?;
        for (path, bytes) in [
            (name, b"public".as_slice()),
            ("secret.txt", b"secret".as_slice()),
        ] {
            fixture
                .files
                .put_content(
                    &project,
                    &write,
                    path,
                    bytes,
                    "text/plain",
                    path,
                    4096,
                    &crate::IdempotencyKey::new(format!("seed-{path}"))?,
                    None,
                    crate::conversation::ContentAccess::default(),
                )
                .await?;
        }
        let reference = bounded_io::content_workspace(&fixture.files.provider, &project, None)?;
        let generation = fixture.files.resolve(&reference, None).await?.generation;
        let read = crate::conversation::ContentGrant::verify_directory_read(
            &verifier, &boot, &project, "", &mut None,
        )?;
        let (public, _) = fixture
            .files
            .read_private_path(
                &project,
                &read,
                name,
                Some(&generation),
                4096,
                crate::conversation::ContentAccess::default(),
            )
            .await?;
        let (secret, _) = fixture
            .files
            .read_private_path(
                &project,
                &read,
                "secret.txt",
                Some(&generation),
                4096,
                crate::conversation::ContentAccess::default(),
            )
            .await?;
        let mut ids = Vec::new();
        for volume in [&project, &fixture.destination, &fixture.storage] {
            let reference = bounded_io::content_workspace(&fixture.files.provider, volume, None)?;
            let workspace = fixture.files.open(&reference, None).await?;
            ids.push(uuid::Uuid::from_bytes(workspace.id().volume_id().into_bytes()).to_string());
        }
        let volume = ids.remove(0);
        Ok(Self {
            fixture,
            project,
            generation,
            public,
            secret,
            parent,
            volume,
            other_volumes: ids,
            policy: serde_json::from_slice(crate::filesystem::access::INITIAL_POLICY)?,
        })
    }

    async fn reader(&self, mode: u8) -> TestResult<(Box<Reader>, Capability)> {
        let mut layer: Vec<_> = self
            .other_volumes
            .iter()
            .map(|volume| grant(Some(volume.clone()), "**", &Action::ALL))
            .collect();
        let project = Some(self.volume.clone());
        match mode {
            0 => layer.push(grant(
                project,
                "**",
                &[Action::Read, Action::List, Action::ReadHistory],
            )),
            1 => {
                layer.push(grant(
                    project.clone(),
                    "",
                    &[Action::List, Action::ReadHistory],
                ));
                layer.push(grant(
                    project,
                    self.public.path(),
                    &[Action::Read, Action::List, Action::ReadHistory],
                ));
            }
            2 => layer.push(grant(project, "**", &[Action::List, Action::ReadHistory])),
            3 => layer.push(grant(project, "**", &[Action::Read, Action::ReadHistory])),
            4 => (),
            _ => unreachable!(),
        }
        let ceiling = self.parent.clone().attenuate(layer, None);
        let label = ceiling.to_scope_label()?;
        let mut admission = self.fixture.admission(30 + mode, json!(0));
        admission.grants = Capabilities::new(
            admission
                .grants
                .iter()
                .filter(|label| !label.starts_with(acyclic_fs::access::SCOPE_PREFIX))
                .map(str::to_owned)
                .chain(std::iter::once(label)),
        );
        self.fixture.host.admit(admission).await?;
        self.fixture.claim(30 + mode, "original").await?;
        let cancel = acyclic_fs::CancellationToken::new();
        let owner = Arc::new(
            self.fixture
                .owner(30 + mode)
                .await?
                .with_invocation_cancellation(&cancel),
        );
        let mut authorization = ProjectSourceAuthorization {
            owner: self.fixture.issuer.audience().clone(),
            caller_task: owner.task_id(),
            caller_admission_digest: owner.admission().canonical_digest_with_work(&mut None)?,
            caller_fence: owner.task_binding().1.clone(),
            operation_id: crate::OperationId::from_bytes([90 + mode; 16]),
            source_task: owner.task_id(),
            project: self.project.clone(),
            generation: self.generation.clone(),
            native_snapshot: None,
            graph_root: owner.task_id(),
            maximum_work: 1_000_000,
            maximum_bytes: 65536,
            policy_scope: Scope::from_wire(
                String::new(),
                Capabilities::default(),
                String::new(),
                None,
                None,
                [0; 32],
                Vec::new(),
            ),
        };
        let label = authorization.scope_label(&mut None)?;
        authorization.policy_scope = self.fixture.issuer.issue(
            &label,
            &Capabilities::new([label.clone()]),
            None,
            None,
            &mut None,
        )?;
        let handle = ProjectSourceReadHandle::seal(
            authorization,
            &self.fixture.issuer.verifier(&mut None)?,
            &mut None,
        )?;
        let io = PreparationIo::from_budget(acyclic_fs::performance::BOUNDARY_BUDGET, &cancel)?;
        let reader = ProjectSourceReader::open(
            self.fixture.files.clone(),
            owner,
            handle,
            Limits::default(),
            io,
            Default::default(),
        )
        .await?;
        let mut effective: Capability =
            Capability::whole_volume(None, acyclic_fs::access::ActionSet::SourceRead.actions());
        effective.layers.extend(ceiling.layers);
        effective.expires_at = ceiling.expires_at;
        Ok((reader, effective))
    }

    async fn deny_secret(&mut self) -> TestResult {
        self.policy.version += 1;
        let mut denied = self.policy.layers[0][0].clone();
        denied.id = "secret".into();
        denied.effect = Effect::Deny;
        denied.resource.path = self.secret.path().into();
        denied.conditions = Conditions::default();
        self.policy.layers[0].push(denied);
        let reference =
            bounded_io::content_workspace(&self.fixture.files.provider, &self.project, None)?;
        let workspace = self.fixture.files.open(&reference, None).await?;
        let cancel = acyclic_fs::CancellationToken::new();
        let mut checkout = workspace
            .checkout(
                acyclic_fs::model::GenerationSelector::Head,
                acyclic_fs::model::CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await?
            .value;
        checkout
            .write_access_policy(
                &serde_json::to_vec(&self.policy)?,
                acyclic_fs::NamedAttributeWriteMode::Replace,
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await?;
        checkout
            .commit(
                acyclic_fs::OperationId::new(),
                None,
                None,
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancel,
                |_| {
                    std::future::ready(Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: Default::default(),
                        },
                    ))
                },
            )
            .await?;
        Ok(())
    }
}

async fn late_selected_verification(protected: bool) -> TestResult {
    let mut source = Source::new("verify-cut.txt").await?;
    let (reader, _) = source.reader(0).await?;
    let view = crate::filesystem::host::project_source_access(
        &reader.workspace,
        &reader.handle,
        &reader.owner.admission().grants,
        reader.generation.id(),
        &reader.io,
    )
    .await?;
    // Policy publication completes while the final task-owner verification is
    // still outstanding. Its journal remains valid; only the volume cut changed.
    source.deny_secret().await?;
    let result = if protected {
        Reader::finish_verification(
            &reader.workspace,
            &reader.owner,
            &view.0,
            &reader.io,
            &reader.owner_work,
        )
        .await
    } else {
        Reader::verify_owner(&reader.owner, &reader.io, &reader.owner_work).await?;
        crate::filesystem::access::require_unrestricted(
            &view.0,
            &[Action::List, Action::Read],
            "selected-project queries",
        )
    };
    assert!(
        matches!(result, Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized),
        "stale selected verification accepted a revoked volume cut"
    );
    Ok(())
}

#[tokio::test]
async fn final_selected_verification_confirms_volume_after_task_owner() -> TestResult {
    late_selected_verification(true).await
}

#[tokio::test]
#[should_panic(expected = "stale selected verification accepted")]
async fn task_owner_only_verification_negative_control() {
    late_selected_verification(false).await.unwrap();
}

#[tokio::test]
async fn selected_raw_reads_reject_restrictions_and_observe_revocation() -> TestResult {
    let mut source = Source::new("public.txt").await?;
    let (reader, _) = source.reader(0).await?;
    assert_eq!(reader.read_file(&source.secret).await?.as_ref(), b"secret");
    assert!(
        reader
            .list(&source.project, "", "", &source.generation, None, 10)
            .await
            .is_ok()
    );
    for mode in 1..=4 {
        let result = source.reader(mode).await;
        assert!(
            matches!(&result,
            Err(error) if matches!(error.downcast_ref::<Error>(), Some(Error::FilesystemAccessConfiguration(_)))),
            "mode {mode}: {:?}",
            result.as_ref().err()
        );
    }
    source.deny_secret().await?;
    assert!(matches!(
        reader.read_file(&source.secret).await,
        Err(Error::FilesystemAccessConfiguration(
            "selected-project queries"
        ))
    ));
    assert!(matches!(
        reader.read_file(&source.public).await,
        Err(Error::FilesystemAccessConfiguration(
            "selected-project queries"
        ))
    ));
    assert!(matches!(
        reader
            .read_path(
                &source.project,
                "",
                source.public.path(),
                &source.generation
            )
            .await,
        Err(Error::FilesystemAccessConfiguration(
            "selected-project queries"
        ))
    ));
    assert!(matches!(
        reader
            .list(&source.project, "", "", &source.generation, None, 10)
            .await,
        Err(Error::FilesystemAccessConfiguration(
            "selected-project queries"
        ))
    ));
    assert!(matches!(
        reader.diff(&source.public, &source.secret).await,
        Err(Error::FilesystemAccessConfiguration(
            "selected-project queries"
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn selected_import_inspection_requires_the_original_live_owner_and_selection() -> TestResult {
    use crate::core::{AggregateKind, Authority, AuthorityIssuer, Reducer, SchemaRegistry};
    use crate::filesystem::ParentProjectController;
    use crate::runtime::{Bindings, RuntimeScope};

    for deny_volume in [false, true] {
        let mut source = Source::new("selected-import.txt").await?;
        let (reader, _) = source.reader(0).await?;
        let owner = &reader.owner;
        let mut bindings = Bindings::local();
        bindings.scope =
            RuntimeScope::new(owner.admission().grants.clone(), owner.admission().limits)?;
        bindings.durable_host = Some(source.fixture.host.clone());
        let harness = bindings.build(None)?;
        let mut context = harness
            .durable_context(owner.task_id(), owner.operation_id(), None)
            .await?;
        Arc::get_mut(&mut context)
            .ok_or("context is already shared")?
            .owned_mailbox = Some(Arc::new(
            crate::filesystem::task_runtime::TaskJournalMailbox {
                host: source.fixture.host.clone(),
                owner: owner.clone(),
                native_custody: None,
            },
        ));

        // The destination conversation and task owner have independent authority.
        // The source is readable only through the selected-source handle.
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "selected-import-parent".into(),
        };
        let issuer = AuthorityIssuer::new("selected-import-parent", [17; 32], authority.clone());
        let verifier = issuer.verifier(&mut None)?;
        let agent = crate::AgentId::from_bytes([1; 16]);
        let scope = issuer.issue(
            "selected-import-parent",
            &Capabilities::new([
                "conversation:bind".to_owned(),
                "project:merge".to_owned(),
                source
                    .fixture
                    .destination
                    .capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )?;
        let mut parent = Reducer::new(
            authority,
            issuer.verifier(&mut None)?,
            SchemaRegistry::new(),
        );
        parent.apply(&crate::core::Command {
            operation_id: crate::OperationId::from_bytes([93; 16]),
            idempotency_key: crate::IdempotencyKey::new("selected-import-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: crate::core::Action::BindConversation { agent },
        })?;
        let files = source.fixture.files.clone();
        let destination_volume = source.fixture.destination.clone();
        let controller = ParentProjectController::new(
            &files,
            &parent,
            &verifier,
            &scope,
            &destination_volume,
            None,
        )?;
        assert!(
            controller
                .inspect_project_import(&source.project, &source.generation)
                .await
                .is_err()
        );
        let operation = reader.handle.authorization().operation_id;
        let selected = || {
            controller.inspect_project_import_from_selected_source(
                &source.project,
                &source.generation,
                &reader.handle,
                owner,
                &context,
                operation,
            )
        };
        let inspected = selected().await;
        assert!(
            inspected.is_ok(),
            "selected inspection attempted join planning: {inspected:?}"
        );
        let target = inspected?;
        let destination = bounded_io::content_workspace(
            &source.fixture.files.provider,
            &source.fixture.destination,
            None,
        )?;
        assert_eq!(
            source
                .fixture
                .files
                .resolve(&destination, None)
                .await?
                .generation,
            source.fixture.files.generation_ref_id(target, None)?
        );
        assert!(
            controller
                .inspect_project_import_from_selected_source(
                    &source.project,
                    &source.generation,
                    &reader.handle,
                    owner,
                    &context,
                    crate::OperationId::from_bytes([94; 16]),
                )
                .await
                .is_err(),
            "selected import accepted another operation"
        );
        if deny_volume {
            source.deny_secret().await?;
            assert!(
                controller
                    .inspect_project_import_from_selected_source(
                        &source.project,
                        &source.generation,
                        &reader.handle,
                        owner,
                        &context,
                        operation,
                    )
                    .await
                    .is_err(),
                "selected import bypassed the source policy cut"
            );
        } else {
            source.fixture.replace(30).await?;
            assert!(
                selected().await.is_err(),
                "selected import accepted a retired owner"
            );
        }
    }
    Ok(())
}
