use super::*;
use acyclic_fs::facade::FsWork;
use acyclic_fs::{CancellationToken, Lifecycle, VolumeConfig, WorkBudget};
use proptest::prelude::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn initialize() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    acyclic_fs::initialize_native_mount_services()?;
    Ok(())
}

async fn recovery(name: &str, durable: bool, recreate: bool) -> TestResult {
    initialize()?;
    let directory = tempfile::tempdir()?;
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.path())).await?;
    let config = VolumeConfig::native(if durable {
        Lifecycle::Durable
    } else {
        Lifecycle::Ephemeral
    });
    let original = filesystem
        .create_workspace_with_config(
            name,
            config,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    original.write_text("/retained", "original").await?;
    let head = original
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value
        .id();
    let recovered = if recreate {
        let mut operation = [0; 16];
        operation.copy_from_slice(&blake3::hash(name.as_bytes()).as_bytes()[..16]);
        filesystem
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                name,
                acyclic_fs::VolumeInitialization {
                    config: VolumeConfig::native(Lifecycle::Durable),
                    access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
                },
                acyclic_fs::OperationId::from_bytes(operation),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                None,
            )
            .await
            .map(|receipt| receipt.value.workspace())
            .map_err(|failure| Error::Invalid(failure.error.to_string()))
    } else {
        local_owner_workspace(&filesystem, name).await
    };
    let recovered = recovered.expect("existing workspace must recover its recorded creation");
    assert_eq!(recovered.id(), original.id());
    assert_eq!(recovered.volume_config(), config);
    assert_eq!(
        recovered
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value
            .id(),
        head
    );
    assert_eq!(
        recovered
            .read(
                "/retained",
                64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
        b"original"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn existing_owner_workspaces_keep_creation_and_configuration(name in "[a-z]{1,12}", durable in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(recovery(&name, durable, false))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
#[should_panic(expected = "existing workspace must recover its recorded creation")]
async fn recreating_an_existing_owner_is_detected() {
    recovery("previous-creation", true, true).await.unwrap();
}

async fn readonly_import(bytes: &[u8], stale_options: bool, pinned_capability: bool) -> TestResult {
    use acyclic_fs::demand::native::NativeDemandSource;
    use acyclic_fs::{
        LazyWorkspace, MemoryLazyWorkspaceStore, MountOptions, MountPublication, PublicationPermit,
    };
    initialize()?;
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("hot"))?;
    std::fs::write(source.path().join("hot/file"), bytes)?;
    let destination = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let demand = NativeDemandSource::open(
        std::borrow::Cow::Owned(source.path().to_path_buf()),
        config.profile,
        config.limits,
        None,
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?
    .value;
    let lazy = LazyWorkspace::attach(
        &acyclic_fs::Fs::memory(),
        "readonly-import",
        Arc::new(demand),
        MemoryLazyWorkspaceStore::default(),
        acyclic_fs::VolumeInitialization {
            config,
            access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
        },
        acyclic_fs::OperationId::new(),
        &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
    )
    .await?
    .0;
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let original = lazy.workspace().head(&work, &cancellation).await?.id();
    let options = if pinned_capability {
        use acyclic_fs::access::{
            Action, Capability, GenerationRange, Generations, Grant, ResourcePattern,
        };
        let issuer = crate::core::AuthorityIssuer::new(
            "readonly-import",
            [37; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: "readonly-import".into(),
            },
        );
        let capability = Capability {
            layers: vec![vec![Grant {
                actions: [Action::List, Action::Read, Action::ReadHistory]
                    .into_iter()
                    .collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: Some(
                        uuid::Uuid::from_bytes(lazy.workspace().id().volume_id().into_bytes())
                            .to_string(),
                    ),
                    generations: Generations::Range(GenerationRange { first: 1, last: 1 }),
                    path: "**".into(),
                },
            }]],
            expires_at: None,
        };
        let grants = crate::Capabilities::new([capability.to_scope_label()?]);
        let scope = issuer.issue("native-volume", &grants, None, None, &mut None)?;
        authenticated_mount_options(
            lazy.workspace(),
            original,
            NativeAccessAuthority {
                verifier: &issuer.verifier(&mut None)?,
                scope: &scope,
                ceiling: &grants,
            },
            false,
            &work,
        )
        .await?
        .native_context(&work, None, None)
    } else {
        local_owner_mount_options(lazy.workspace(), "readonly-import", false).await?
    }
    .publication(MountPublication::Manual)
    .subdirectory("/hot");
    // The owner's actual ledger is retained by its options.
    let view = options.require_access_view()?.clone();
    assert_eq!(view.current()?.data_generation(), Some(original));
    let plain = MountOptions::read_only()
        .access_view(view.clone())
        .publication(MountPublication::Manual)
        .subdirectory("/hot");
    assert!(matches!(
        lazy.prepare_native_working_set(
            plain,
            &acyclic_fs::MaterializeOptions::native(destination.path()),
            WorkBudget::UNBOUNDED,
            &cancellation,
            PublicationPermit::Unrestricted,
        )
        .await,
        Err(acyclic_fs::MountLifecycleError::NativePolicy(
            "exactification requires the authenticated selection owner"
        ))
    ));
    assert_eq!(
        lazy.workspace().head(&work, &cancellation).await?.id(),
        original
    );
    let prepared = if stale_options {
        let exact = lazy
            .exactify(
                "/hot",
                PublicationPermit::Unrestricted,
                &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await?;
        exact
            .prepare_native_working_set(
                &options,
                &acyclic_fs::MaterializeOptions::native(destination.path()),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
    } else {
        lazy.prepare_native_working_set(
            options,
            &acyclic_fs::MaterializeOptions::native(destination.path()),
            WorkBudget::UNBOUNDED,
            &cancellation,
            PublicationPermit::Unrestricted,
        )
        .await
    };
    if pinned_capability {
        assert!(
            matches!(
                prepared,
                Err(acyclic_fs::MountLifecycleError::NativePolicy(
                    "materialized working sets require unrestricted disclosure"
                ))
            ),
            "exactification widened a pinned capability or failed outside authorization"
        );
        assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
        return Ok(());
    }
    let prepared = prepared.expect("read-only exactification must retain authorization");
    assert_ne!(
        lazy.workspace().head(&work, &cancellation).await?.id(),
        original
    );
    assert_eq!(
        view.current()?.data_generation(),
        Some(original),
        "rebinding changed an existing pinned view"
    );
    assert_eq!(std::fs::read(prepared.path().join("hot/file"))?, bytes);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn readonly_import_reauthorizes_the_exact_generation(bytes in prop::collection::vec(any::<u8>(), 1..32)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(readonly_import(&bytes, false, false))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
#[should_panic(expected = "read-only exactification must retain authorization")]
async fn unchanged_preimport_options_are_detected() {
    readonly_import(b"imported", true, false).await.unwrap();
}

#[tokio::test]
async fn exactification_cannot_widen_a_pinned_capability() -> TestResult {
    readonly_import(b"pinned", false, true).await
}
