//! The public host must publish through the same recoverable workspace graph.
use super::*;

async fn fork<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    filesystem: Fs<A, O>,
    lineage: acyclic_fs::WorkspaceLineageStorage,
) -> Result<acyclic_fs::WorkspaceLineageRecord> {
    let provider = ProviderRef::new(
        "host-lineage".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(filesystem, &provider, None, lineage)?;
    let owner = AgentId::from_bytes([57; 16]);
    let volume = |name: &str| {
        VolumeRef::new(
            provider.clone(),
            name.into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
            &mut None,
        )
    };
    let parent = volume("parent")?;
    let child = volume("child")?;
    let initial = host.create_volume(&parent).await?;
    let issuer = AuthorityIssuer::new(
        "host-lineage",
        [58; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "host-lineage".into(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &Capabilities::new([
            parent.capability(VolumeOperation::Read, &mut None)?,
            child.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner),
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let grant = |volume, operation| {
        ContentGrant::verify(&verifier, &scope, volume, operation, u64::MAX, &mut None)
    };
    let read = grant(&parent, VolumeOperation::Read)?;
    let write = grant(&child, VolumeOperation::Write)?;
    let key = IdempotencyKey::new("fork-through-public-host")?;
    let observed = host
        .fork_volume(
            &parent,
            &read,
            &initial.generation,
            &child,
            &write,
            None,
            &key,
        )
        .await?;
    let opened = host.open(&observed.workspace, None).await?;
    let source = host.open(&initial.workspace, None).await?;
    let record = host
        .workspace_graph()
        .authorize_join(opened.id(), source.id())
        .await?;
    assert!(record.ready);
    assert_eq!(
        record.fork_generation.digest().as_bytes(),
        initial.generation.as_resource().key()
    );
    assert_eq!(
        record.initial_generation.digest().as_bytes(),
        observed.generation.as_resource().key()
    );
    // Idempotent public replay must retain the exact recorded cut and creation.
    let replayed = host
        .fork_volume(
            &parent,
            &read,
            &initial.generation,
            &child,
            &write,
            None,
            &key,
        )
        .await?;
    assert_eq!(replayed.generation, observed.generation);
    assert_eq!(host.workspace_graph().resolve(opened.id()).await?, record);
    Ok(record)
}

#[tokio::test]
async fn public_memory_host_fork_registers_the_exact_creation() -> Result<()> {
    fork(
        Fs::memory(),
        acyclic_fs::WorkspaceLineageStorage::Memory(Arc::new(
            acyclic_fs::MemoryWorkspaceLineageStore::new(),
        )),
    )
    .await?;
    Ok(())
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn public_local_host_fork_survives_both_provider_owners()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let options = acyclic_fs::LocalOptions::new(directory.path().join("filesystem"));
    let state = directory.path().join("workspace");
    let (release, expected) = {
        let filesystem = Fs::local(options.clone()).await?;
        let release = filesystem.local_root_release_barrier().unwrap();
        let record = fork(
            filesystem,
            acyclic_fs::WorkspaceLineageStorage::Local(
                acyclic_fs::LocalCoreStateStore::open_shared_owner(&state)?,
            ),
        )
        .await?;
        (release, record)
    };
    release.wait_async().await;
    let filesystem = Fs::local(options).await?;
    let provider = ProviderRef::new(
        "host-lineage".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(
        filesystem,
        &provider,
        None,
        acyclic_fs::WorkspaceLineageStorage::Local(
            acyclic_fs::LocalCoreStateStore::open_shared_owner(state)?,
        ),
    )?;
    assert_eq!(
        host.workspace_graph()
            .resolve(expected.workspace_id)
            .await?,
        expected
    );
    let workspace = host
        .open(&workspace_ref(provider, &expected.workspace_name)?, None)
        .await?;
    assert_eq!(workspace.id(), expected.workspace_id);
    Ok(())
}
