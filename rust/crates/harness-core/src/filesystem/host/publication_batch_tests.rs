use super::*;
use crate::core::{AggregateKind, AuthorityIssuer};

fn publication_grants(volume: &VolumeRef, agent: AgentId) -> Result<(ContentGrant, ContentGrant)> {
    let issuer = AuthorityIssuer::new(
        "batch",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "batch".into(),
        },
    );
    let scope = issuer.issue(
        "batch-owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Write, &mut None)?,
            volume.capability(VolumeOperation::Read, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let write = ContentGrant::verify(
        &verifier,
        &scope,
        volume,
        VolumeOperation::Write,
        64,
        &mut None,
    )?;
    let read = ContentGrant::verify(
        &verifier,
        &scope,
        volume,
        VolumeOperation::Read,
        64,
        &mut None,
    )?;
    Ok((write, read))
}

#[tokio::test]
async fn publication_adapters_preserve_pinned_content_and_reject_changed_operation() -> Result<()> {
    for adapter in 0..3 {
        let provider =
            ProviderRef::new("batch".into(), "filesystem".into(), "1".into(), &mut None)?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = crate::AgentId::from_bytes([37; 16]);
        let volume = VolumeRef::new(
            provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let (write, read) = publication_grants(&volume, agent)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
        let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = match adapter {
            0 => None,
            1 => Some(PreparationIo::from_budget(budget, &cancellation)?),
            _ => Some(PreparationIo::from_native_work(&native)?),
        };
        let access = crate::conversation::ContentAccess {
            ceiling: None,
            io: io.as_ref(),
        };
        let key = IdempotencyKey::new("batch-first")?;
        let original = host
            .put_content(
                &volume,
                &write,
                "nested/value",
                b"long original",
                "text/plain",
                "value",
                64,
                &key,
                None,
                access,
            )
            .await?;
        let replacement = host
            .put_content(
                &volume,
                &write,
                "nested/value",
                b"x",
                "text/plain",
                "value",
                64,
                &IdempotencyKey::new("batch-replacement")?,
                None,
                access,
            )
            .await?;
        assert_eq!(
            host.read_content(&original, &read, 64, io.as_ref())
                .await?
                .as_ref(),
            b"long original"
        );
        assert_eq!(
            host.read_content(&replacement, &read, 64, io.as_ref())
                .await?
                .as_ref(),
            b"x"
        );
        assert_eq!(
            host.content_for_operation(&volume, &write, &key, "nested/value", 64, access)
                .await?,
            Some(original)
        );
        let workspace = fork_preparer_io::content_workspace(&provider, &volume, None)?;
        let before = host.resolve(&workspace, None).await?;
        assert!(
            host.put_content(
                &volume,
                &write,
                "nested/value",
                b"forged",
                "text/plain",
                "value",
                64,
                &key,
                None,
                access,
            )
            .await
            .is_err()
        );
        assert_eq!(host.resolve(&workspace, None).await?, before);
    }
    Ok(())
}

#[tokio::test]
async fn immutable_records_cannot_enter_the_replacement_batch() -> Result<()> {
    let provider = ProviderRef::new(
        "immutable-batch".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let filesystem = Fs::memory();
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    filesystem
        .create_workspace("records", budget, &cancellation)
        .await
        .map_err(|failure| map_error(failure.error))?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let workspace = workspace_ref(provider, "records")?;
    let io = PreparationIo::from_budget(budget, &cancellation)?;
    let first = [WorkspaceMutation::PutFile {
        path: "/sealed".into(),
        bytes: b"original".to_vec().into(),
    }];
    host.create_records(
        &workspace,
        &first,
        IdempotencyKey::new("create-original")?,
        Some(&io),
        None,
    )?
    .await?;
    let before = host.resolve(&workspace, None).await?;
    let forged = [WorkspaceMutation::PutFile {
        path: "/sealed".into(),
        bytes: b"forged".to_vec().into(),
    }];
    assert!(
        host.create_records(
            &workspace,
            &forged,
            IdempotencyKey::new("overwrite-sealed")?,
            Some(&io),
            None
        )?
        .await
        .is_err()
    );
    assert_eq!(host.resolve(&workspace, None).await?, before);
    assert_eq!(
        host.read(&workspace, None, "/sealed", 64, None)
            .await?
            .as_ref(),
        b"original"
    );
    Ok(())
}
