use super::*;

type MemoryJournal = FilesystemExecutionJournal<
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

/// One journal over a fresh memory filesystem.
async fn journal() -> Result<MemoryJournal> {
    let provider = ProviderRef::new(
        "group-commit".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = AgentId::from_bytes([6; 16]);
    let volume = VolumeRef::new(
        provider,
        "journal".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityIssuer::new(
        "group-commit",
        [82; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Task,
            id: crate::TaskId::from_bytes([7; 16]).to_string(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        &SchemaRegistry::new(),
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            maximum_bytes: 4096,
            io: None,
        },
    )
}

fn payloads<'a>(members: &[(&'a str, &'a [u8])]) -> Vec<crate::executor::StagedPayload<'a>> {
    members
        .iter()
        .map(|(key, bytes)| crate::executor::StagedPayload {
            idempotency_key: key,
            bytes,
            media_type: "application/json",
        })
        .collect()
}

#[tokio::test]
async fn a_group_is_one_generation_and_one_retry_unit() -> Result<()> {
    crate::executor::model_group_faults::refuse_adoption(false);
    let journal = journal().await?;
    let operation = OperationId::from_bytes([8; 16]);
    let group = payloads(&[("event:0", b"0"), ("event:1", b"1"), ("event:2", b"2")]);
    let files = journal.stage_all(operation, &group, None)?.await?;
    // Every member names the one generation that published all of them, at
    // the path its own key derives, and reads back as its own bytes.
    assert_eq!(files.len(), 3);
    for (file, payload) in files.iter().zip(&group) {
        assert_eq!(file.version(), files[0].version());
        let (_, path) = journal.stage_identity_with_io(operation, payload.idempotency_key, None)?;
        assert_eq!(file.path(), path);
        assert_eq!(journal.load(file, None)?.await?.as_ref(), payload.bytes);
    }
    // Restaging the identical group replays that publication.
    assert_eq!(journal.stage_all(operation, &group, None)?.await?, files);
    // The same keys with other bytes are another request.
    let changed = payloads(&[("event:0", b"0"), ("event:1", b"x"), ("event:2", b"2")]);
    assert_eq!(
        journal
            .stage_all(operation, &changed, None)?
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Conflict
    );
    // Recovery may regroup: a member an earlier group published is that
    // group's member, and only the absent members are published again.
    let regrouped = payloads(&[("event:2", b"2"), ("event:3", b"3")]);
    let again = journal.stage_all(operation, &regrouped, None)?.await?;
    assert_eq!(again[0], files[2]);
    assert_ne!(again[1].version(), files[0].version());
    assert_eq!(journal.load(&again[1], None)?.await?.as_ref(), b"3");
    // Retrying that regrouped request replays it, members of both origins.
    assert_eq!(
        journal.stage_all(operation, &regrouped, None)?.await?,
        again
    );
    // Every member already present: nothing new is published.
    let republished = payloads(&[("event:3", b"3"), ("event:1", b"1")]);
    assert_eq!(
        journal.stage_all(operation, &republished, None)?.await?,
        [again[1].clone(), files[1].clone()]
    );
    // Negative control: a regrouped member with other bytes is refused, not
    // adopted from the earlier publication or republished over it.
    for tampered in [
        payloads(&[("event:1", b"x"), ("event:5", b"5")]),
        payloads(&[("event:1", b"x")]),
    ] {
        assert_eq!(
            journal
                .stage_all(operation, &tampered, None)?
                .await
                .unwrap_err()
                .code(),
            crate::wire::ErrorCode::Conflict
        );
    }
    assert_eq!(journal.load(&files[1], None)?.await?.as_ref(), b"1");
    assert_eq!(crate::executor::model_group_faults::adopted(), 4);
    // A lone payload keeps its own retry key: its single-item group is the
    // ordinary staged publication, recoverable by that key.
    let lone = payloads(&[("event:4", b"4")]);
    let [file] = <[FileRef; 1]>::try_from(journal.stage_all(operation, &lone, None)?.await?)
        .map_err(|_| Error::Invalid("one file".into()))?;
    assert_eq!(
        journal
            .staged_ref_with_io(operation, "event:4", None)
            .await?,
        file
    );
    assert_eq!(
        journal
            .stage(operation, "event:4", b"4", "application/json", None)?
            .await?,
        file
    );
    Ok(())
}

#[tokio::test]
async fn a_group_needs_distinct_members() -> Result<()> {
    let journal = journal().await?;
    let operation = OperationId::from_bytes([9; 16]);
    for members in [
        &[][..],
        &[("event:0", &b"0"[..]), ("event:0", &b"0"[..])][..],
    ] {
        assert_eq!(
            journal
                .stage_all(operation, &payloads(members), None)?
                .await
                .unwrap_err()
                .code(),
            crate::wire::ErrorCode::Invalid
        );
    }
    Ok(())
}

#[tokio::test]
async fn negative_control_without_adoption_a_regrouped_member_is_republished() -> Result<()> {
    // Refusing adoption, a regrouped member is published again under the
    // new group's key, and its retry key then admits other content.
    let journal = journal().await?;
    let operation = OperationId::from_bytes([10; 16]);
    let group = payloads(&[("event:0", b"0"), ("event:1", b"1")]);
    let files = journal.stage_all(operation, &group, None)?.await?;
    crate::executor::model_group_faults::refuse_adoption(true);
    let regrouped = journal
        .stage_all(
            operation,
            &payloads(&[("event:1", b"1"), ("event:2", b"2")]),
            None,
        )?
        .await;
    let tampered = journal
        .stage_all(operation, &payloads(&[("event:0", b"x")]), None)?
        .await;
    crate::executor::model_group_faults::refuse_adoption(false);
    assert_ne!(regrouped?[0], files[1]);
    assert!(tampered.is_ok());
    Ok(())
}
