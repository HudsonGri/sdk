use super::*;

#[tokio::test]
async fn journal_and_content_adapters_reuse_one_authenticated_owner() -> Result<()> {
    use acyclic_auth::crypto::SignatureVerification;
    use acyclic_test_support::differential::observe;
    let (fixture, issuer) = super::super::test_support::journal().await?;
    let verifier = fixture.content_authority().verifier();
    let scope = fixture.content_authority().scope();
    let (control, baseline): (_, Vec<SignatureVerification>) = observe(
        || (),
        |()| {
            ContentGrant::verify(
                verifier,
                scope,
                &fixture.volume,
                VolumeOperation::Write,
                u64::MAX,
                &mut None,
            )
        },
    );
    control?;
    assert!(!baseline.is_empty());
    let (journal, signatures): (_, Vec<SignatureVerification>) = observe(
        || (),
        |()| {
            FilesystemExecutionJournal::new(
                fixture.stream.clone(),
                fixture.host.clone(),
                &SchemaRegistry::new(),
                crate::filesystem::FilesystemJournalBinding {
                    volume: &fixture.volume,
                    verifier,
                    scope,
                    maximum_bytes: 65_536,
                    io: None,
                },
            )
        },
    );
    let journal = journal?;
    let journal_signatures = signatures.len();
    let reader = || {
        crate::filesystem::FilesystemContentVerifier::from_authenticated(
            journal.host.clone(),
            journal.content_authority(),
            65_536,
            None,
        )
    };
    let publisher = || {
        crate::filesystem::FilesystemContentPublisher::from_authenticated(
            journal.host.clone(),
            &journal.volume,
            journal.content_authority(),
            65_536,
            None,
        )
    };
    let (result, reader_signatures): (_, Vec<SignatureVerification>) =
        observe(|| (), |()| reader());
    drop(result?);
    let (result, publisher_signatures): (_, Vec<SignatureVerification>) =
        observe(|| (), |()| publisher());
    drop(result?);
    let repeated = [
        ("journal interaction", journal_signatures != baseline.len()),
        ("content reader", !reader_signatures.is_empty()),
        ("content publisher", !publisher_signatures.is_empty()),
    ];
    assert!(
        repeated.iter().all(|(_, repeated)| !repeated),
        "adapters repeated immutable authentication: {repeated:?}"
    );
    issuer.revoke_workload(scope)?;
    assert!(
        matches!(reader(), Err(Error::Unauthorized(_))),
        "admitted content copy bypassed revocation"
    );
    assert!(
        matches!(publisher(), Err(Error::Unauthorized(_))),
        "admitted publisher copy bypassed revocation"
    );
    Ok(())
}

#[tokio::test]
async fn interaction_owner_authenticates_once() -> Result<()> {
    use acyclic_auth::crypto::SignatureVerification;
    use acyclic_test_support::differential::observe;
    let (journal, _issuer) = super::super::test_support::journal().await?;
    let verifier = journal.write_grant.authority.verifier();
    let scope = journal.write_grant.authority.scope();
    let (control, baseline): (_, Vec<SignatureVerification>) = observe(
        || (),
        |()| {
            ContentGrant::verify(
                verifier,
                scope,
                &journal.volume,
                VolumeOperation::Write,
                u64::MAX,
                &mut None,
            )
        },
    );
    control?;
    assert!(!baseline.is_empty(), "control did not authenticate");
    let (host, authenticated): (_, Vec<SignatureVerification>) = observe(
        || (),
        |()| {
            FilesystemInteractionHost::new(
                journal.stream.clone(),
                journal.host.clone(),
                verifier.audience(),
                &SchemaRegistry::new(),
                crate::filesystem::FilesystemJournalBinding {
                    volume: &journal.volume,
                    verifier,
                    scope,
                    maximum_bytes: 65_536,
                    io: None,
                },
            )
        },
    );
    host?;
    assert_eq!(
        authenticated.len(),
        baseline.len(),
        "interaction admission authenticated the owner more than once"
    );
    Ok(())
}

#[tokio::test]
async fn interaction_staging_reuses_live_admitted_authority() -> Result<()> {
    use acyclic_auth::crypto::SignatureVerification;
    use acyclic_test_support::differential::observe_async;
    let (journal, issuer) = super::super::test_support::journal().await?;
    let verifier = journal.write_grant.authority.verifier();
    let scope = journal.write_grant.authority.scope();
    let host = FilesystemInteractionHost::new(
        journal.stream.clone(),
        journal.host.clone(),
        verifier.audience(),
        &SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &journal.volume,
            verifier,
            scope,
            maximum_bytes: 65_536,
            io: None,
        },
    )?;
    let id = InteractionId::new();
    let answer = InteractionResponse::Question {
        value: serde_json::json!("answer"),
    };
    let (staged, signatures): (_, Vec<SignatureVerification>) =
        observe_async(host.stage_answer(id, 1, OperationId::new(), &answer, None)).await;
    let staged = staged?;
    assert!(
        signatures.is_empty(),
        "interaction staging reauthenticated its admitted owner"
    );
    let bytes = journal
        .host
        .read_pinned_receipt_io(&staged, 65_536, None)
        .await?
        .retain()
        .await?;
    assert_eq!(
        serde_json::from_slice::<InteractionResponse>(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        answer
    );
    issuer.revoke_workload(scope)?;
    assert!(
        matches!(
            host.read_request(id, None).await,
            Err(Error::Unauthorized(_))
        ),
        "interaction read bypassed live revocation"
    );
    assert!(
        matches!(
            host.stage_answer(id, 1, OperationId::new(), &answer, None)
                .await,
            Err(Error::Unauthorized(_))
        ),
        "interaction staging bypassed live revocation"
    );
    Ok(())
}

#[tokio::test]
async fn interaction_history_read_reuses_its_admitted_content_reader() -> Result<()> {
    use acyclic_auth::crypto::SignatureVerification;
    let (journal, _issuer) = super::super::test_support::journal().await?;
    let verifier = journal.write_grant.authority.verifier();
    let host = FilesystemInteractionHost::new(
        journal.stream.clone(),
        journal.host.clone(),
        verifier.audience(),
        &SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &journal.volume,
            verifier,
            scope: journal.write_grant.authority.scope(),
            maximum_bytes: 65_536,
            io: None,
        },
    )?;
    // This fixture has no bound conversation. Opening its public reader still
    // constructs and drops the content barrier before reporting that conflict.
    let (result, signatures) =
        acyclic_test_support::differential::observe_async(host.read(InteractionId::new(), None))
            .await;
    assert!(matches!(result, Err(Error::Conflict(_))));
    let signatures: Vec<SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "interaction history reconstructed raw read authority"
    );
    Ok(())
}

#[tokio::test]
async fn journal_admission_authenticates_its_single_grant_once() -> Result<()> {
    let (journal, _issuer) = super::super::test_support::journal().await?;
    let verifier = journal.write_grant.authority.verifier();
    let scope = journal.write_grant.authority.scope();
    let authenticate = |journal_admission: bool| {
        acyclic_test_support::differential::observe(
            || (),
            |()| -> Result<ContentGrant> {
                let mut work = None;
                let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
                if journal_admission {
                    journal_authority(verifier, scope, &journal.volume, None, &mut quota)
                } else {
                    ContentGrant::verify_in_quota(
                        verifier,
                        scope,
                        &journal.volume,
                        VolumeOperation::Write,
                        u64::MAX,
                        &mut quota,
                    )
                }
            },
        )
    };
    let (baseline, baseline_signatures) = authenticate(false);
    baseline?;
    let baseline_signatures: Vec<acyclic_auth::crypto::SignatureVerification> = baseline_signatures;
    assert!(
        !baseline_signatures.is_empty(),
        "control did not authenticate"
    );
    let (admitted, signatures) = authenticate(true);
    admitted?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert_eq!(
        signatures.len(),
        baseline_signatures.len(),
        "journal duplicated the grant's authenticated authority"
    );
    Ok(())
}

#[tokio::test]
async fn rebinding_rotates_the_execution_identity_and_discards_cached_authority() -> Result<()> {
    let (mut journal, _issuer) = super::super::test_support::journal().await?;
    let operation = OperationId::new();
    let mut identity = journal.cache_identity();
    assert!(identity.is_some());
    // Populate the real authenticated summary, so this also checks invalidation.
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    assert_eq!(
        journal.verified.get_mut().summary().operation,
        Some(operation)
    );
    journal = journal.with_turn(operation, OperationId::new());
    assert_ne!(journal.cache_identity(), identity);
    assert_eq!(journal.verified.get_mut().summary().operation, None);
    identity = journal.cache_identity();
    journal = journal.with_task_files(None);
    assert_ne!(journal.cache_identity(), identity);
    identity = journal.cache_identity();
    journal = journal.with_history_read_limits(crate::store::HistoryReadLimits {
        maximum_events: 8,
        maximum_bytes: 65_536,
    })?;
    assert_ne!(journal.cache_identity(), identity);
    identity = journal.cache_identity();
    let input = crate::filesystem::FilesystemContentVerifier::new(
        journal.host.clone(),
        journal.write_grant.authority.verifier(),
        journal.write_grant.authority.scope(),
        65_536,
        None,
    )?;
    journal = journal.with_input_verifier(input);
    assert_ne!(journal.cache_identity(), identity);
    identity = journal.cache_identity();
    let verifier = journal.write_grant.authority.verifier().copy(&mut None)?;
    let scope = journal.write_grant.authority.scope().copy(&mut None)?;
    let volume = journal.volume.copy(&mut None)?;
    journal =
        journal.with_interaction_owner(&verifier, &SchemaRegistry::new(), &scope, &volume, None)?;
    assert_ne!(journal.cache_identity(), identity);
    Ok(())
}

#[tokio::test]
async fn journal_retains_immutable_authentication_but_rechecks_live_revocation() -> Result<()> {
    let (journal, issuer) = super::super::test_support::journal().await?;
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                ContentGrant::authorize_authenticated_volume(
                    &journal.write_grant.authority,
                    &journal.volume,
                    operation,
                    u64::MAX,
                    &mut None,
                )?;
            }
            Ok(())
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "journal reauthenticated its immutable owner"
    );
    issuer.revoke_workload(journal.write_grant.authority.scope())?;
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        assert!(matches!(
            ContentGrant::authorize_authenticated_volume(
                &journal.write_grant.authority,
                &journal.volume,
                operation,
                u64::MAX,
                &mut None,
            ),
            Err(Error::Unauthorized(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn private_staging_reuses_its_original_authenticated_grant_and_rechecks_revocation()
-> Result<()> {
    let (journal, issuer) = super::super::test_support::journal().await?;
    let (result, signatures) = acyclic_test_support::differential::observe_async(
        journal.stage_with_io(OperationId::new(), "stage", b"{}", "application/json", None),
    )
    .await;
    let _: crate::conversation::FileRef = result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "private staging reauthenticated its immutable owner"
    );
    issuer.revoke_workload(journal.write_grant.authority.scope())?;
    assert!(matches!(
        journal
            .stage_with_io(
                OperationId::new(),
                "revoked",
                b"{}",
                "application/json",
                None
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}
