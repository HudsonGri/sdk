use super::*;

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
