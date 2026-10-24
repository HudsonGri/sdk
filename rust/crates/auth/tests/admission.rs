//! Trusted runtime admission receipts and adversarial historical replay.
#[path = "common/signed.rs"]
mod signed;
use acyclic_auth::{
    admission::RuntimeAdmission,
    authority::{self, PolicyDecision},
    contract::*,
    verify::{self, CertifiedSigner, VerifyError},
};
use ed25519_dalek::SigningKey;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn authenticated_admission_transfers_the_single_verified_workload() -> TestResult {
    let (signer, workload) = fixture()?;
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::try_new(vec![])?;
    let usage = signed::limits(None);
    let request = signed::request("fs:/volume/public/file", &attributes, &usage);
    let proof = RuntimeAdmission::new(&signer, &trust, signed::NOW)
        .record(&workload, &request, [7; 32], &[8; 32])?;
    let authenticated = proof.authenticate_history(&trust, signed::NOW)?;
    let identity = authenticated.workload().block_ids().as_ptr();
    authenticated.check_request(&trust, &request, [7; 32])?;
    authenticated.check_request(&trust, &request, [7; 32])?;
    assert!(
        authenticated
            .check_request(&trust, &request, [9; 32])
            .is_err()
    );
    assert_eq!(identity, authenticated.workload().block_ids().as_ptr());
    let transferred = authenticated.authorize(&trust, &request, [7; 32])?;
    assert_eq!(identity, transferred.block_ids().as_ptr());
    // The former second verification creates a distinct authenticated chain,
    // and fails the same ownership oracle while the first remains alive.
    let duplicate = proof.verify_history(&trust, &request, [7; 32])?;
    assert_ne!(identity, duplicate.block_ids().as_ptr());
    assert_eq!(transferred.block_ids(), duplicate.block_ids());
    Ok(())
}

#[test]
fn authenticated_admission_rechecks_every_request_and_current_trust() -> TestResult {
    let (signer, workload) = fixture()?;
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::try_new(vec![])?;
    let usage = signed::limits(None);
    let request = signed::request("fs:/volume/public/file", &attributes, &usage);
    let proof = RuntimeAdmission::new(&signer, &trust, signed::NOW)
        .record(&workload, &request, [7; 32], &[8; 32])?;
    let revoked = signed::Snapshot {
        revoked_blocks: vec![*workload.block_ids().first().ok_or(VerifyError::Malformed)?],
        ..Default::default()
    };
    let wrong_path = authority::Request {
        target: authority::Target::Resource {
            action: "fs/read",
            resource: "fs:/volume/private/file",
        },
        ..request
    };
    let future = authority::Request {
        now_unix_seconds: signed::NOW - 1,
        ..request
    };
    // Saving a successful authorization would admit these changed requests.
    // Retained and newly authenticated proofs must both recheck each decision.
    let saved_allow = proof.verify_history(&trust, &request, [7; 32]).is_ok();
    assert!(saved_allow);
    let retained = proof.authenticate_history(&trust, signed::NOW)?;
    for (current, changed, payload) in [
        (&trust, &request, [6; 32]),
        (&trust, &wrong_path, [7; 32]),
        (&trust, &future, [7; 32]),
        (&revoked, &request, [7; 32]),
    ] {
        let authenticated = proof.authenticate_history(&trust, signed::NOW)?;
        assert!(retained.check_request(current, changed, payload).is_err());
        assert!(authenticated.authorize(current, changed, payload).is_err());
        assert!(proof.verify_history(current, changed, payload).is_err());
        assert!(
            saved_allow,
            "saved-decision negative control was not admitted"
        );
    }
    // Authentication itself cannot stand in for a live, unexpired admission.
    let expired = authority::Request {
        now_unix_seconds: 1100,
        ..request
    };
    let authenticated = proof.authenticate_history(&trust, signed::NOW)?;
    let historical = authenticated.authorize(&trust, &expired, [7; 32])?;
    assert!(
        historical
            .authorize(&trust, &expired, PolicyDecision::Allow)
            .is_err()
    );
    let mut forged = proof;
    *forged.workload.first_mut().ok_or(VerifyError::Malformed)? ^= 1;
    assert!(forged.authenticate_history(&trust, signed::NOW).is_err());
    assert!(forged.verify_history(&trust, &request, [7; 32]).is_err());
    Ok(())
}

fn fixture() -> TestResult<(CertifiedSigner, verify::VerifiedCapability)> {
    let encoded = signed::root(|block| {
        if let Some(certificate) = block
            .certificates
            .as_mut()
            .and_then(|chain| chain.keys.first_mut())
        {
            certificate.key.grants.grants.push(Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: "harness:/admissions".into(),
                        prefix: true,
                    },
                    actions: vec!["harness/admit".into()],
                }),
            });
        }
        block.caveats = vec![
            Caveat {
                kind: CaveatKind::Grants(GrantSet {
                    grants: vec![Grant {
                        kind: GrantKind::Resource(ResourceGrant {
                            resource: ResourcePattern {
                                name: "fs:/volume/public".into(),
                                prefix: true,
                            },
                            actions: vec!["fs/read".into()],
                        }),
                    }],
                }),
            },
            Caveat {
                kind: CaveatKind::Audience(ResourcePattern {
                    name: "workers:/test".into(),
                    prefix: false,
                }),
            },
            Caveat {
                kind: CaveatKind::ExpiresAtUnixSeconds(200),
            },
        ];
    });
    let envelope = authority::decode_capability(&encoded)?;
    let first = envelope.blocks.first().ok_or(VerifyError::Malformed)?;
    let certificates = authority::decode_block(&first.payload)?
        .certificates
        .ok_or(VerifyError::Malformed)?;
    Ok((
        CertifiedSigner::new(certificates, SigningKey::from_bytes(&[3; 32]))?,
        verify::verify(&encoded, &signed::Snapshot::default(), signed::NOW)?,
    ))
}

#[test]
fn historical_evidence_survives_expiry_without_becoming_fresh_authority() -> TestResult {
    let (signer, workload) = fixture()?;
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::try_new(vec![])?;
    let usage = signed::limits(None);
    let request = signed::request("fs:/volume/public/file", &attributes, &usage);
    let proof = RuntimeAdmission::new(&signer, &trust, signed::NOW)
        .record(&workload, &request, [7; 32], &[8; 32])?;
    let later = authority::Request {
        now_unix_seconds: 1100,
        ..request
    };
    let historical = proof.verify_history(&trust, &later, [7; 32])?;
    assert!(historical.is_sealed());
    assert!(historical.attenuate(vec![], &[9; 32]).is_err());
    assert!(
        historical
            .authorize(&trust, &later, PolicyDecision::Allow)
            .is_err()
    );
    assert!(matches!(
        RuntimeAdmission::new(&signer, &trust, 1100).record(&workload, &later, [7; 32], &[10; 32]),
        Err(VerifyError::Time)
    ));
    assert!(proof.verify_history(&trust, &later, [6; 32]).is_err());
    Ok(())
}

#[test]
fn receipts_bind_the_exact_chain_clock_recipient_action_path_and_payload() -> TestResult {
    let (signer, workload) = fixture()?;
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::try_new(vec![])?;
    let usage = signed::limits(None);
    let request = signed::request("fs:/volume/public/file", &attributes, &usage);
    let proof = RuntimeAdmission::new(&signer, &trust, signed::NOW)
        .record(&workload, &request, [7; 32], &[8; 32])?;
    assert!(proof.verify_history(&trust, &request, [7; 32]).is_ok());
    let changed = authority::Request {
        audience: "workers:/other",
        ..request
    };
    assert!(proof.verify_history(&trust, &changed, [7; 32]).is_err());
    for (action, resource) in [
        ("fs/read", "fs:/volume/private/file"),
        ("fs/write", "fs:/volume/public/file"),
    ] {
        let changed = authority::Request {
            target: authority::Target::Resource { action, resource },
            ..request
        };
        assert!(proof.verify_history(&trust, &changed, [7; 32]).is_err());
        assert!(
            RuntimeAdmission::new(&signer, &trust, signed::NOW)
                .record(&workload, &changed, [7; 32], &[10; 32])
                .is_err()
        );
    }
    let mut changed = proof.clone();
    changed.admitted_at_unix_seconds -= 1;
    assert!(changed.verify_history(&trust, &request, [7; 32]).is_err());
    let mut changed = proof.clone();
    changed.admitted_at_unix_seconds += 1;
    assert!(changed.verify_history(&trust, &request, [7; 32]).is_err());
    for receipt in [false, true] {
        let mut changed = proof.clone();
        let bytes = if receipt {
            &mut changed.receipt
        } else {
            &mut changed.workload
        };
        let index = bytes.len() / 2;
        *bytes.get_mut(index).ok_or(VerifyError::Malformed)? ^= 1;
        assert!(changed.verify_history(&trust, &request, [7; 32]).is_err());
    }
    Ok(())
}

#[test]
fn holder_signed_backdating_and_revoked_ancestors_cannot_authenticate_history() -> TestResult {
    let (signer, workload) = fixture()?;
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::try_new(vec![])?;
    let usage = signed::limits(None);
    let request = signed::request("fs:/volume/public/file", &attributes, &usage);
    let proof = RuntimeAdmission::new(&signer, &trust, signed::NOW)
        .record(&workload, &request, [7; 32], &[8; 32])?;
    let mut forged = proof.clone();
    forged.receipt = workload
        .attenuate(
            vec![Caveat {
                kind: CaveatKind::NotBeforeUnixSeconds(signed::NOW),
            }],
            &[11; 32],
        )?
        .seal()?
        .encode()
        .to_vec();
    assert!(matches!(
        forged.verify_history(&trust, &request, [7; 32]),
        Err(VerifyError::Signature)
    ));
    let revoked = signed::Snapshot {
        revoked_blocks: vec![*workload.block_ids().first().ok_or(VerifyError::Malformed)?],
        ..Default::default()
    };
    assert!(matches!(
        proof.verify_history(&revoked, &request, [7; 32]),
        Err(VerifyError::Revoked)
    ));
    // Removing only that revocation restores the exact same evidence.
    assert!(proof.verify_history(&trust, &request, [7; 32]).is_ok());
    Ok(())
}

#[test]
fn retained_certifications_recheck_new_blocks_current_trust_and_request_bindings() -> TestResult {
    use acyclic_auth::admission::HistoryVerifier;
    let (signer, workload) = fixture()?;
    let trust = signed::Snapshot::default();
    let attributes = RecordAttributes::try_new(vec![])?;
    let usage = signed::limits(None);
    let request = signed::request("fs:/volume/public/file", &attributes, &usage);
    let first = RuntimeAdmission::new(&signer, &trust, signed::NOW)
        .record(&workload, &request, [7; 32], &[8; 32])?;
    let next = RuntimeAdmission::new(&signer, &trust, signed::NOW)
        .record(&workload, &request, [9; 32], &[10; 32])?;
    let mut owner = HistoryVerifier::default();
    let authenticated = owner.authenticate(&first, &trust, signed::NOW)?;
    authenticated.check_request(&trust, &request, [7; 32])?;
    let different = owner.authenticate(&next, &trust, signed::NOW)?;
    different.check_request(&trust, &request, [9; 32])?;
    assert_ne!(
        authenticated.workload().block_ids(),
        different.workload().block_ids()
    );
    assert!(different.check_request(&trust, &request, [7; 32]).is_err());
    let mut changed = next.clone();
    *changed.workload.first_mut().ok_or(VerifyError::Malformed)? ^= 1;
    assert!(owner.authenticate(&changed, &trust, signed::NOW).is_err());
    changed = next.clone();
    *changed.receipt.first_mut().ok_or(VerifyError::Malformed)? ^= 1;
    assert!(owner.authenticate(&changed, &trust, signed::NOW).is_err());
    for field in 0..7 {
        let mut current = signed::Snapshot::default();
        match field {
            0 => current.environments.clear(),
            1 => current.issuers.clear(),
            2 => current.minimum_epoch = 2,
            3 => current
                .revoked_blocks
                .push(*workload.block_ids().first().ok_or(VerifyError::Malformed)?),
            4 => current.revoked_keys.push("key".into()),
            5 => current.available = false,
            _ => current
                .issuers
                .push(signed::key(4).verifying_key().to_bytes()),
        }
        assert!(owner.authenticate(&next, &current, signed::NOW).is_err());
        assert!(next.authenticate_history(&current, signed::NOW).is_err());
    }
    assert!(owner.authenticate(&next, &trust, signed::NOW - 1).is_err());
    Ok(())
}
