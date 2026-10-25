use super::*;
use ed25519_dalek::{Signer, SigningKey};

fn strict_calls(run: impl FnOnce()) -> usize {
    let ((), calls): (_, Vec<SignatureVerification>) =
        acyclic_test_support::differential::observe(|| (), |()| run());
    calls.len()
}

#[test]
fn signature_evidence_binds_every_input_and_evicts_with_fixed_storage() {
    let cache = SignatureCache::default();
    let key = SigningKey::from_bytes(&[7; 32]);
    let public = key.verifying_key();
    let message = b"original";
    let signature = key.sign(message).to_bytes();
    assert_eq!(
        strict_calls(|| {
            cache.verify(&public, message, &signature).unwrap();
            cache.verify(&public, message, &signature).unwrap();
        }),
        1
    );
    assert!(cache.verify(&public, b"tampered", &signature).is_err());
    assert!(
        cache
            .verify(
                &SigningKey::from_bytes(&[8; 32]).verifying_key(),
                message,
                &signature
            )
            .is_err()
    );
    let mut changed = signature;
    changed[0] ^= 1;
    assert!(cache.verify(&public, message, &changed).is_err());
    assert!(cache.verify(&public, message, &signature[..63]).is_err());
    // Failed checks cannot poison or evict successful evidence.
    assert_eq!(
        strict_calls(|| cache.verify(&public, message, &signature).unwrap()),
        0
    );
    for index in 0..32_u8 {
        let message = [index];
        cache
            .verify(&public, &message, &key.sign(&message).to_bytes())
            .unwrap();
    }
    assert_eq!(
        strict_calls(|| cache.verify(&public, message, &signature).unwrap()),
        1
    );
}

#[test]
fn active_certificate_evidence_survives_streaming_record_churn() {
    let cache = SignatureCache::default();
    let key = SigningKey::from_bytes(&[19; 32]);
    let public = key.verifying_key();
    let certificate = b"runtime certificate";
    let signature = key.sign(certificate).to_bytes();
    cache.verify(&public, certificate, &signature).unwrap();
    for index in 0..96_u8 {
        assert_eq!(
            strict_calls(|| cache.verify(&public, certificate, &signature).unwrap()),
            0
        );
        let record = [index];
        cache
            .verify(&public, &record, &key.sign(&record).to_bytes())
            .unwrap();
    }
    // Capacity remains 32. Evidence not read during churn must be evicted.
    let record = [0];
    assert_eq!(
        strict_calls(|| cache
            .verify(&public, &record, &key.sign(&record).to_bytes())
            .unwrap()),
        1
    );
}

#[test]
fn cached_signatures_never_cache_trust_time_or_capability_admission() {
    use crate::{testing, verify};
    let cache = SignatureCache::default();
    let bytes = testing::root(|_| {});
    let trust = testing::Snapshot::default();
    let capability = verify::verify(&bytes, &cache.with_trust(&trust), testing::NOW).unwrap();
    let id = *capability.block_ids().first().unwrap();
    assert_eq!(
        strict_calls(|| {
            verify::verify(&bytes, &cache.with_trust(&trust), testing::NOW).unwrap();
        }),
        0
    );
    for change in 0..6 {
        let mut trust = testing::Snapshot::default();
        match change {
            0 => trust.available = false,
            1 => trust.environments.clear(),
            2 => trust.issuers.clear(),
            3 => trust.minimum_epoch = 2,
            4 => trust.revoked_keys.push("key".into()),
            _ => trust.revoked_blocks.push(id),
        }
        assert!(
            verify::verify(&bytes, &cache.with_trust(&trust), testing::NOW).is_err(),
            "change {change}"
        );
    }
    assert!(verify::verify(&bytes, &cache.with_trust(&trust), 0).is_err());
    assert!(verify::verify(&bytes, &cache.with_trust(&trust), 1000).is_err());
    let mut wire = crate::authority::decode_capability(&bytes).unwrap();
    *wire
        .blocks
        .first_mut()
        .unwrap()
        .signature
        .first_mut()
        .unwrap() ^= 1;
    assert!(
        verify::verify(
            &crate::contract::CapabilityProto::from(wire).encode_to_vec(),
            &cache.with_trust(&trust),
            testing::NOW
        )
        .is_err()
    );
}

#[test]
fn verified_public_keys_reuse_the_same_evidence_slots_and_reject_changed_bytes() {
    let cache = SignatureCache::default();
    let bytes = crate::testing::root(|_| {});
    let trust = crate::testing::Snapshot::default();
    crate::verify::verify(&bytes, &cache.with_trust(&trust), crate::testing::NOW).unwrap();
    let ((), keys): (_, Vec<PublicKeyPreparation>) = acyclic_test_support::differential::observe(
        || (),
        |()| {
            crate::verify::verify(&bytes, &cache.with_trust(&trust), crate::testing::NOW).unwrap();
        },
    );
    assert!(
        keys.is_empty(),
        "authenticated public keys were decoded again: {}",
        keys.len()
    );
    assert!(cache.public_key(&[0; 31]).is_err());
    assert!(cache.public_key(&[0; 32]).is_err());
    let mut identity = [0; 32];
    *identity.first_mut().unwrap() = 1;
    assert!(cache.public_key(&identity).is_err());
    let unrelated = SigningKey::from_bytes(&[29; 32]).verifying_key();
    let (key, keys): (_, Vec<PublicKeyPreparation>) = acyclic_test_support::differential::observe(
        || (),
        |()| cache.public_key(unrelated.as_bytes()).unwrap(),
    );
    assert_eq!(key, unrelated);
    assert_eq!(
        keys.len(),
        1,
        "unverified keys must use the canonical decoder"
    );
}
