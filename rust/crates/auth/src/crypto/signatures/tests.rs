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
fn cached_signatures_never_cache_trust_time_or_capability_admission() {
    use crate::{testing, verify};
    let cache = SignatureCache::default();
    let bytes = testing::root(|_| {});
    let trust = testing::Snapshot::default();
    let capability = verify::verify(&bytes, &cache.with_trust(&trust), testing::NOW).unwrap();
    let id = capability.block_ids()[0];
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
    wire.blocks[0].signature[0] ^= 1;
    assert!(
        verify::verify(
            &crate::contract::CapabilityProto::from(wire).encode_to_vec(),
            &cache.with_trust(&trust),
            testing::NOW
        )
        .is_err()
    );
}
