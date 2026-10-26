//! Raw verification must admit its input before allocating decoded authority.
use super::*;
use acyclic_auth::credential::MAX_CREDENTIAL_BYTES;

fn with_audience_padding(padding: usize) -> Vec<u8> {
    root(|block| {
        block.caveats.push(Caveat {
            kind: CaveatKind::Audience(ResourcePattern {
                name: format!("workers:/{}", "a".repeat(padding)),
                prefix: false,
            }),
        });
    })
}

fn signed_at_size(size: usize) -> Vec<u8> {
    // Both payload lengths have the same protobuf varint width at this bound.
    let baseline = with_audience_padding(1024);
    let bytes = with_audience_padding(1024 + size - baseline.len());
    assert_eq!(bytes.len(), size);
    bytes
}

#[test]
fn direct_verification_accepts_signed_inputs_through_the_shared_byte_ceiling() {
    let trust = Snapshot::default();
    for size in [MAX_CREDENTIAL_BYTES - 1, MAX_CREDENTIAL_BYTES] {
        assert!(verify(&signed_at_size(size), &trust, NOW).is_ok());
    }
}

#[test]
fn direct_verification_refuses_oversized_signed_input_without_decode_allocations() {
    let trust = Snapshot::default();
    let bytes = signed_at_size(MAX_CREDENTIAL_BYTES + 1);
    let measured = acyclic_test_support::measure(|| {
        assert!(matches!(
            verify(&bytes, &trust, NOW),
            Err(VerifyError::Malformed)
        ));
    });
    assert_eq!(measured.count_total, 0);
    assert_eq!(measured.bytes_total, 0);
}
