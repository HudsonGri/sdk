use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // A provider attempt is stable only at the same semantic record and fence.
    // Logical operation identity alone must never alias a rebased attempt.
    #[test]
    fn append_attempt_keys_bind_the_record_and_every_fence(
        authority in any::<[u8;16]>(), operation in any::<[u8;16]>(), record in any::<[u8;32]>(),
        epoch in any::<u64>(), gate in any::<u64>(), lease in "[a-z0-9]{1,64}",
    ) {
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        let authority = AuthorityId::from_bytes(authority);
        let operation = OperationId::from_bytes(operation);
        let digest = Digest::from_bytes(record);
        let path = StreamPath::new(format!("leases/{lease}")).unwrap();
        let key = append_key(authority, operation, digest, epoch, gate, Some(&path), &mut work).unwrap();
        prop_assert_eq!(&key, &append_key(authority, operation, digest, epoch, gate, Some(&path), &mut work).unwrap());
        let mut changed_authority = authority.into_bytes();
        changed_authority[0] ^= 1;
        prop_assert_ne!(&key, &append_key(AuthorityId::from_bytes(changed_authority), operation, digest, epoch, gate, Some(&path), &mut work).unwrap());
        let mut changed_operation = operation.into_bytes();
        changed_operation[0] ^= 1;
        prop_assert_ne!(&key, &append_key(authority, OperationId::from_bytes(changed_operation), digest, epoch, gate, Some(&path), &mut work).unwrap());
        let mut changed = record;
        changed[0] ^= 1;
        let other = StreamPath::new(format!("leases/{lease}-other")).unwrap();
        for (record, epoch, gate, lease) in [
            (Digest::from_bytes(changed), epoch, gate, Some(&path)),
            (digest, epoch ^ 1, gate, Some(&path)),
            (digest, epoch, gate ^ 1, Some(&path)),
            (digest, epoch, gate, Some(&other)),
            (digest, epoch, gate, None),
        ] {
            prop_assert_ne!(&key, &append_key(authority, operation, record, epoch, gate, lease, &mut work).unwrap());
        }
    }
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn the_longest_lease_attempt_keeps_its_original_allocation_custody() {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)
        .unwrap();
    let mut native = AuthorityWork::native(&original, &cancellation);
    let lease = native
        .lease_path(
            crate::WorkspaceId::from_bytes([255; 16]),
            crate::OperationLeaseId::from_bytes([255; 16]),
            u64::MAX,
        )
        .unwrap();
    let mut ordinary = AuthorityWork::new(budget, &cancellation);
    let authority = AuthorityId::from_bytes([255; 16]);
    let operation = OperationId::from_bytes([255; 16]);
    let record = Digest::from_bytes([255; 32]);
    let expected = append_key(
        authority,
        operation,
        record,
        u64::MAX,
        u64::MAX,
        Some(&lease),
        &mut ordinary,
    )
    .unwrap();
    let before = original.receipt().bytes_copied;
    let actual = append_key(
        authority,
        operation,
        record,
        u64::MAX,
        u64::MAX,
        Some(&lease),
        &mut native,
    )
    .unwrap();
    assert_eq!(actual, expected);
    original.receipt().verify(budget).unwrap();
    assert!(original.receipt().bytes_copied - before >= actual.as_bytes().len() as u64);
}
