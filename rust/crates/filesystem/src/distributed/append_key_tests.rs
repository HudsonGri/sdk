use super::*;
use proptest::prelude::*;

proptest! {
    // Retry identity binds every condition and every record, and is invariant
    // under canonical condition ordering and the caller's previous retry key.
    #[test]
    fn proposal_identity_and_hash_work_cover_the_complete_request(
        body in proptest::collection::vec(any::<u8>(), 0..64), tail in 0_u64..u64::MAX,
        extra in proptest::collection::vec(any::<u8>(), 0..64), reverse in any::<bool>(),
    ) {
        let path = StreamPath::new("authority/records").unwrap();
        let gate = StreamPath::new("authority/gate").unwrap();
        let placeholder = || acyclic_stream::IdempotencyKey::new(
            acyclic_stream::OwnedBytes::from_static(b"placeholder")
        ).unwrap();
        let mut request = CommitRequest {
            conditions: vec![CommitCondition::Tail { path: gate.clone(), expected: tail }],
            mutations: vec![CommitMutation::Append { path: path.clone(), records: vec![body.clone().into(), extra.clone().into()] }],
            idempotency_key: placeholder(),
        };
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let key = super::append::proposal_key(&mut request, &mut work).unwrap();
        let expected_hash_bytes = (b"acyclic.stream.request.v1\0".len() + b"commit".len()
            + 1 + 8 + gate.as_str().len() + 8
            + 1 + 8 + path.as_str().len() + 8 + 8 + body.len() + 8 + extra.len()) as u64;
        prop_assert_eq!(work.spent().bytes_hashed, expected_hash_bytes);
        let check = |mut candidate: CommitRequest| {
            let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            super::append::proposal_key(&mut candidate, &mut work).unwrap()
        };
        request.idempotency_key = key.clone();
        if reverse { request.conditions.reverse(); }
        prop_assert_eq!(&key, &check(request.clone()));
        let mut changed = request.clone();
        let CommitCondition::Tail { expected, .. } = &mut changed.conditions[0] else { unreachable!() };
        *expected += 1;
        prop_assert_ne!(&key, &check(changed));
        for index in 0..2 {
            let mut changed = request.clone();
            let CommitMutation::Append { records, .. } = &mut changed.mutations[0] else { unreachable!() };
            let mut bytes = records[index].to_vec();
            bytes.push(0);
            records[index] = bytes.into();
            prop_assert_ne!(&key, &check(changed));
        }
        let mut changed = request;
        changed.conditions.push(CommitCondition::Absent { path: StreamPath::new("authority/retirement").unwrap() });
        let expected = check(changed.clone());
        changed.conditions.reverse();
        prop_assert_eq!(&expected, &check(changed));
        prop_assert_ne!(&key, &expected);
    }
}
