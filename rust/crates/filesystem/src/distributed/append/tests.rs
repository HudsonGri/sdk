use super::*;
use proptest::prelude::*;

fn request(kind: u8, fork: bool, value: u64, body: &[u8]) -> CommitRequest {
    let path = StreamPath::new("authority/records").expect("fixed path");
    let other = StreamPath::new("authority/source").expect("fixed path");
    let condition_path = if fork { other.clone() } else { path.clone() };
    let mut conditions = vec![match kind {
        0 => CommitCondition::Absent {
            path: condition_path,
        },
        1 => CommitCondition::Tail {
            path: condition_path,
            expected: value,
        },
        _ => CommitCondition::Head {
            path: condition_path,
            expected: value.max(1),
            commit_id: acyclic_stream::CommitId::from_bytes([body[0]; 32]),
        },
    }];
    if fork {
        conditions.push(CommitCondition::Absent { path: path.clone() });
    }
    CommitRequest {
        conditions,
        mutations: vec![if fork {
            CommitMutation::Fork {
                source: other,
                destination: path,
                at_tail: value,
                records: vec![body.to_vec().into()],
            }
        } else {
            CommitMutation::Append {
                path,
                records: vec![body.to_vec().into()],
            }
        }],
        idempotency_key: acyclic_stream::IdempotencyKey::new(b"same-logical-operation".to_vec())
            .expect("fixed key"),
    }
}

fn check_pair(
    left: CommitRequest,
    right: CommitRequest,
) -> Result<(), proptest::test_runner::TestCaseError> {
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    check_bound_pair(left.clone(), right.clone(), &mut work)?;
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::initialize_test_native_io();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        let mut work = AuthorityWork::native(&original, &cancellation);
        check_bound_pair(left, right, &mut work)?;
    }
    Ok(())
}

fn check_bound_pair(
    mut left: CommitRequest,
    mut right: CommitRequest,
    work: &mut AuthorityWork<'_>,
) -> Result<(), proptest::test_runner::TestCaseError> {
    let equal = left == right;
    bind_attempt_key(&mut left, work).expect("left binding");
    bind_attempt_key(&mut right, work).expect("right binding");
    prop_assert_eq!(left.idempotency_key == right.idempotency_key, equal);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn attempt_identity_covers_every_condition_and_mutation(
        left in (any::<u64>(), proptest::collection::vec(any::<u8>(), 1..9)),
        right in (any::<u64>(), proptest::collection::vec(any::<u8>(), 1..9)),
    ) {
        for kind in 0..3 {
            for fork in [false, true] {
                let original = request(kind, fork, left.0, &left.1);
                check_pair(original.clone(), original)?;
                check_pair(request(kind, fork, left.0, &left.1), request(kind, fork, right.0, &right.1))?;
            }
        }
    }
}

#[test]
fn omitting_request_binding_fails_the_same_identity_property() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            OMIT_REQUEST_BINDING.set(false);
        }
    }
    let _reset = Reset;
    OMIT_REQUEST_BINDING.set(true);
    for kind in 0..3 {
        for fork in [false, true] {
            assert!(
                check_pair(
                    request(kind, fork, 1, b"first"),
                    request(kind, fork, 2, b"second")
                )
                .is_err()
            );
        }
    }
}
