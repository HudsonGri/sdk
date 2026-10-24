use super::*;
use proptest::prelude::*;

pub(super) fn amount(small: u64) -> impl Strategy<Value = u64> {
    prop_oneof![7 => 0..small, 1 => any::<u64>()]
}

pub(super) fn counters(values: [u64; 24]) -> WorkCounters {
    let mut fields = serde_json::to_value(WorkCounters::UNCHARGED).unwrap();
    let fields_map = fields.as_object_mut().unwrap();
    assert_eq!(fields_map.len(), values.len());
    for (field, value) in fields_map.values_mut().zip(values) {
        *field = serde_json::json!(value);
    }
    serde_json::from_value(fields).unwrap()
}

pub(super) fn status(result: Result<(), crate::FsError>) -> Result<(), String> {
    result.map_err(|error| format!("{error:?}"))
}

#[test]
fn nested_receipts_share_the_original_budget_and_record_failed_work() {
    let cancellation = crate::CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.backend_read_operations = 3;
    let mut accounting = KernelAccounting::standalone(budget, &cancellation);
    accounting
        .charge(WorkCounters {
            backend_read_operations: 2,
            ..WorkCounters::UNCHARGED
        })
        .unwrap();
    let spent = WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::UNCHARGED
    };
    let failed = crate::OperationFailure::new(
        crate::FsError::Object(crate::ObjectStoreError::Corrupt),
        spent,
    );
    let result: Result<(), crate::FsError> = accounting.accept_receipt(Err(failed));
    assert!(matches!(
        result,
        Err(crate::FsError::Object(crate::ObjectStoreError::Corrupt))
    ));
    assert_eq!(accounting.spent().backend_read_operations, 3);
    assert_eq!(accounting.remaining().unwrap().backend_read_operations, 0);
    // Negative control: resetting a nested operation to the original budget
    // admits another read. The shared bridge refuses that same receipt.
    spent.verify(budget).unwrap();
    let result: Result<(), crate::FsError> =
        accounting.accept_receipt(Ok::<_, crate::OperationFailure<crate::FsError>>(
            crate::OperationReceipt {
                value: (),
                work: spent,
            },
        ));
    assert!(result.is_err());
    assert_eq!(accounting.spent().backend_read_operations, 4);
}

#[test]
fn nested_receipts_include_live_parent_ownership_in_the_simultaneous_peak() {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut held = accounting.claim_owned(32, 1).unwrap();
    let nested = WorkCounters {
        peak_allocation_bytes: 64,
        ..WorkCounters::UNCHARGED
    };
    let result: Result<(), crate::FsError> =
        accounting.accept_receipt(Ok::<_, crate::OperationFailure<crate::FsError>>(
            crate::OperationReceipt {
                value: (),
                work: nested,
            },
        ));
    result.unwrap();
    assert_eq!(accounting.spent().peak_allocation_bytes, 96);
    // The former checked_add bridge merged independent peaks with max and
    // lost the caller's still-live backing during nested IO.
    assert_eq!(
        WorkCounters {
            peak_allocation_bytes: 32,
            ..WorkCounters::UNCHARGED
        }
        .checked_add(nested)
        .unwrap()
        .peak_allocation_bytes,
        64
    );
    accounting.release_owned(&mut held, 32).unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // For arbitrary mixed work sequences, both standalone contexts equal a
    // checked sum followed by budget verification. Cancellation has priority;
    // admission never changes state and a rejected charge changes no field.
    #[test]
    fn standalone_and_borrowed_accounting_match_verified_work_sequences(
        mut prior in prop::array::uniform24(amount(16)),
        limits in prop::array::uniform24(amount(128)),
        deltas in prop::collection::vec(prop::array::uniform24(amount(8)), 0..16),
        budget_mode in 0..3_u8,
        spike_field in 0..24_usize,
        spike in any::<bool>(),
        cancel_at in prop::option::of(0..16_usize),
    ) {
        if spike { prior[spike_field] = u64::MAX; }
        let mut expected = counters(prior);
        let budget = match budget_mode {
            0 => WorkBudget::UNBOUNDED,
            1 => counters(limits),
            _ => crate::performance::work_budget(64,512),
        };
        let cancellation = crate::CancellationToken::default();
        let mut ledger = AllocationLedger::default();
        let mut borrowed_work = expected;
        let mut borrowed = KernelAccounting::borrowed_standalone(
            &mut ledger, &mut borrowed_work, budget, &cancellation,
        );
        let mut standalone = KernelAccounting::Standalone {
            allocations: AllocationLedger::default(), work: expected,
            budget, cancellation: &cancellation,
        };
        for (step, delta) in deltas.into_iter().map(counters).enumerate() {
            if cancel_at == Some(step) { cancellation.cancel(); }
            let next = cancellation.check().map_err(crate::FsError::from)
                .and_then(|()| expected.checked_add(delta)
                    .and_then(|sum| sum.verify(budget).map(|()| sum))
                    .map_err(crate::FsError::from));
            let expected_status = next.as_ref().map(|_| ()).map_err(|error| format!("{error:?}"));
            let updated = next.unwrap_or(expected);
            for accounting in [&mut standalone, &mut borrowed] {
                prop_assert_eq!(status(accounting.admit(delta)), expected_status.clone());
                prop_assert_eq!(accounting.spent(), expected);
                prop_assert_eq!(status(accounting.charge(delta)), expected_status.clone());
                prop_assert_eq!(accounting.spent(), updated);
            }
            expected = updated;
        }
        prop_assert_eq!(ledger.live_bytes(), 0);
    }
}
