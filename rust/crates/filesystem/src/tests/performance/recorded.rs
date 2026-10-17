//! Completed sparse charges against an independent scalar accounting model.
use super::*;
use crate::kernel::allocation::{AllocationLedger, KernelAccounting};
use proptest::prelude::*;

const NAMES: [&str; 24] = [
    "authority_records_read",
    "authority_records_appended",
    "authority_bytes_read",
    "authority_bytes_written",
    "object_probes",
    "backend_read_operations",
    "backend_write_operations",
    "durability_operations",
    "page_reads",
    "page_writes",
    "object_bytes_read",
    "object_bytes_written",
    "bytes_hashed",
    "bytes_copied",
    "bytes_encoded",
    "source_bytes_read",
    "source_path_components",
    "source_entries_visited",
    "output_bytes",
    "items_examined",
    "items_returned",
    "allocation_operations",
    "peak_allocation_bytes",
    "materializations",
];
#[derive(Debug, PartialEq)]
enum Failure {
    Work(WorkError),
    Cancelled,
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    Duplicate,
    Allocation,
}
type Trace = Vec<(
    Result<(), Failure>,
    WorkCounters,
    acyclic_test_support::AllocationInfo,
)>;

fn frozen_record(
    prior: &mut WorkCounters,
    mut delta: WorkCounters,
    mut budget: WorkBudget,
    cancelled: bool,
) -> Result<(), Failure> {
    let current = counter_fields(prior).map(|v| *v);
    let increment = counter_fields(&mut delta).map(|v| *v);
    let maximum = counter_fields(&mut budget).map(|v| *v);
    let mut sum = [0; 24];
    let mut overflow = false;
    for (i, value) in sum.iter_mut().enumerate() {
        if i == 22 {
            *value = current[i].max(increment[i]);
        } else {
            let added = current[i].overflowing_add(increment[i]);
            *value = added.0;
            overflow |= added.1;
        }
    }
    if overflow {
        return Err(Failure::Work(WorkError::Overflow));
    }
    for (field, value) in counter_fields(prior).into_iter().zip(sum) {
        *field = value;
    }
    for i in 0..24 {
        if sum[i] > maximum[i] {
            return Err(Failure::Work(WorkError::BudgetExceeded {
                counter: NAMES[i],
                observed: sum[i],
                maximum: maximum[i],
            }));
        }
    }
    if cancelled {
        Err(Failure::Cancelled)
    } else {
        Ok(())
    }
}
fn compare(steps: &[(usize, u64)], budget: WorkBudget, cancel_at: usize, fault: Fault) {
    // Neither accounting state nor the result/error calculation is shared.
    let mut actual = WorkCounters::UNCHARGED;
    let mut expected = WorkCounters::UNCHARGED;
    let mut allocations = AllocationLedger::default();
    let cancel = crate::CancellationToken::new();
    let mut actual_trace: Trace = Vec::new();
    let mut expected_trace: Trace = Vec::new();
    for (index, &(field, amount)) in steps.iter().enumerate() {
        let mut delta = WorkCounters::UNCHARGED;
        *counter_fields(&mut delta)[field] = amount;
        let outcome = frozen_record(&mut expected, delta, budget, index >= cancel_at);
        expected_trace.push((outcome, expected, Default::default()));
        if index == cancel_at {
            cancel.cancel();
        }
        let mut accounting =
            KernelAccounting::borrowed_standalone(&mut allocations, &mut actual, budget, &cancel);
        let mut outcome = Ok(());
        let measured = acyclic_test_support::measure(|| {
            outcome = accounting
                .record_completed(delta)
                .map_err(|error| match error {
                    crate::FsError::Work(error) => Failure::Work(error),
                    crate::FsError::Cancelled(_) => Failure::Cancelled,
                    error => panic!("unexpected accounting error: {error:?}"),
                });
            if matches!(fault, Fault::Duplicate) {
                let _ = accounting.record_completed(delta);
            }
            if matches!(fault, Fault::Allocation) {
                std::hint::black_box(Vec::<u8>::with_capacity(64));
            }
        });
        actual_trace.push((outcome, actual, measured));
    }
    assert_eq!(
        actual_trace, expected_trace,
        "completed accounting diverged from independent work/allocation trace"
    );
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn completed_sparse_charges_preserve_exact_errors_receipts_and_zero_allocations(
        steps in prop::collection::vec((0usize..24, prop_oneof![0u64..100, Just(u64::MAX)]), 1..16),
        units in prop_oneof![0u64..200, Just(u64::MAX)], bytes in prop_oneof![0u64..200, Just(u64::MAX)],
        cancel_at in 0usize..18,
    ) {
        compare(&steps, crate::performance::work_budget(units, bytes), cancel_at, Fault::None);
    }
}
#[test]
#[should_panic(expected = "completed accounting diverged")]
fn duplicate_production_charge_fails_the_same_exact_trace_check() {
    compare(
        &[(19, 1)],
        WorkBudget::UNBOUNDED,
        usize::MAX,
        Fault::Duplicate,
    );
}
#[test]
#[should_panic(expected = "completed accounting diverged")]
fn uncharged_production_allocation_fails_the_same_exact_trace_check() {
    compare(
        &[(19, 1)],
        WorkBudget::UNBOUNDED,
        usize::MAX,
        Fault::Allocation,
    );
}
