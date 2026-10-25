use super::*;
use proptest::prelude::*;

fn counter_values() -> impl Strategy<Value = Vec<u64>> {
    proptest::collection::vec(
        prop_oneof![Just(0), Just(u64::MAX), 0_u64..128, any::<u64>()],
        8,
    )
}

fn reference_view(
    ledger: WindowLedger,
    limit: WorkCounters,
    work: WorkCounters,
) -> Result<WorkCounters, WorkError> {
    limit
        .checked_add_cumulative(&work.checked_sub_cumulative(&ledger.consumed)?)?
        .checked_sub_cumulative(&ledger.held)
}

fn reference_settle(
    ledger: WindowLedger,
    work: (WorkCounters, WorkCounters),
    pending: (WorkCounters, WorkCounters),
    stored: WorkBudget,
    limit: WorkBudget,
) -> Result<(WindowLedger, WorkBudget), WorkError> {
    let charged = work.1.checked_sub_cumulative(&work.0)?;
    Ok((
        WindowLedger {
            consumed: ledger.consumed.checked_add_cumulative(&charged)?,
            held: ledger
                .held
                .checked_add_cumulative(&pending.1)?
                .checked_sub_cumulative(&pending.0)?,
        },
        stored.checked_add_bounded(&charged, &limit)?,
    ))
}

fn compare(inputs: [WorkCounters; 8]) {
    let [
        consumed,
        held,
        before,
        after,
        pending_before,
        pending_after,
        stored,
        limit,
    ] = inputs;
    let ledger = WindowLedger { consumed, held };
    assert_eq!(
        ledger.view(&limit, &before),
        reference_view(ledger, limit, before)
    );
    assert_eq!(
        ledger.settle(
            (&before, &after),
            (&pending_before, &pending_after),
            &stored,
            &limit
        ),
        reference_settle(
            ledger,
            (before, after),
            (pending_before, pending_after),
            stored,
            limit
        ),
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn fused_window_matches_composed_arithmetic_on_every_counter(values in counter_values()) {
        // Each field reaches the arithmetic independently: an earlier overflow
        // cannot hide a later field. Include distinct peaks, which are not sums.
        macro_rules! check {
            ($($field:ident),* $(,)?) => {$({
                let mut inputs = [WorkCounters::UNCHARGED; 8];
                for ((input, value), peak) in inputs.iter_mut().zip(&values).zip(1_u64..) {
                    input.$field = *value;
                    input.peak_allocation_bytes = peak;
                }
                compare(inputs);
            })*};
        }
        crate::performance::additive_counters!(check);
    }
}

#[test]
fn window_transition_visits_each_counter_in_two_passes() {
    let ledger = WindowLedger::default();
    let limit = crate::performance::work_budget(128, 128);
    let before = WorkCounters::UNCHARGED;
    let after = crate::performance::work_budget(1, 1);
    crate::performance::CUMULATIVE_PASSES.with(|passes| passes.set(0));
    ledger.view(&limit, &before).unwrap();
    ledger
        .settle((&before, &after), (&before, &before), &limit, &limit)
        .unwrap();
    assert_eq!(
        crate::performance::CUMULATIVE_PASSES.with(std::cell::Cell::get),
        2
    );
}

#[test]
fn unchanged_window_counters_require_no_addition() {
    let ledger = WindowLedger::default();
    let before = WorkCounters::UNCHARGED;
    let limit = crate::performance::work_budget(128, 128);
    WINDOW_ADDITIONS.with(|count| count.set(0));
    let (settled, budget) = ledger
        .settle((&before, &before), (&before, &before), &limit, &limit)
        .unwrap();
    assert_eq!(settled, ledger);
    assert_eq!(budget, limit);
    assert_eq!(WINDOW_ADDITIONS.with(std::cell::Cell::get), 0);
}

#[test]
fn window_counters_without_external_work_require_no_addition() {
    for value in [0, 1, u64::MAX] {
        let consumed = crate::performance::work_budget(value, value);
        let ledger = WindowLedger {
            consumed,
            held: WorkCounters::UNCHARGED,
        };
        let limit = crate::performance::work_budget(value, value);
        WINDOW_ADDITIONS.with(|count| count.set(0));
        assert_eq!(ledger.view(&limit, &consumed).unwrap(), limit);
        assert_eq!(WINDOW_ADDITIONS.with(std::cell::Cell::get), 0);
    }
}
