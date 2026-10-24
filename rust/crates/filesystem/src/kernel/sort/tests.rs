use super::*;
use crate::{CancellationToken, WorkBudget, facade::FsWork as _};

#[test]
fn denied_sort_never_compares_or_moves_a_value() {
    let cancellation = CancellationToken::new();
    let budget = crate::performance::work_budget(0, 0);
    let mut work = budget.accounting(&cancellation);
    let mut values = [3, 1, 2];
    let mut comparisons = 0;
    assert!(
        sort_by_accounted(&mut values, &mut work, |a, b| {
            comparisons += 1;
            a.cmp(b)
        })
        .is_err()
    );
    assert_eq!(comparisons, 0);
    assert_eq!(values, [3, 1, 2]);
    assert_eq!(work.spent(), WorkCounters::UNCHARGED);
}

#[test]
fn already_ordered_frontier_is_linear_and_allocates_nothing() {
    let cancellation = CancellationToken::new();
    let mut values: Vec<u32> = (0..100_000).collect();
    let budget = crate::performance::work_budget(99_999, 0);
    let mut work = budget.accounting(&cancellation);
    let physical = acyclic_test_support::measure(|| {
        sort_by_accounted(&mut values, &mut work, Ord::cmp).unwrap();
    });
    assert_eq!(physical.bytes_total, 0);
    assert_eq!(work.spent().items_examined, 99_999);
    assert_eq!(work.spent().bytes_copied, 0);
}

#[test]
fn cancellation_precedes_even_an_empty_sort() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut work = WorkBudget::UNBOUNDED.accounting(&cancellation);
    assert!(sort_by_accounted::<u8>(&mut [], &mut work, Ord::cmp).is_err());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(96))]
    #[test]
    fn admitted_comparisons_and_moves_match_the_physical_work(
        mut values in proptest::collection::vec(proptest::num::u16::ANY, 0..512),
    ) {
        let mut expected = values.clone();
        expected.sort_unstable();
        let cancellation = CancellationToken::new();
        let mut work = WorkBudget::UNBOUNDED.accounting(&cancellation);
        let mut comparisons = 0_u64;
        let physical = acyclic_test_support::measure(|| {
            sort_by_accounted(&mut values, &mut work, |a, b| {
                comparisons += 1;
                a.cmp(b)
            }).unwrap();
        });
        proptest::prop_assert_eq!(&values, &expected);
        proptest::prop_assert_eq!(physical.bytes_total, 0);
        proptest::prop_assert_eq!(work.spent().items_examined, comparisons);
        proptest::prop_assert_eq!(work.spent().bytes_copied % (2 * std::mem::size_of::<u16>() as u64), 0);
        proptest::prop_assert_eq!(work.spent().backend_write_operations, 0);

        #[cfg(not(target_arch = "wasm32"))]
        {
            let native = crate::NativeMaterializationWork::new(
                crate::performance::BOUNDARY_BUDGET, &cancellation,
            ).unwrap_or_else(|(error, _)| panic!("{error:?}"));
            let mut work = KernelAccounting::native(&native, &cancellation);
            expected.reverse();
            let mut comparisons = 0_u64;
            sort_by_accounted(&mut expected, &mut work, |a, b| {
                comparisons += 1;
                a.cmp(b)
            }).unwrap();
            proptest::prop_assert!(expected.windows(2).all(|pair| matches!(pair, [a, b] if a <= b)));
            proptest::prop_assert_eq!(native.receipt().items_examined, comparisons);
            proptest::prop_assert_eq!(native.receipt().backend_write_operations, 0);
        }
    }
}
