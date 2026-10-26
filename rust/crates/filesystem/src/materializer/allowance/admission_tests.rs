use super::*;
use proptest::prelude::*;

fn scope(budget: WorkBudget) -> NativeMaterializationWork {
    NativeMaterializationWork::new(
        crate::performance::BOUNDARY_BUDGET,
        &crate::CancellationToken::new(),
    )
    .unwrap()
    .native_volume_work(budget)
    .unwrap()
}

fn former_check(
    scope: &NativeResourceAllowance,
    state: &NativeResourceState,
    delta: WorkCounters,
    bytes: u64,
) -> Result<(), DemandError> {
    state.check()?;
    let bytes = bytes
        .checked_add(state.allocations.reserved_bytes())
        .ok_or(WorkError::Overflow)?;
    Ok(state.work.admit(
        &state.allocation(delta, bytes)?,
        &state.pending.remaining(scope.budget)?,
    )?)
}

fn outcome(result: Result<(), DemandError>) -> Result<(), String> {
    result.map_err(|error| format!("{error:?}"))
}

proptest! {
    // Every counter independently covers exact admission, overflow, existing
    // debt, held storage and rollback when the leaf refuses the transaction.
    #[test]
    fn admitted_commit_matches_checked_completion(
        values in prop::collection::vec(
            prop_oneof![0_u64..65, Just(u64::MAX), Just(u64::MAX - 1)], 7),
        poisoned in any::<bool>(),
        refused in any::<bool>(),
    ) {
        macro_rules! check {
            ($($field:ident),* $(,)?) => {$({
                let view = scope(WorkBudget { $field: values[0], ..WorkBudget::UNBOUNDED });
                let resource = view.resource.as_ref().unwrap();
                let mut before = NativeResourceState::default();
                before.work.$field = values[1];
                before.pending.$field = values[2];
                before.poisoned = poisoned;
                before.allocations.record_owned_bytes(values[3], &mut before.work).unwrap();
                before.allocations.reserve_allocation(
                    values[4].min(u64::MAX - values[3]),
                    &WorkCounters::UNCHARGED,
                    WorkBudget::UNBOUNDED,
                ).unwrap();
                let delta = WorkCounters { $field: values[5], ..WorkCounters::UNCHARGED };
                *resource.lock() = before;
                let mut entered = 0;
                let expected = outcome(resource.transaction(None, &|scope, state| {
                    former_check(scope, state, delta, values[6])?;
                    state.completed(delta, values[6])
                }, &mut || {
                    entered += 1;
                    if refused { Err(DemandError::InvalidRequest) } else { Ok(()) }
                }));
                let after = *resource.lock();
                *resource.lock() = before;
                let mut actual_entered = 0;
                let actual = outcome(resource.admission(delta, values[6], &mut || {
                    actual_entered += 1;
                    if refused { Err(DemandError::InvalidRequest) } else { Ok(()) }
                }));
                let actual_state = *resource.lock();
                prop_assert_eq!(actual, expected);
                prop_assert_eq!(actual_entered, entered);
                prop_assert_eq!(actual_state.work, after.work);
                prop_assert_eq!(actual_state.pending, after.pending);
                prop_assert_eq!(actual_state.allocations, after.allocations);
                prop_assert_eq!(actual_state.poisoned, after.poisoned);
                // This synthetic resource state owns no physical payload.
                *resource.lock() = NativeResourceState::default();
            })*};
        }
        crate::performance::additive_counters!(check);
        check!(peak_allocation_bytes);
    }
}

#[test]
fn admitted_resource_work_does_not_recheck_completed_work() {
    let view = scope(WorkBudget::UNBOUNDED);
    COMPLETED_CHECKS.with(|count| count.set(0));
    view.charge(WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    })
    .unwrap();
    COMPLETED_CHECKS.with(|count| assert_eq!(count.get(), 0));
    view.spent(WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    })
    .unwrap();
    COMPLETED_CHECKS.with(|count| assert_eq!(count.get(), 1));
}
