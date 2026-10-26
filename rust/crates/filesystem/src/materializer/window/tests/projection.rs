use super::*;

fn passes(operation: impl FnOnce()) -> u64 {
    crate::performance::CUMULATIVE_PASSES.with(|count| count.set(0));
    operation();
    crate::performance::CUMULATIVE_PASSES.with(std::cell::Cell::get)
}

#[test]
fn consecutive_window_charges_reuse_the_settled_ledger_projection() {
    let session = root();
    let window = session.invocation_window().unwrap();
    window.charge(appends(1)).unwrap();
    assert_eq!(passes(|| window.charge(appends(1)).unwrap()), 1);
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS - 2
    );
}

#[test]
fn raw_leaf_and_other_window_access_invalidate_projection_authority() {
    let session = root();
    let first = session.invocation_window().unwrap();
    let second = session.invocation_window().unwrap();
    first.charge(appends(1)).unwrap();
    session
        .state
        .with_original_leaf(|state| state.charge(appends(1)))
        .unwrap();
    assert_eq!(passes(|| first.charge(appends(1)).unwrap()), 2);
    second.charge(appends(1)).unwrap();
    assert_eq!(passes(|| first.charge(appends(1)).unwrap()), 2);
    assert_eq!(
        first.remaining().unwrap().authority_records_appended,
        APPENDS - 3
    );
    assert_eq!(
        second.remaining().unwrap().authority_records_appended,
        APPENDS - 1
    );
}

#[test]
fn reconciled_budget_adjustments_are_derived_from_the_window_ledger() {
    let session = root();
    let window = session.invocation_window().unwrap();
    window.state(|state| {
        // View-gate completion raises shared work and the temporary budget
        // independently of roots.pending. It is still this window's work.
        state.work.authority_records_appended += 3;
        state.budget.authority_records_appended += 3;
    });
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS - 3
    );
    let held = window.reserve_work(appends(4)).unwrap();
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS - 7
    );
    drop(held);
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS - 3
    );
}

#[test]
fn exhausted_projection_epoch_disables_reuse() {
    let session = root();
    let window = session.invocation_window().unwrap();
    session
        .state
        .with_original_leaf(|state| state.window_epoch = Some(u64::MAX));
    window.charge(appends(1)).unwrap();
    assert_eq!(passes(|| window.charge(appends(1)).unwrap()), 2);
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS - 2
    );
}
