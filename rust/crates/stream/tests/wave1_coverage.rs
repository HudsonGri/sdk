//! Coverage refusals operate on the real exported suite, without provider stubs.
use acyclic_stream::conformance::wave1::{
    Case,
    coverage::{self, HistoryId, UnqualifiedReason},
};

#[test]
fn stable_ids_and_partial_cases_match_the_adopted_cloud_contract() {
    let expected = [
        ("HST-01", &[Case::SubtreeFork, Case::ComposedFork][..]),
        ("HST-02", &[Case::AppendRetry, Case::CommitRetry][..]),
        ("HST-03", &[Case::AppendRetry, Case::CommitRetry][..]),
        ("HST-04", &[Case::AppendRetry, Case::CommitRetry][..]),
        ("HST-05", &[Case::ComposedFork][..]),
        ("HST-06", &[][..]),
        ("HST-07", &[][..]),
        ("HST-08", &[][..]),
        ("HST-09", &[][..]),
        ("HST-10", &[][..]),
        ("HST-11", &[][..]),
        ("HST-12", &[][..]),
    ];
    for (history, (id, partial)) in HistoryId::ALL.into_iter().zip(expected) {
        assert_eq!((history.id(), history.partial_cases()), (id, partial));
    }
}

#[test]
fn ordinary_case_availability_does_not_qualify_complete_histories() {
    let missing = coverage::hst_coverage();
    assert_eq!(missing.len(), 12);
    for (history, row) in HistoryId::ALL.into_iter().zip(missing) {
        assert_eq!(row.history, history);
        assert_eq!(row.reason, UnqualifiedReason::CanonicalHistoryNotExported);
        assert_eq!(row.reason.name(), "CanonicalHistoryNotExported");
        for case in row.history.partial_cases() {
            assert!(Case::ALL.contains(case));
        }
    }
}

#[test]
fn qualification_request_refuses_all_unexported_histories() {
    let result = coverage::require_hst_qualification();
    assert!(
        result.is_err(),
        "complete HST qualification must refuse unexported histories"
    );
    if let Err(failure) = result {
        assert_eq!(failure.missing, coverage::hst_coverage());
        let message = failure.to_string();
        assert!(message.contains("step=require_hst_qualification"));
        assert!(message.contains("recovery:"));
    }
}
