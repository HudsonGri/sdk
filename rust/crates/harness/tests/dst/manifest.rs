//! Expected live failures retain their exact production variant and cause.
use super::*;
#[path = "expected_fail.rs"]
mod entries;

fn expected(
    id: &str,
    owner_pr: u64,
    matches: fn(&Error) -> bool,
    result: Result<()>,
) -> Result<()> {
    assert_ne!(owner_pr, 0, "expected failure has no owning PR");
    match result {
        Err(error) if matches(&error) => Ok(()),
        Err(error) => Err(error),
        Ok(()) => Err(Error::Invalid(format!(
            "{id} now passes; PR #{owner_pr} must remove its expected-fail line"
        ))),
    }
}

pub(super) fn check(id: &str, result: Result<()>) -> Result<()> {
    let mut names = std::collections::BTreeSet::new();
    for (name, owner, _) in entries::EXPECTED_FAILS {
        assert!(matches!(*name, "F3"), "unexercised expected failure {name}");
        assert!(names.insert(name), "duplicate expected failure {name}");
        assert_ne!(*owner, 0, "expected failure has no owning PR");
    }
    match entries::EXPECTED_FAILS.iter().find(|entry| entry.0 == id) {
        Some((_, owner, matches)) => expected(id, *owner, *matches, result),
        None => result,
    }
}

#[test]
fn a_fixed_scenario_requires_its_manifest_line_to_be_removed() {
    let error = expected(
        "F3",
        904,
        |error| matches!(error, Error::Conflict(cause) if cause == "expected cause"),
        Ok(()),
    )
    .unwrap_err();
    assert!(matches!(error, Error::Invalid(message) if message.contains("must remove")));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn only_the_exact_typed_cause_is_expected(cause in ".{0,64}") {
        const CAUSE: &str = "local agent already has an admitted turn";
        let matches: fn(&Error) -> bool = |error| matches!(error, Error::Conflict(cause) if cause == CAUSE);
        let result = expected("F3", 904, matches, Err(Error::Conflict(cause.clone())));
        proptest::prop_assert_eq!(result.is_ok(), cause == CAUSE);
        if let Err(error) = result {
            proptest::prop_assert!(matches!(error, Error::Conflict(retained) if retained == cause));
        }
        // Erasing the production variant must never turn an unrelated storage
        // failure into an expected live finding, even with the same message.
        let result = expected("F3", 904, matches, Err(Error::Storage(cause.clone())));
        proptest::prop_assert!(matches!(result, Err(Error::Storage(retained)) if retained == cause));
    }
}
