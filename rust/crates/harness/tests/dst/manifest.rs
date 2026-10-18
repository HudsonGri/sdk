//! Expected live failures retain their exact production variant and cause.
use super::*;
use serde_json::{Value, json};

fn expected(id: &str, entry: &Value, result: Result<()>) -> Result<()> {
    let fields = entry
        .as_object()
        .ok_or_else(|| Error::Invalid("expected-fail entry must be an object".into()))?;
    if fields.len() != 2
        || fields
            .get("owner_pr")
            .and_then(Value::as_u64)
            .is_none_or(|pr| pr == 0)
        || fields.get("Conflict").and_then(Value::as_str).is_none()
    {
        return Err(Error::Invalid("invalid expected-fail entry".into()));
    }
    match result {
        Err(Error::Conflict(cause)) if Some(cause.as_str()) == entry["Conflict"].as_str() => Ok(()),
        Err(error) => Err(error),
        Ok(()) => Err(Error::Invalid(format!(
            "{id} now passes; PR #{} must remove its expected-fail line",
            entry["owner_pr"]
        ))),
    }
}

pub(super) fn check(id: &str, result: Result<()>) -> Result<()> {
    let entries: BTreeMap<String, Value> =
        serde_json::from_str(include_str!("expected-fail.json")).map_err(storage)?;
    for name in entries.keys() {
        assert!(
            matches!(name.as_str(), "F3"),
            "unexercised expected failure {name}"
        );
    }
    match entries.get(id) {
        Some(entry) => expected(id, entry, result),
        None => result,
    }
}

#[test]
fn a_fixed_scenario_requires_its_manifest_line_to_be_removed() {
    let entry = json!({"owner_pr": 904, "Conflict": "expected cause"});
    let error = expected("F3", &entry, Ok(())).unwrap_err();
    assert!(matches!(error, Error::Invalid(message) if message.contains("must remove")));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn only_the_exact_typed_cause_is_expected(cause in ".{0,64}") {
        const CAUSE: &str = "local agent already has an admitted turn";
        let entry = json!({"owner_pr": 904, "Conflict": CAUSE});
        let result = expected("F3", &entry, Err(Error::Conflict(cause.clone())));
        proptest::prop_assert_eq!(result.is_ok(), cause == CAUSE);
        if let Err(error) = result {
            proptest::prop_assert!(matches!(error, Error::Conflict(retained) if retained == cause));
        }
        // Erasing the production variant must never turn an unrelated storage
        // failure into an expected live finding, even with the same message.
        let result = expected("F3", &entry, Err(Error::Storage(cause.clone())));
        proptest::prop_assert!(matches!(result, Err(Error::Storage(retained)) if retained == cause));
    }
}
