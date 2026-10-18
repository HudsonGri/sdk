#![no_main]
#[path = "../../harness/src/bin/graphcoder/tests/scenarios/faults.rs"]
mod faults;
#[path = "../../harness/src/bin/graphcoder/tests/scenarios/history.rs"]
mod history;
#[path = "../../harness/tests/support/json_matchers.rs"]
mod json_matchers;
#[path = "../../harness/tests/e2e/provider_case.rs"]
mod provider_case;
#[path = "../../harness/tests/support/scenario_actions.rs"]
mod scenario_actions;
#[path = "../../harness/tests/support/scenario_data.rs"]
mod scenario_data;
#[path = "../../harness/src/bin/graphcoder/tests/scenarios/results.rs"]
mod wire;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return;
    };
    let Ok((value, binary)) = scenario_data::split_tiers(value) else {
        return;
    };
    if let Ok(fault) = serde_json::from_value::<faults::Fault>(value.clone()) {
        let bytes = serde_json::to_vec(&fault).unwrap();
        let decoded: faults::Fault = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.validate(), fault.validate());
        assert_eq!(decoded, fault);
    }
    if let Ok(action) = serde_json::from_value::<scenario_actions::Action>(value.clone()) {
        let encoded = serde_json::to_vec(&action).unwrap();
        assert_eq!(
            serde_json::from_slice::<scenario_actions::Action>(&encoded).unwrap(),
            action
        );
        if let Ok(file) = scenario_actions::file(&action) {
            assert_eq!(
                acyclic_harness::task_volume::FileKey::parse(&file.key.path()).unwrap(),
                file.key
            );
        }
    }
    if let Some(object) = value.as_object() {
        let mut envelope = object.clone();
        envelope.insert("binary".into(), binary.into());
        let encoded = serde_json::to_vec(&envelope).unwrap();
        assert_eq!(
            scenario_data::split_tiers(serde_json::from_slice(&encoded).unwrap()).unwrap(),
            (value.clone(), binary)
        );
    }
    if let (Some(expected), Some(actual)) = (value.get("history_expect"), value.get("wire_request"))
    {
        for protocol in ["responses", "anthropic"] {
            if let Ok(matched) = history::matches(expected, actual, protocol) {
                let decoded: serde_json::Value =
                    serde_json::from_slice(&serde_json::to_vec(actual).unwrap()).unwrap();
                assert_eq!(
                    history::matches(expected, &decoded, protocol).unwrap(),
                    matched
                );
            }
        }
    }
    let request = value.get("wire_request").unwrap_or(&value);
    for protocol in ["responses", "anthropic"] {
        if let Ok(outputs) = wire::outputs(request, protocol) {
            let decoded = serde_json::from_slice(&serde_json::to_vec(request).unwrap()).unwrap();
            assert_eq!(wire::outputs(&decoded, protocol).unwrap(), outputs);
            if let Ok(unique) = wire::unique_outputs(outputs.clone()) {
                assert_eq!(unique.len(), outputs.len());
                if let Some(first) = outputs.first() {
                    let mut duplicate = outputs.clone();
                    duplicate.push(first.clone());
                    assert!(wire::unique_outputs(duplicate).is_err());
                }
            }
        }
    }
    let Ok(case) = serde_json::from_value::<provider_case::ProviderCase>(value) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    assert_eq!(
        serde_json::from_slice::<provider_case::ProviderCase>(&encoded).unwrap(),
        case
    );
    if case.validate().is_ok() {
        assert!(!case.responses.is_empty());
        assert_eq!(case.responses.len(), case.expect.events.len());
        assert!(case.chunk_bytes.get() > 0);
    }
});
