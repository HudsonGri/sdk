#![no_main]
#[path = "../../harness/tests/e2e/provider_case.rs"]
mod provider_case;
#[path = "../../harness/tests/support/scenario_data.rs"]
mod scenario_data;
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return;
    };
    let Ok((value, binary)) = scenario_data::split_tiers(value) else {
        return;
    };
    if let Some(object) = value.as_object() {
        let mut envelope = object.clone();
        envelope.insert("binary".into(), binary.into());
        let encoded = serde_json::to_vec(&envelope).unwrap();
        assert_eq!(
            scenario_data::split_tiers(serde_json::from_slice(&encoded).unwrap()).unwrap(),
            (value.clone(), binary)
        );
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
