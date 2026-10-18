#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

#[path = "../../harness/tests/e2e/matchers.rs"]
mod matchers;
#[path = "../../harness/tests/e2e/notification_history.rs"]
mod notification_history;
#[path = "../../harness/tests/e2e/notifications.rs"]
mod notifications;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let Some(value) = fuzz_support::json_input!(fuzz_input, serde_json::Value) else {
        return;
    };
    let _ = matchers::subset(
        &serde_json::json!({"$json": &value}),
        &serde_json::Value::String(fuzz_support::must_decode(std::str::from_utf8(bytes)).into()),
    );
    let Some(case) = value.admit(notifications::Notifications::parse) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    let _ = fuzz_support::round_trip(
        &case,
        notifications::Notifications::parse(serde_json::from_slice(&encoded).unwrap()),
    );
    let _ = case.verify_history();
});
