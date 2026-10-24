//! Harness composition with real model provider adapters.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_harness::{
    Result,
    client::{CommandCatalog, CommandResolution},
};
use client_files::Fixture;
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[path = "../../../harness/tests/support/client_files.rs"]
mod client_files;
mod client_mention_provider;
#[path = "../../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../../harness/tests/support/frame_oracle.rs"]
mod frame_oracle;
#[path = "../../../harness/tests/e2e/local_admission.rs"]
mod local_admission;
#[path = "../../../harness/tests/e2e/local_workspace.rs"]
mod local_workspace;
#[path = "../../../harness/tests/support/scenario_matchers.rs"]
mod matchers;
mod mcp_identity;
mod native_hook_http;
#[path = "../../../harness/tests/support/ordered_matcher.rs"]
mod ordered_matcher;
#[path = "../../../harness/tests/support/progress_guard.rs"]
mod progress_guard;
#[path = "../../../harness/tests/support/progress_registry.rs"]
mod progress_registry;
mod provider_case;
mod provider_failure_exit;
mod provider_http;
mod provider_tests;
mod providers;
#[path = "../../../harness/tests/support/quiescent.rs"]
mod quiescent;
#[path = "../../../harness/tests/support/scenario_data.rs"]
mod scenario_data;
#[path = "../../../harness/tests/e2e/stall.rs"]
mod stall;

mod provider_scenarios {
    use super::{TestResult, provider_case::Protocol};

    #[tokio::test]
    async fn anthropic() -> TestResult {
        super::run_provider_scenarios(Protocol::Anthropic).await
    }
    #[tokio::test]
    async fn responses() -> TestResult {
        super::run_provider_scenarios(Protocol::Responses).await
    }
    #[tokio::test]
    async fn chat() -> TestResult {
        super::run_provider_scenarios(Protocol::Chat).await
    }
    #[tokio::test]
    async fn gemini() -> TestResult {
        super::run_provider_scenarios(Protocol::Gemini).await
    }
}

// Each protocol is an independent libtest suite with private filesystem state.
// The standard four-thread runner can drive these without one serial test
// paying for every protocol's cold durable-store lifecycle.
async fn run_provider_scenarios(protocol: provider_case::Protocol) -> TestResult {
    let root = scenario_root();
    let mut paths = Vec::new();
    scenario_data::discover(&root, &mut paths)?;
    paths.sort();
    if let Some(filter) = std::env::var_os("E2E_FILTER") {
        let filter = filter.to_string_lossy();
        paths.retain(|path| {
            path.to_string_lossy()
                .replace('\\', "/")
                .contains(filter.as_ref())
        });
    }
    assert!(!paths.is_empty());
    let mut admitted = 0;
    for path in paths {
        let mut value = scenario_data::load_value(&root, &path)?;
        assert_eq!(
            value.as_object_mut().ok_or("scenario")?.remove("driver"),
            Some("provider".into())
        );
        let case: provider_case::ProviderCase = serde_json::from_value(value)?;
        if case.protocol != protocol {
            continue;
        }
        admitted += 1;
        stall::guarded(&path, providers::run(case)).await??;
        println!("PASS {}", path.strip_prefix(&root)?.display());
    }
    assert!(admitted > 0, "protocol suite has no cases: {protocol:?}");
    Ok(())
}

fn scenario_root() -> std::path::PathBuf {
    acyclic_test_support::source_path("rust/crates/harness-providers").join("tests/scenarios")
}
