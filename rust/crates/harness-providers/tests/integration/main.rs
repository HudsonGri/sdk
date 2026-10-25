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
    // Each fixture has private durable state. Giving libtest each independent
    // case avoids a serial protocol tail after its other workers become idle.
    // The inventory oracle below makes an omitted or newly added case fail.
    macro_rules! cases {
        ($($protocol:ident { $($case:ident),* $(,)? })*) => {
            const INVENTORY: &[&str] = &[$($(concat!(stringify!($protocol), "/", stringify!($case))),*),*];
            $(mod $protocol {
                $(#[tokio::test]
                async fn $case() -> super::super::TestResult {
                    super::super::run_provider_scenario(concat!(stringify!($protocol), "/", stringify!($case))).await
                })*
            })*
        };
    }
    cases! {
        anthropic { bytewise, exhausted_500, mcp_name, reasoning, refused_400,
            retry_429, stall_after_output, stall_retry, text, unreachable_exhausted,
            unreachable_retry, utf8_cut, write }
        responses { bytewise, mcp_name, reasoning, text, utf8_cut, write }
        chat { bytewise, reasoning, text, utf8_cut, write }
        gemini { bytewise, reasoning, text, utf8_cut, write }
    }

    #[test]
    fn inventory_contains_every_provider_fixture() -> super::TestResult {
        let root = super::scenario_root();
        let mut paths = Vec::new();
        super::scenario_data::discover(&root, &mut paths)?;
        let actual = paths
            .iter()
            .map(|path| {
                path.strip_prefix(root.join("providers")).map(|relative| {
                    relative
                        .with_extension("")
                        .to_string_lossy()
                        .replace('\\', "/")
                })
            })
            .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()?;
        let declared = INVENTORY
            .iter()
            .map(|name| name.replace('_', "-"))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, declared);
        Ok(())
    }
}

async fn run_provider_scenario(name: &str) -> TestResult {
    let root = scenario_root();
    let path = root
        .join("providers")
        .join(name.replace('_', "-"))
        .with_extension("json");
    let mut value = scenario_data::load_value(&root, &path)?;
    assert_eq!(
        value.as_object_mut().ok_or("scenario")?.remove("driver"),
        Some("provider".into())
    );
    let case: provider_case::ProviderCase = serde_json::from_value(value)?;
    stall::guarded(&path, providers::run(case)).await??;
    println!("PASS {}", path.strip_prefix(&root)?.display());
    Ok(())
}

fn scenario_root() -> std::path::PathBuf {
    acyclic_test_support::source_path("rust/crates/harness-providers").join("tests/scenarios")
}
