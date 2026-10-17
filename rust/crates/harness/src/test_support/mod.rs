//! Deterministic model scripts for harness consumers and scenario tests.

pub(crate) mod admission_work;
mod scripted;
mod search;
mod task_volume_work;
pub use search::{ScriptedSearch, ScriptedSearchProvider};

pub use scripted::{ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn};
pub use task_volume_work::measure as measure_task_volume_work;

/// Exercises local owner configuration and credential parsers without I/O.
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub fn fuzz_local_owner_input(bytes: &[u8]) {
    crate::external_executor::fuzz_local_owner_input(bytes);
}

/// Exercises native Codex transcript ancestry without opening files or processes.
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub fn fuzz_codex_hook_parent(bytes: &[u8]) {
    crate::external_executor::fuzz_codex_parent(bytes);
}

/// Exercises the typed native-to-stock command decoder without dispatching effects.
#[cfg(feature = "filesystem")]
pub fn fuzz_external_command(request: &crate::external_executor::ExternalRequest) {
    crate::external_executor::fuzz_command(request);
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    crate::external_executor::fuzz_request_record(request);
}
pub(crate) use task_volume_work::{
    Files as TaskVolumeFiles, examined as task_volume_item_examined,
};

/// Local test composition with temporary settings, independent of HOME and CODEX_HOME.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn local_harness_config(
    workspace: impl Into<std::path::PathBuf>,
    model: (
        crate::model::Model,
        std::sync::Arc<dyn crate::model::ModelProvider>,
    ),
) -> crate::Result<crate::LocalHarnessConfig> {
    let workspace = workspace.into();
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let settings = crate::native_settings::SettingsConfig::for_test(&workspace)?;
    let config = crate::LocalHarnessConfig::new(workspace, model);
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let config = config.settings(settings);
    Ok(config)
}

/// The composition's actual task provider, for scenario wire-frame assertions.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn local_task_stream(
    harness: &crate::LocalHarness,
) -> std::sync::Arc<acyclic_stream::LocalStream> {
    harness.task_stream()
}

pub(crate) mod model_view_work;
pub use model_view_work::measure as measure_model_view_work;
