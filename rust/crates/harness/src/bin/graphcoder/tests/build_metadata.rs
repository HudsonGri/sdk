//! The shared integration-binary fixture seam, including cross-built layouts.
#[expect(
    clippy::disallowed_macros,
    reason = "Cargo-provided executable metadata for this integration crate"
)]
const EXECUTABLE: &str = env!("CARGO_BIN_EXE_graphcoder");
pub(super) fn executable() -> std::path::PathBuf {
    acyclic_test_support::binary_path(EXECUTABLE)
}
