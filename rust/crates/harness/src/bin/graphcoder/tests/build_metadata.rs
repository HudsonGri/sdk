//! The shared integration-binary fixture seam, including cross-built layouts.
#[expect(
    clippy::disallowed_macros,
    reason = "Cargo-provided executable metadata for this integration crate"
)]
const EXECUTABLE: &str = env!("CARGO_BIN_EXE_graphcoder");
pub(super) fn executable() -> std::path::PathBuf {
    // The Windows test runner admits all Cargo artifacts into one bin directory.
    std::env::current_exe()
        .ok()
        .and_then(|test| {
            test.parent()
                .map(|parent| parent.join(format!("graphcoder{}", std::env::consts::EXE_SUFFIX)))
        })
        .filter(|colocated| colocated.is_file())
        .unwrap_or_else(|| EXECUTABLE.into())
}
