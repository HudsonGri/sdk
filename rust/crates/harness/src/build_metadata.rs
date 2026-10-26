//! Compile-time Cargo metadata of the facade; everything else comes from
//! `acyclic_harness_core::build_metadata`.
#![expect(
    clippy::disallowed_macros,
    reason = "explicit Cargo metadata only; user environment discovery is forbidden"
)]

/// Cargo manifest directory for checked-in fixtures.
pub const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");
