//! Shared widgets and ClientViews presentation for native clients.
pub use acyclic_harness_tui::*;
#[cfg(feature = "terminal")]
mod render;
#[cfg(feature = "terminal")]
pub use render::ClientRenderer;
#[cfg(all(feature = "terminal", feature = "grpc", not(target_arch = "wasm32")))]
pub use render::AgentRenderer;

#[cfg(feature = "terminal")]
mod diff;
#[cfg(feature = "terminal")]
pub use diff::diff_lines;

#[cfg(feature = "terminal")]
mod help;

pub(crate) mod catalog;
