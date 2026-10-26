//! Shared native HTTP and immutable recording format.
#[cfg(all(feature = "native-http", not(target_arch = "wasm32")))]
pub mod native;
#[cfg(not(target_arch = "wasm32"))]
pub mod recording;

/// Bounded event framing shared by provider and replay adapters.
pub mod sse;
