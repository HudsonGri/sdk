//! Shared HTTP exchange and event framing beneath protocol adapters.
#[cfg(all(feature = "native-http", not(target_arch = "wasm32")))]
pub mod native;
pub mod sse;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub mod browser;
