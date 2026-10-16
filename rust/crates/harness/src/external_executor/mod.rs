//! External host hooks normalized into immutable task requests.

mod answer;
mod append;
#[cfg(feature = "filesystem")]
pub(crate) mod cancellation;
mod catalog;
mod client;
#[cfg(feature = "filesystem")]
mod command_catalog;
#[cfg(feature = "filesystem")]
mod commands;
mod failure;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
mod handoff;
#[cfg(feature = "filesystem")]
mod intake;
#[cfg(feature = "filesystem")]
pub(crate) use intake::LIFECYCLE;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod local_owner;
#[cfg(feature = "filesystem-local")]
pub(crate) mod local_paths;
mod manifest;
#[cfg(all(feature = "filesystem", feature = "hooks", not(target_arch = "wasm32")))]
mod mcp;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
mod native;
mod normalize;
#[cfg(feature = "filesystem")]
mod prepared;
#[cfg(feature = "filesystem-local")]
pub(crate) mod progress;
#[cfg(all(feature = "filesystem", feature = "test-support"))]
pub(crate) use prepared::fuzz_command;
#[cfg(feature = "filesystem")]
mod publication;
mod request;
mod result;
mod session;
#[cfg(feature = "filesystem-local")]
pub(crate) mod task_bindings;
#[cfg(feature = "filesystem")]
mod tool_input;
pub(crate) mod volume;

pub use answer::hook_answer;
pub(crate) use append::publish as publish_request;
pub use catalog::{ExternalHost, HookAction, HookSpec, hooks};
#[cfg(feature = "filesystem")]
pub use commands::RequestCommands;
pub use failure::hook_failure;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub(crate) use handoff::file_key as native_result_key;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub use handoff::native_tool_request;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub(crate) use handoff::read_result as read_native_result;
#[cfg(feature = "filesystem")]
pub use intake::RequestConsumer;
#[cfg(all(
    feature = "test-support",
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use local_owner::fuzz_input as fuzz_local_owner_input;
#[cfg(all(
    feature = "test-support",
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use local_owner::fuzz_request as fuzz_request_record;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use local_owner::{run_local_owner, stop_local_owner};
pub use manifest::{claude_hooks, claude_manifest};
#[cfg(all(feature = "filesystem", feature = "hooks", not(target_arch = "wasm32")))]
pub use mcp::{native_tool_catalog, serve_native_tools};
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub use native::{HookBinding, run_hook};
pub use normalize::request;
pub use request::{ExternalAction, ExternalRequest};
pub use result::tool_result;
pub use session::CommandSessionMachine;
