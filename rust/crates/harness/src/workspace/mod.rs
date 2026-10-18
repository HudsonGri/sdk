//! Task-selected native views and their filesystem access decisions.
mod composition;
mod discovery;
pub use acyclic_fs::access::MountMode;
pub use composition::{Mount, MountTable, VolumeView, compose};

/// Root names reserved for SDK state in every checkout. Source demand, native
/// capture and native publication all skip them, so host bytes under these
/// names can neither enter a checkout head nor overwrite SDK-owned entries.
pub(crate) const SDK_NAMESPACES: [&str; 2] = [".system", ".volumes"];
