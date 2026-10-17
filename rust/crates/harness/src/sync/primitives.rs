//! Production and model synchronization share these exact imports.
#[cfg(acyclic_harness_loom)]
pub(crate) use loom::sync::{Arc, Mutex, atomic};
#[cfg(not(acyclic_harness_loom))]
pub(crate) use std::sync::{Arc, Mutex, atomic};

#[cfg(all(acyclic_harness_loom, feature = "terminal", any(unix, windows)))]
pub(crate) use loom::sync::{Condvar, MutexGuard};
#[cfg(all(not(acyclic_harness_loom), feature = "terminal", any(unix, windows)))]
pub(crate) use std::sync::{Condvar, MutexGuard};
