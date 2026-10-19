//! Live admission, cancellation and result observation.
use crate::{Admission, NativeWork, OperationId, Outcome};
#[cfg(all(not(target_arch = "wasm32"), feature = "test-support"))]
use acyclic_native_runtime::testing::{AbortHandle, TaskHandle as OwnedTask, spawn};
use acyclic_stream::BoxProviderStream as BoxStream;
#[cfg(target_arch = "wasm32")]
use futures::future::AbortHandle;
use futures::{StreamExt, stream};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Semaphore, oneshot};
#[cfg(all(not(target_arch = "wasm32"), not(feature = "test-support")))]
use tokio::{
    spawn,
    task::{AbortHandle, JoinHandle as OwnedTask},
};
mod group;
pub use group::TaskGroup;
mod handle;
pub use handle::TaskHandle;
mod completion;
pub use completion::*;
mod timer;
pub(crate) use timer::{sleep_millis, timeout_until};
#[cfg(all(test, target_arch = "wasm32"))]
mod browser_tests;
#[cfg(test)]
mod tests;
