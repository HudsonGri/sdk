//! Owned Tokio tasks that keep native-runtime instrumentation attached.

use std::future::Future;

/// Spawns an owned Tokio task.
///
/// Callers use this one entry point in every build. The crate that owns the
/// test-support split decides whether an active poll-sweep schedule also
/// observes the task's submission and terminal boundaries; without an active
/// schedule (and in every production build) it is exactly `tokio::spawn`.
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "test-support")]
    return crate::testing::spawn(future);
    #[cfg(not(feature = "test-support"))]
    tokio::spawn(future)
}
