//! The one owned-task API for production callers.
//!
//! Callers name a single `spawn`/`spawn_blocking` regardless of build
//! features. A `test-support` build routes these through the seeded
//! simulation scheduler, which itself falls back to Tokio when no simulation
//! is installed; every other build submits directly to Tokio. Both report a
//! cancelled task as `ErrorKind::Interrupted`.
#[cfg(feature = "test-support")]
pub use crate::testing::{AbortHandle, TaskHandle, spawn, spawn_blocking};
#[cfg(not(feature = "test-support"))]
pub use native::{AbortHandle, TaskHandle, spawn, spawn_blocking};

/// A task that requests cancellation when its awaiting owner is dropped.
///
/// Dropping requests cancellation; await the handle to observe task retirement.
/// This policy is shared by native and simulated execution on every OS.
#[must_use = "dropping this handle cancels the task"]
pub struct CancelOnDrop<T>(TaskHandle<T>);

impl<T> TaskHandle<T> {
    /// Cancels this task when its awaiting owner is dropped instead of detaching it.
    pub fn cancel_on_drop(self) -> CancelOnDrop<T> {
        CancelOnDrop(self)
    }
}

impl<T> Drop for CancelOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> std::future::Future for CancelOnDrop<T> {
    type Output = std::io::Result<T>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

/// Maps a Tokio join failure onto the shared task error contract.
pub(crate) fn join_error(cause: tokio::task::JoinError) -> std::io::Error {
    if cause.is_cancelled() {
        std::io::Error::new(std::io::ErrorKind::Interrupted, cause)
    } else {
        std::io::Error::other(cause)
    }
}

#[cfg(not(feature = "test-support"))]
mod native {
    use std::{
        future::Future,
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    /// Owned task handle. Dropping detaches; aborting cancels the future.
    pub struct TaskHandle<T>(tokio::task::JoinHandle<T>);

    /// Cancellation authority retained independently of the task's result.
    #[derive(Clone)]
    pub struct AbortHandle(tokio::task::AbortHandle);

    impl AbortHandle {
        /// Requests cancellation of the original owned future.
        pub fn abort(&self) {
            self.0.abort();
        }
    }

    impl<T> TaskHandle<T> {
        /// Cancels the task even if its caller no longer awaits it.
        pub fn abort(&self) {
            self.0.abort();
        }

        /// Retains cancellation authority without retaining the result.
        pub fn abort_handle(&self) -> AbortHandle {
            AbortHandle(self.0.abort_handle())
        }
    }

    impl<T> Future for TaskHandle<T> {
        type Output = io::Result<T>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.get_mut().0)
                .poll(cx)
                .map(|result| result.map_err(super::join_error))
        }
    }

    /// Submits an owned future to the current Tokio runtime.
    pub fn spawn<F>(future: F) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        TaskHandle(tokio::spawn(future))
    }

    /// Runs a blocking closure on Tokio's blocking pool, retaining ownership
    /// after the caller drops the returned future.
    pub fn spawn_blocking<F, T>(work: F) -> impl Future<Output = io::Result<T>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let task = tokio::task::spawn_blocking(work);
        async move { task.await.map_err(super::join_error) }
    }
}

#[cfg(test)]
#[cfg(not(acyclic_native_runtime_loom))]
mod tests {
    #[cfg(feature = "test-support")]
    use std::time::Duration;

    #[test]
    fn dropping_a_scoped_task_cancels_before_releasing_its_work() {
        struct Completion(bool, Option<tokio::sync::oneshot::Sender<bool>>);
        impl Drop for Completion {
            fn drop(&mut self) {
                self.1.take().unwrap().send(self.0).unwrap();
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (started, starting) = tokio::sync::oneshot::channel();
            let (release, wait) = tokio::sync::oneshot::channel();
            let (finished, finishing) = tokio::sync::oneshot::channel();
            let task = super::spawn(async move {
                let mut completion = Completion(false, Some(finished));
                started.send(()).unwrap();
                wait.await.unwrap();
                completion.0 = true;
            })
            .cancel_on_drop();
            starting.await.unwrap();
            drop(task);
            // A detached task would finish successfully after this release.
            // Cancellation has to win before its owner can publish the release.
            let _ = release.send(());
            assert!(!finishing.await.unwrap());
        });
    }

    // Outside a simulation, blocking work must stay visible to a paused Tokio
    // clock: virtual time may not auto-advance past an unfinished closure.
    // Negative control: routing this path through the native worker service
    // (invisible to Tokio) lets the hour-long virtual timeout elapse at once.
    #[test]
    #[cfg(feature = "test-support")]
    fn unsimulated_blocking_work_holds_a_paused_clock() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            release.send(()).unwrap();
        });
        let finished = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(3600),
                super::spawn_blocking(move || wait.recv().unwrap()),
            )
            .await
        });
        releaser.join().unwrap();
        assert!(matches!(finished, Ok(Ok(()))), "{finished:?}");
    }

    #[test]
    fn an_aborted_task_reports_interrupted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let error = runtime.block_on(async {
            let task = super::spawn(std::future::pending::<()>());
            task.abort();
            task.await.unwrap_err()
        });
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    }
}
