use crate::*;

/// Shared lifetime token for native resources that must not outlive an external owner.
pub struct OwnershipAnchor {
    _owner: Arc<dyn RetainFence>,
    release: OwnershipReleaseBarrier,
}

impl Clone for OwnershipAnchor {
    fn clone(&self) -> Self {
        Self {
            _owner: Arc::clone(&self._owner),
            release: self.release_barrier(),
        }
    }
}

struct ReleaseState {
    complete: crate::sync::Mutex<bool>,
    changed: crate::sync::Condvar,
    async_changed: tokio::sync::Notify,
}

// This field must be dropped after both the resource and every retained fence.
// Arc's strong count reaches zero *before* running the resource destructor.
struct ReleaseNotification(Arc<ReleaseState>);

impl Drop for ReleaseNotification {
    fn drop(&mut self) {
        {
            let mut complete = self
                .0
                .complete
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *complete = true;
        }
        // Wake outside the predicate lock: a waker may inspect the barrier.
        self.0.changed.notify_all();
        self.0.async_changed.notify_waiters();
    }
}

// Field order is the boundary: final shared-owner cleanup precedes every fence.
// Lock order: reopen fence -> `fences`; retain never acquires a reopen lock.
// Final cleanup drops the fence list without locking it.
struct SharedOwner<Owner, Fence> {
    _owner: Owner,
    fences: crate::sync::Mutex<Vec<Fence>>,
    released: ReleaseNotification,
}

impl<Owner, Fence> SharedOwner<Owner, Fence> {
    fn new(owner: Owner) -> Self {
        Self {
            _owner: owner,
            fences: crate::sync::Mutex::new(Vec::new()),
            released: ReleaseNotification(Arc::new(ReleaseState {
                complete: crate::sync::Mutex::new(false),
                changed: crate::sync::Condvar::new(),
                async_changed: tokio::sync::Notify::new(),
            })),
        }
    }

    fn retain(&self, fence: Fence) {
        self.fences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(fence);
    }
}

trait RetainFence: Send + Sync {
    fn retain_fence(&self, fence: Box<dyn Send + Sync>);
}

impl<T: Send + Sync> RetainFence for SharedOwner<T, Box<dyn Send + Sync>> {
    fn retain_fence(&self, fence: Box<dyn Send + Sync>) {
        self.retain(fence);
    }
}

impl OwnershipAnchor {
    /// Holds a provisional fence during initialization and retains it only on success.
    /// Failed or cancelled work keeps its own fence until its last worker releases it.
    pub async fn initialize_fenced<T, E, F: Future<Output = Result<T, E>>>(
        self,
        fence: impl Send + Sync + 'static,
        initialize: impl FnOnce(Self) -> F,
    ) -> Result<T, E> {
        // Both tuples drop the root before the fence, including on cancellation.
        // The fence never owns the root: committing cannot introduce an Arc cycle.
        let pending = (self, Arc::new(fence));
        let provisional = Self::new((pending.0.clone(), pending.1.clone()));
        let result = initialize(provisional).await;
        if result.is_ok() {
            pending.0._owner.retain_fence(Box::new(pending.1.clone()));
        }
        result
    }

    /// Retains a fence through cleanup of every clone, including pre-existing clones.
    pub fn fenced(owner: Self, fence: impl Send + Sync + 'static) -> Self {
        owner._owner.retain_fence(Box::new(fence));
        owner
    }

    /// Erases and retains one ownership value behind a cloneable lifetime token.
    pub fn new(owner: impl Send + Sync + 'static) -> Self {
        let owner = Arc::new(SharedOwner::<_, Box<dyn Send + Sync>>::new(owner));
        let released = Arc::clone(&owner.released.0);
        Self {
            release: OwnershipReleaseBarrier { released },
            _owner: owner,
        }
    }

    /// Observes the exact boundary at which every clone has released the owner.
    #[must_use]
    pub fn release_barrier(&self) -> OwnershipReleaseBarrier {
        OwnershipReleaseBarrier {
            released: Arc::clone(&self.release.released),
        }
    }
}

/// A non-owning release condition for an [`OwnershipAnchor`] clone family.
pub struct OwnershipReleaseBarrier {
    released: Arc<ReleaseState>,
}

impl OwnershipReleaseBarrier {
    /// Returns whether the final owner's cleanup and all retained fences finished.
    #[must_use]
    pub fn is_released(&self) -> bool {
        *self
            .released
            .complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Waits for physical cleanup without holding a blocking worker or the owner.
    /// Dropping this future cancels only the observation, never owner cleanup.
    pub async fn wait_async(self) {
        let changed = self.released.async_changed.notified();
        tokio::pin!(changed);
        // Register before inspecting the shared predicate. Cleanup can occur
        // before registration, between registration and inspection, or during
        // the await; every cut observes either completion or its notification.
        changed.as_mut().enable();
        if !self.is_released() {
            changed.await;
        }
    }

    /// Blocks until the final owner's cleanup and all retained fences finished.
    pub fn wait(self) {
        let mut complete = self
            .released
            .complete
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*complete {
            complete = self
                .released
                .changed
                .wait(complete)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(test)]
#[cfg(acyclic_native_runtime_loom)]
mod model;
