use crate::*;

/// Shared lifetime token for native resources that must not outlive an external owner.
pub struct OwnershipAnchor {
    pub(crate) _owner: Arc<dyn Send + Sync>,
    pub(crate) release: OwnershipReleaseBarrier,
}

impl Clone for OwnershipAnchor {
    fn clone(&self) -> Self {
        Self {
            _owner: Arc::clone(&self._owner),
            release: self.release_barrier(),
        }
    }
}

impl Drop for OwnershipReleaseBarrier {
    fn drop(&mut self) {
        if self.owner.strong_count() == 0 {
            let (lock, released) = &*self.released;
            let _guard = lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            released.notify_all();
        }
    }
}

// Field drop order is the synchronization boundary: cleanup completes before
// the sole reopen fence is released. No other locks may be acquired by cleanup.
struct FencedOwner<Owner, Fence> {
    _owner: Owner,
    _fence: Fence,
}

impl OwnershipAnchor {
    /// Retains an owner and releases its fence only after owner cleanup completes.
    pub fn fenced(owner: impl Send + Sync + 'static, fence: impl Send + Sync + 'static) -> Self {
        Self::new(FencedOwner {
            _owner: owner,
            _fence: fence,
        })
    }

    /// Erases and retains one ownership value behind a cloneable lifetime token.
    pub fn new(owner: impl Send + Sync + 'static) -> Self {
        let owner: Arc<dyn Send + Sync> = Arc::new(owner);
        Self {
            release: OwnershipReleaseBarrier {
                owner: Arc::downgrade(&owner),
                released: Arc::new((Mutex::new(()), Condvar::new())),
            },
            _owner: owner,
        }
    }

    /// Observes the exact boundary at which every clone has released the owner.
    #[must_use]
    pub fn release_barrier(&self) -> OwnershipReleaseBarrier {
        OwnershipReleaseBarrier {
            owner: self.release.owner.clone(),
            released: Arc::clone(&self.release.released),
        }
    }
}

/// A non-owning release condition for an [`OwnershipAnchor`] clone family.
pub struct OwnershipReleaseBarrier {
    pub(crate) owner: Weak<dyn Send + Sync>,
    pub(crate) released: Arc<(Mutex<()>, Condvar)>,
}

impl OwnershipReleaseBarrier {
    /// Returns whether every ownership-anchor clone has released the owner.
    #[must_use]
    pub fn is_released(&self) -> bool {
        self.owner.strong_count() == 0
    }

    /// Blocks until every ownership-anchor clone has released the owner.
    pub fn wait(self) {
        let (lock, released) = &*self.released;
        let mut guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.owner.strong_count() != 0 {
            guard = released
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(all(test, acyclic_native_runtime_loom))]
mod model;
