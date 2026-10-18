use super::*;

/// An accepted worker, including its completion payload, participates in
/// shutdown. Ordinary accounting handles retain funding without delaying it.
pub(crate) struct CaptureWorker {
    custody: CaptureCustody,
}

impl CaptureTransientCustody {
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn worker(
        custody: &CaptureCustody,
    ) -> Result<CaptureWorker, crate::NativeTreeMaterializationError> {
        if custody.closing.load(Ordering::SeqCst) {
            return Err(crate::NativeTreeMaterializationError::OriginalWorkPoisoned);
        }
        custody
            .workers
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |workers| {
                workers.checked_add(1)
            })
            .map_err(|_| crate::WorkError::Overflow)?;
        let worker = CaptureWorker {
            custody: custody.clone(),
        };
        // Close either observes this registration or this load rejects it.
        if custody.closing.load(Ordering::SeqCst) {
            return Err(crate::NativeTreeMaterializationError::OriginalWorkPoisoned);
        }
        Ok(worker)
    }

    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn close(&self) -> Result<(), DemandError> {
        self.closing.store(true, Ordering::SeqCst);
        #[cfg(all(test, acyclic_fs_loom))]
        let wait = !model_checking::skip_worker_wait();
        #[cfg(not(all(test, acyclic_fs_loom)))]
        let wait = true;
        while wait && self.workers.load(Ordering::SeqCst) != 0 {
            #[cfg(acyclic_fs_loom)]
            loom::thread::yield_now();
            #[cfg(not(acyclic_fs_loom))]
            std::thread::yield_now();
        }
        // Join in-flight transient accounting before draining retained bytes.
        // No worker wait or funding-owner destruction occurs under the leaf.
        #[cfg(all(test, acyclic_fs_loom))]
        if model_checking::close_without_leaf() {
            self.closed.store(true, Ordering::Release);
            let bytes = self.take_retained_bytes();
            return self.header.work().state(|work| work.release(bytes));
        }
        self.header.work().state(|work| {
            self.closed.store(true, Ordering::Release);
            work.release(self.take_retained_bytes())
        })
    }
}

impl Drop for CaptureWorker {
    fn drop(&mut self) {
        let previous = self.custody.workers.fetch_sub(1, Ordering::SeqCst);
        assert!(previous != 0, "balanced capture worker retirement");
    }
}
