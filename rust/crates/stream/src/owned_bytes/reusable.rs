use super::*;

/// A funded byte buffer that can be reused after its last published view dies.
///
/// The allocation and its byte-owner header are constructed once, before any
/// physical operation. An outstanding immutable view prevents mutable access.
/// No allocation, admission or cancellation check occurs when reusing storage.
pub struct ReusableOwnedBytes<G: Send + Sync + 'static> {
    guard: Arc<GuardBundle<G>>,
}

impl<G: Send + Sync + 'static> ReusableOwnedBytes<G> {
    /// Admits fully initialized fixed storage and its original allocation owner.
    pub fn new(
        data: Vec<u8>,
        guard: G,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        PreparedOwnedBytes::new(data, guard, meter).map(|prepared| Self {
            guard: prepared.into_guard(),
        })
    }

    /// Initialized capacity, fixed for this buffer's entire lifetime.
    #[must_use]
    pub fn maximum_len(&self) -> usize {
        self.guard.data.len()
    }

    /// Borrows mutable storage only when every previous immutable view is gone.
    ///
    /// A failed borrow leaves both the original bytes and their owner untouched.
    pub fn try_writable(&mut self) -> Option<&mut [u8]> {
        Arc::get_mut(&mut self.guard).map(|guard| guard.data.as_mut_slice())
    }

    /// Publishes an initialized prefix while retaining this buffer for reuse.
    ///
    /// Returned values and slices keep the same allocation owner alive even
    /// after this reusable handle is dropped. An oversized prefix is rejected.
    #[must_use]
    pub fn snapshot(&self, actual_len: usize) -> Option<OwnedBytes> {
        if actual_len > self.maximum_len() {
            return None;
        }
        Some(OwnedBytes {
            retained_capacity: Some(self.guard.data.capacity()),
            range: 0..actual_len,
            data: Data::Native(self.guard.clone()),
        })
    }
}

impl<G: Send + Sync + 'static> std::fmt::Debug for ReusableOwnedBytes<G> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReusableOwnedBytes")
            .field("maximum_len", &self.maximum_len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
