use super::*;
use acyclic_native_runtime::ReadAllocation;

/// A growing traversal stack retains the allocation ledger that paid for it.
pub(super) struct Slots<T> {
    pub(super) values: Vec<T>,
    allocation: Option<ReadAllocation>,
}

impl<T> Default for Slots<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            allocation: None,
        }
    }
}

impl<T: Copy> Slots<T> {
    pub(super) fn push(
        &mut self,
        value: T,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<(), LocalStreamError> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .ok_or(LocalStreamError::InvalidLimits)?
                .max(1);
            let mut allocation = work.map(|work| ReadAllocation::empty(&work.meter));
            let mut values = if let (Some(work), Some(allocation)) = (work, &mut allocation) {
                page::observe(
                    Some(work),
                    crate::native::vector(capacity, &work.meter, allocation),
                )?
            } else {
                let mut values = Vec::new();
                values
                    .try_reserve_exact(capacity)
                    .map_err(|_| LocalStreamError::InvalidLimits)?;
                values
            };
            let copied = self
                .values
                .len()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(LocalStreamError::InvalidLimits)? as u64;
            page::codec(work, copied, 0, true)?;
            values.extend_from_slice(&self.values);
            page::codec(work, copied, 0, false)?;
            // Destroy the old capacity before releasing its original charge.
            self.values = values;
            self.allocation = allocation;
        }
        self.values.push(value);
        Ok(())
    }
}
