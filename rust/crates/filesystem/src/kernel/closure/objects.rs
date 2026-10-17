use super::*;
use crate::kernel::allocation::VisitedObjectSet;
use crate::kernel::persistent_point::Owned;

// Membership has its own metered index. Canonical ordering is needed only when
// the completed proof leaves the walker, never while discovering new objects.
const INDEX_THRESHOLD: usize = 32;

pub(super) struct ProofObjects<'a> {
    values: ProofVec<'a, ObjectId>,
    index: Option<Owned<VisitedObjectSet>>,
    maximum: u64,
}

impl<'a> ProofObjects<'a> {
    pub(super) fn new(meter: &ProofMeter<'a>, maximum: u64) -> Self {
        Self {
            values: ProofVec::new(meter),
            index: None,
            maximum,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.values.values.len()
    }

    pub(super) fn contains(&self, object: ObjectId) -> Result<bool, crate::FsError> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if tests::old_insertions() {
            return Ok(self.values.values.binary_search(&object).is_ok());
        }
        self.index.as_ref().map_or_else(
            || Ok(self.values.values.binary_search(&object).is_ok()),
            |index| {
                self.values.meter.with_accounting(|accounting| {
                    index.value.contains_with_accounting(object, accounting)
                })
            },
        )
    }

    pub(super) fn insert(&mut self, object: ObjectId) -> Result<(), crate::FsError> {
        if self.contains(object)? {
            return Ok(());
        }
        let observed = usize_to_u64(self.len())
            .checked_add(1)
            .ok_or(WorkError::Overflow)?;
        if observed > self.maximum {
            return Err(ClosureError::TooManyObjects {
                observed,
                maximum: self.maximum,
            }
            .into());
        }
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if tests::old_insertions() {
            set_insert(&mut self.values, object)?;
            return Ok(());
        }
        if self.len() < INDEX_THRESHOLD {
            set_insert(&mut self.values, object)?;
            return Ok(());
        }
        if self.index.is_none() {
            self.index = Some(self.values.meter.with_accounting(|accounting| {
                VisitedObjectSet::new_with_accounting(
                    usize::try_from(self.maximum).unwrap_or(usize::MAX),
                    accounting,
                )
            })?);
            let index = self.index.as_mut().ok_or(ObjectStoreError::Corrupt)?;
            for &existing in &self.values.values {
                self.values.meter.with_accounting(|accounting| {
                    VisitedObjectSet::insert_with_accounting(index, existing, accounting).map(drop)
                })?;
            }
        }
        let index = self.index.as_mut().ok_or(ObjectStoreError::Corrupt)?;
        self.values.meter.with_accounting(|accounting| {
            VisitedObjectSet::insert_with_accounting(index, object, accounting).map(drop)
        })?;
        self.values.push(object)
    }

    pub(super) fn into_parts(mut self) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if tests::old_insertions() {
            return self.take_values().into_parts();
        }
        if self.index.is_none() {
            return self.take_values().into_parts();
        }
        // In-place heapsort has no scratch allocation and exposes every
        // comparison and swap to the original accounting before performing it.
        let count = self.len();
        for root in (0..count / 2).rev() {
            self.sift(root, count)?;
        }
        for end in (1..count).rev() {
            self.swap(0, end)?;
            self.sift(0, end)?;
        }
        self.take_values().into_parts()
    }

    fn take_values(&mut self) -> ProofVec<'a, ObjectId> {
        let empty = ProofVec::new(&self.values.meter);
        std::mem::replace(&mut self.values, empty)
    }

    fn sift(&mut self, mut root: usize, end: usize) -> Result<(), crate::FsError> {
        while root < end / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < end && self.less(child, child + 1)? {
                child += 1;
            }
            if !self.less(root, child)? {
                break;
            }
            self.swap(root, child)?;
            root = child;
        }
        Ok(())
    }

    fn less(&self, left: usize, right: usize) -> Result<bool, crate::FsError> {
        self.values.meter.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(self.values.values[left] < self.values.values[right])
    }

    fn swap(&mut self, left: usize, right: usize) -> Result<(), crate::FsError> {
        let copied = WorkCounters {
            bytes_copied: 3 * size_of::<ObjectId>() as u64,
            ..WorkCounters::UNCHARGED
        };
        let values = &mut self.values.values;
        self.values
            .meter
            .with_accounting(|accounting| accounting.perform(copied, || values.swap(left, right)))
    }
}

impl Drop for ProofObjects<'_> {
    fn drop(&mut self) {
        if let Some(index) = self.index.take() {
            let retired = self
                .values
                .meter
                .with_accounting(|accounting| index.retire(accounting));
            assert!(retired.is_ok(), "proof object index ownership: {retired:?}");
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(super) mod tests;
