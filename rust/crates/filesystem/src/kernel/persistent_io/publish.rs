use super::*;
use crate::kernel::persistent_btree::entries::Entry;
use crate::kernel::persistent_point::Owned;

// Call only after put_hashed succeeds: a decoded cache hit must never make an
// object visible before its authenticated bytes exist in the object store.
pub(crate) fn retain_written_leaf<S: AsyncObjectStore, F: Format>(
    store: &S,
    object: ObjectId,
    limits: DecodeLimits,
    entries: &[Arc<Entry<F::Value>>],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if !store.supports_decoded_cache() {
        return Ok(());
    }
    let empty = accounting.claim_owned(0, 0)?;
    let mut copied = Owned::allocate(entries.len(), accounting)?;
    let mut logical_bytes = copied.bytes;
    let result = (|| {
        for entry in entries {
            logical_bytes = logical_bytes
                .checked_add(entry.bytes)
                .ok_or(WorkError::Overflow)?;
            accounting.charge(WorkCounters {
                bytes_copied: usize_to_u64(size_of::<Arc<Entry<F::Value>>>()),
                ..WorkCounters::UNCHARGED
            })?;
            copied.value.push(Arc::clone(entry));
        }
        Ok::<_, FsError>(())
    })();
    if let Err(error) = result {
        copied.retire(accounting)?;
        return Err(error);
    }
    let shared = Owned::new(
        SharedPage::<F>::Leaf {
            entries: copied.value,
            logical_bytes,
        },
        copied.allocation,
        copied.bytes,
    );
    let shared = super::cache::reserve_shared_controls(shared, accounting)?;
    let (page, allocation) = super::cache::admit_shared_page(
        store,
        DecodedCacheKey::new::<SharedPage<F>>(object, limits),
        shared,
        empty,
        accounting,
    )?;
    let bytes = page.logical_bytes;
    drop(page);
    crate::kernel::persistent_point::retire_allocation(allocation, bytes, accounting)
}
