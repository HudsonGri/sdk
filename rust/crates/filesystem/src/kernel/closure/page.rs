use super::*;
use std::any::Any;

pub(super) struct ProvenPage<T> {
    value: PageValue<T>,
    // Data retires before its operation-owned allocation.
    _allocation: ProofGuard,
}

enum PageValue<T> {
    Owned(T),
    #[cfg(not(target_arch = "wasm32"))]
    Shared {
        page: Arc<T>,
        _custody: Option<Arc<crate::NativeAllocation>>,
    },
}

#[cfg(not(target_arch = "wasm32"))]
enum Decoded<T> {
    Owned(T),
    Cache(crate::async_storage::DecodedCacheValue),
}

impl<T> ProvenPage<T> {
    pub(super) fn page(&self) -> &T {
        match &self.value {
            PageValue::Owned(page) => page,
            #[cfg(not(target_arch = "wasm32"))]
            PageValue::Shared { page, .. } => page,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn shared<T: Any + Send + Sync>(
    cached: crate::async_storage::DecodedCacheValue,
) -> Result<PageValue<T>, crate::FsError> {
    Ok(PageValue::Shared {
        page: cached
            .value
            .downcast()
            .map_err(|_| ObjectStoreError::Corrupt)?,
        _custody: cached.native_custody,
    })
}

pub(super) fn persistent<S: crate::AsyncObjectStore, F: crate::kernel::persistent_btree::Format>(
    store: &S,
    id: ObjectId,
    limits: DecodeLimits,
    meter: &ProofMeter<'_>,
    bytes: &[u8],
) -> Result<ProvenPage<crate::kernel::persistent_btree::Page<F>>, crate::FsError> {
    // Persistent page readers count the decoded body when cloning a cached page.
    load(
        store,
        id,
        limits,
        meter,
        |observe| F::decode(bytes, limits, &mut |step| observe(step)),
        |body, _| body,
    )
}

pub(super) fn decode<S: crate::AsyncObjectStore, T: Any + Send + Sync>(
    store: &S,
    id: ObjectId,
    limits: DecodeLimits,
    meter: &ProofMeter<'_>,
    decode: impl FnOnce(
        &mut dyn FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), crate::FsError>,
    ) -> Result<T, crate::FsError>,
) -> Result<ProvenPage<T>, crate::FsError> {
    load(store, id, limits, meter, decode, |_, retained| retained)
}

fn load<S: crate::AsyncObjectStore, T: Any + Send + Sync>(
    store: &S,
    id: ObjectId,
    limits: DecodeLimits,
    meter: &ProofMeter<'_>,
    decode: impl FnOnce(
        &mut dyn FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), crate::FsError>,
    ) -> Result<T, crate::FsError>,
    weight: impl FnOnce(u64, u64) -> u64,
) -> Result<ProvenPage<T>, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        use crate::kernel::file_read::{
            OriginalReadAccounting, ReadOwned, decoded_cache_controls, share_decoded_read,
        };
        meter.charge(WorkCounters::UNCHARGED)?;
        let key = crate::async_storage::DecodedCacheKey::new::<T>(id, limits);
        if let Some(cached) = store.decoded_cache_get(key)? {
            let (_, allocation) =
                meter.owned(|accounting| Ok(((), accounting.claim_owned(0, 0)?, 0)))?;
            return Ok(ProvenPage {
                value: shared(cached)?,
                _allocation: allocation,
            });
        }
        let supports_cache = store.supports_decoded_cache();
        // Never invoke a provider while holding the standalone proof-state lock.
        let (value, allocation) = meter.owned(|accounting| {
            let mut collector = OriginalReadAccounting::new(accounting)?;
            let page = match decode(&mut |step| collector.observe(step)) {
                Ok(page) => page,
                Err(error) => {
                    collector.release_failed()?;
                    return Err(error);
                }
            };
            let (allocation, bytes) = collector.into_parts();
            let (controls, operations) = decoded_cache_controls::<T>(accounting)?;
            let remaining = accounting.remaining()?;
            if !supports_cache
                || controls as u64 > remaining.peak_allocation_bytes
                || operations > remaining.allocation_operations
            {
                return Ok((Decoded::Owned(page), allocation, bytes));
            }
            let empty = accounting.claim_owned(0, 0)?;
            let (mut cached, retained) = share_decoded_read(
                ReadOwned {
                    value: (page, bytes),
                    custody: allocation,
                },
                accounting,
            )?;
            cached.logical_bytes = weight(bytes, retained);
            let retained = if accounting.native_work().is_some() {
                0
            } else {
                retained
            };
            Ok((Decoded::Cache(cached), empty, retained))
        })?;
        let value = match value {
            Decoded::Owned(page) => PageValue::Owned(page),
            Decoded::Cache(cached) => {
                meter.charge(WorkCounters::UNCHARGED)?;
                let cached = match store
                    .decoded_cache_admit(key, cached)
                    .map_err(crate::kernel::file_read::read_store_error)?
                {
                    crate::async_storage::DecodedCacheAdmission::Shared(cached)
                    | crate::async_storage::DecodedCacheAdmission::Uncached(cached) => cached,
                };
                shared(cached)?
            }
        };
        Ok(ProvenPage {
            value,
            _allocation: allocation,
        })
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (store, id, limits, weight);
        let (page, allocation) = meter.decode_steps(decode)?;
        Ok(ProvenPage {
            value: PageValue::Owned(page),
            _allocation: allocation,
        })
    }
}

#[cfg(all(test, feature = "memory", not(target_arch = "wasm32")))]
mod tests;
