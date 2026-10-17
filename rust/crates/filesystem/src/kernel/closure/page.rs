use super::*;
use std::any::Any;

pub(super) enum ProvenPage<T> {
    Owned(T, ProofGuard),
    #[cfg(not(target_arch = "wasm32"))]
    Shared {
        page: Arc<T>,
        // A borrowed page retains the original decoder's native allocation.
        _custody: Option<Arc<crate::NativeAllocation>>,
    },
}

impl<T> ProvenPage<T> {
    pub(super) fn page(&self) -> &T {
        match self {
            Self::Owned(page, _guard) => page,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Shared { page, .. } => page,
        }
    }
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
    #[cfg(not(target_arch = "wasm32"))]
    {
        meter.charge(WorkCounters::UNCHARGED)?;
        let key = crate::async_storage::DecodedCacheKey::new::<T>(id, limits);
        if let Some(cached) = store.decoded_cache_get(key)? {
            return Ok(ProvenPage::Shared {
                page: cached
                    .value
                    .downcast()
                    .map_err(|_| ObjectStoreError::Corrupt)?,
                _custody: cached.native_custody,
            });
        }
    }
    #[cfg(target_arch = "wasm32")]
    let _ = (store, id, limits);
    let (page, guard) = meter.decode_steps(decode)?;
    Ok(ProvenPage::Owned(page, guard))
}

#[cfg(all(test, feature = "memory", not(target_arch = "wasm32")))]
mod tests;
