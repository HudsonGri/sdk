//! Read credit and reusable payload/owner storage issued before an effect.
use super::*;
use crate::ReusableOwnedBytes;
use acyclic_native_runtime::{
    NativeByteReservation, NativeCodecReservation, NativeCodecWork, NativeOperationKind,
    NativeOperationReservation, NativeReadMeter,
};

type Meter = Arc<dyn NativeReadMeter>;

struct ReadBuffer {
    bytes: ReusableOwnedBytes<ReadAllocation>,
    maximum: usize,
}

/// This plan owns real reusable buffers and original-ledger reservations. An
/// escaped page fences its slot until every immutable view is released.
pub(in crate::local::cold) struct PreparedRead {
    buffers: super::super::envelope::Funded<Vec<ReadBuffer>>,
    operations: NativeOperationReservation<Meter>,
    payload: NativeByteReservation<Meter>,
    codec: NativeCodecReservation<Meter>,
    remaining_attempts: u64,
    remaining_bytes: usize,
    work: Arc<crate::memory::Work>,
}

impl PreparedRead {
    pub(in crate::local::cold) fn new(
        work: Arc<crate::memory::Work>,
        capacities: &[usize],
        maximum_bytes: usize,
        maximum_attempts: u64,
    ) -> Result<Self, LocalStreamError> {
        if capacities.is_empty() || maximum_attempts == 0 {
            return Err(LocalStreamError::InvalidLimits);
        }
        let mut buffers = super::super::envelope::Funded::vector(capacities.len(), Some(&work))?;
        for &capacity in capacities {
            if !(PAGE_HEADER_BYTES..=PAGE_HEADER_BYTES + MAX_COMMAND_BYTES).contains(&capacity) {
                return Err(LocalStreamError::InvalidLimits);
            }
            let mut allocation = ReadAllocation::empty(&work.meter);
            let mut bytes = observe(
                Some(&work),
                crate::native::vector(capacity, &work.meter, &mut allocation),
            )?;
            let initialized = bytes.capacity();
            codec(Some(&work), initialized as u64, 0, true)?;
            bytes.resize(initialized, 0);
            codec(Some(&work), initialized as u64, 0, false)?;
            let buffer =
                ReusableOwnedBytes::new(bytes, allocation, &work.meter).map_err(|failure| {
                    match failure.into_error() {
                        crate::PreparedOwnedBytesError::Admission(error) => error,
                        crate::PreparedOwnedBytesError::NotFullyInitialized => {
                            std::io::ErrorKind::InvalidData.into()
                        }
                    }
                });
            buffers.value.push(ReadBuffer {
                bytes: observe(Some(&work), buffer)?,
                maximum: capacity,
            });
        }
        let overhead = maximum_attempts
            .checked_mul((PAGE_DOMAIN.len() + 16) as u64)
            .and_then(|overhead| overhead.checked_add(maximum_bytes as u64))
            .ok_or(LocalStreamError::InvalidLimits)?;
        let operations = observe(
            Some(&work),
            NativeOperationReservation::new(
                Arc::clone(&work.meter),
                NativeOperationKind::Read,
                maximum_attempts,
            ),
        )?;
        let payload = observe(
            Some(&work),
            NativeByteReservation::new(
                Arc::clone(&work.meter),
                NativeOperationKind::Read,
                maximum_bytes,
            ),
        )?;
        let codec = observe(
            Some(&work),
            NativeCodecReservation::new(
                Arc::clone(&work.meter),
                NativeCodecWork {
                    hashed: overhead,
                    ..NativeCodecWork::default()
                },
            ),
        )?;
        observe(Some(&work), work.meter.check())?;
        Ok(Self {
            buffers,
            operations,
            payload,
            codec,
            remaining_attempts: maximum_attempts,
            remaining_bytes: maximum_bytes,
            work,
        })
    }

    pub(in crate::local::cold) fn read(
        &mut self,
        file: &File,
        reference: PageRef,
    ) -> Result<Page, LocalStreamError> {
        self.read_with(reference, |offset, bytes| {
            acyclic_native_runtime::read_at(file, offset, bytes)
        })
    }

    pub(in crate::local::cold) fn read_prepared(
        &mut self,
        file: &acyclic_native_runtime::PreparedFileAccess<'_>,
        reference: PageRef,
    ) -> Result<Page, LocalStreamError> {
        self.read_with(reference, |offset, bytes| file.read_at(offset, bytes))
    }

    fn read_with(
        &mut self,
        reference: PageRef,
        mut read: impl FnMut(u64, &mut [u8]) -> std::io::Result<usize>,
    ) -> Result<Page, LocalStreamError> {
        let length = checked_length(reference.bytes)?;
        if length > self.remaining_bytes {
            return Err(LocalStreamError::InvalidLimits);
        }
        let buffer = self
            .buffers
            .value
            .iter_mut()
            .find_map(|buffer| {
                if buffer.maximum >= length && buffer.bytes.try_writable().is_some() {
                    Some(&mut buffer.bytes)
                } else {
                    None
                }
            })
            .ok_or(LocalStreamError::InvalidLimits)?;
        let bytes = buffer
            .try_writable()
            .and_then(|bytes| bytes.get_mut(..length))
            .ok_or(LocalStreamError::Corrupt)?;
        let mut filled = 0;
        while filled < length {
            self.remaining_attempts = self
                .remaining_attempts
                .checked_sub(1)
                .ok_or(LocalStreamError::InvalidLimits)?;
            let offset = reference
                .offset
                .checked_add(filled as u64)
                .ok_or(LocalStreamError::Corrupt)?;
            let destination = bytes.get_mut(filled..).ok_or(LocalStreamError::Corrupt)?;
            let (physical, observed) = self.operations.attempt(|| read(offset, destination));
            let count = physical.as_ref().copied().unwrap_or(0);
            let transferred = self.payload.record_partial(count);
            let physical = observe(Some(&self.work), physical);
            let observed = observe(Some(&self.work), observed);
            let transferred = observe(Some(&self.work), transferred);
            self.remaining_bytes = self
                .remaining_bytes
                .checked_sub(count)
                .ok_or(LocalStreamError::Corrupt)?;
            observed?;
            transferred?;
            let count = physical?;
            if count == 0 {
                return observe(
                    Some(&self.work),
                    Err(std::io::ErrorKind::UnexpectedEof.into()),
                );
            }
            filled = filled
                .checked_add(count)
                .filter(|filled| *filled <= length)
                .ok_or(LocalStreamError::Corrupt)?;
        }
        let verified = verify(reference, bytes);
        // Each hashed page consumed at least one reserved operation and exactly
        // its encoded length in physical bytes, so this credit cannot overrun.
        observe(
            Some(&self.work),
            self.codec.record(NativeCodecWork {
                hashed: length as u64 + PAGE_DOMAIN.len() as u64 + 16,
                ..NativeCodecWork::default()
            }),
        )?;
        let kind = verified?;
        Ok(Page {
            kind,
            encoded: buffer.snapshot(length).ok_or(LocalStreamError::Corrupt)?,
        })
    }
}

#[cfg(test)]
mod tests;
