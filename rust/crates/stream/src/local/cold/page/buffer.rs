//! Native page payloads and their ownership headers are allocated together,
//! before the read. Freezing never allocates after a completed physical read.
use super::*;
use crate::PreparedOwnedBytes;

enum Storage {
    Ordinary(Vec<u8>),
    Native(PreparedOwnedBytes<ReadAllocation>),
}

/// Only bounded mutations are exposed: an encoder cannot grow the allocation
/// admitted for this page, even when the allocator supplied spare capacity.
pub(in crate::local::cold) struct Bytes {
    storage: Storage,
    maximum: usize,
}

impl Bytes {
    fn slice(&self) -> &[u8] {
        match &self.storage {
            Storage::Ordinary(bytes) => bytes,
            Storage::Native(bytes) => bytes.buffer(),
        }
    }

    fn slice_mut(&mut self) -> &mut [u8] {
        match &mut self.storage {
            Storage::Ordinary(bytes) => bytes,
            Storage::Native(bytes) => bytes.writable(),
        }
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        match &self.storage {
            Storage::Ordinary(bytes) => bytes.capacity(),
            Storage::Native(bytes) => bytes.retained_capacity(),
        }
    }

    fn check_length(&self, length: usize) -> Result<(), LocalStreamError> {
        if length > self.maximum {
            return Err(LocalStreamError::InvalidLimits);
        }
        Ok(())
    }

    pub(in crate::local::cold) fn extend_from_slice(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), LocalStreamError> {
        let length = self
            .len()
            .checked_add(bytes.len())
            .ok_or(LocalStreamError::InvalidLimits)?;
        self.check_length(length)?;
        match &mut self.storage {
            Storage::Ordinary(output) => output.extend_from_slice(bytes),
            Storage::Native(output) => output
                .extend_within_capacity(bytes)
                .ok_or(LocalStreamError::InvalidLimits)?,
        }
        Ok(())
    }

    pub(in crate::local::cold) fn push(&mut self, byte: u8) -> Result<(), LocalStreamError> {
        self.extend_from_slice(&[byte])
    }

    pub(in crate::local::cold) fn resize(
        &mut self,
        length: usize,
        byte: u8,
    ) -> Result<(), LocalStreamError> {
        self.check_length(length)?;
        match &mut self.storage {
            Storage::Ordinary(bytes) => bytes.resize(length, byte),
            Storage::Native(bytes) => bytes
                .resize_within_capacity(length, byte)
                .ok_or(LocalStreamError::InvalidLimits)?,
        }
        Ok(())
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.slice()
    }
}

impl std::ops::DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slice_mut()
    }
}
pub(in crate::local::cold) struct Buffer {
    pub(in crate::local::cold) bytes: Bytes,
}

impl Buffer {
    pub(in crate::local::cold) fn new(
        length: usize,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        let bytes =
            if let Some(work) = work {
                let mut allocation = ReadAllocation::empty(&work.meter);
                let bytes = observe(
                    Some(work),
                    crate::native::vector(length, &work.meter, &mut allocation),
                )?;
                let prepared = PreparedOwnedBytes::new_buffer(bytes, allocation, &work.meter)
                    .map_err(|failure| match failure.into_error() {
                        crate::PreparedOwnedBytesError::Admission(error) => error,
                        crate::PreparedOwnedBytesError::NotFullyInitialized => {
                            std::io::ErrorKind::InvalidData.into()
                        }
                    });
                Storage::Native(observe(Some(work), prepared)?)
            } else {
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(length)
                    .map_err(|_| LocalStreamError::InvalidLimits)?;
                Storage::Ordinary(bytes)
            };
        Ok(Self {
            bytes: Bytes {
                storage: bytes,
                maximum: length,
            },
        })
    }

    pub(in crate::local::cold) fn freeze(self) -> Result<crate::OwnedBytes, LocalStreamError> {
        match self.bytes.storage {
            Storage::Ordinary(bytes) => Ok(bytes.into()),
            Storage::Native(bytes) => {
                let length = bytes.buffer().len();
                bytes.freeze(length).map_err(|_| LocalStreamError::Corrupt)
            }
        }
    }
}

#[cfg(test)]
mod tests;
