//! Immutable envelope descriptors reference existing record facts. Payloads
//! are written once; retry and envelope reads load only this envelope's batch.
use super::*;
use crate::{CommitId, CommittedEnvelope, CommittedMutation, Record, StreamPath};
use acyclic_native_runtime::ReadAllocation;

mod decode;
#[cfg(test)]
mod tests;
pub(super) use decode::read;

const TAG: u8 = 4;

struct Parts<'a> {
    path: &'a StreamPath,
    source: Option<&'a StreamPath>,
    start: u64,
    records: &'a [Record],
}

fn parts(mutation: &CommittedMutation) -> Result<Parts<'_>, LocalStreamError> {
    let (parts, tail) = match mutation {
        CommittedMutation::Append(append) => {
            if append.records.is_empty() || append.end != append.tail {
                return Err(LocalStreamError::Corrupt);
            }
            (
                Parts {
                    path: &append.path,
                    source: None,
                    start: append.start,
                    records: &append.records,
                },
                append.tail,
            )
        }
        CommittedMutation::Fork(fork) => {
            if fork.source == fork.destination {
                return Err(LocalStreamError::Corrupt);
            }
            (
                Parts {
                    path: &fork.destination,
                    source: Some(&fork.source),
                    start: fork.forked_at,
                    records: &fork.records,
                },
                fork.tail,
            )
        }
    };
    if parts.records.len() > crate::MAX_ITEMS
        || parts.start.checked_add(parts.records.len() as u64) != Some(tail)
    {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(parts)
}

fn encoded_len(envelope: &CommittedEnvelope) -> Result<usize, LocalStreamError> {
    if envelope.mutations.is_empty() || envelope.mutations.len() > crate::MAX_ITEMS {
        return Err(LocalStreamError::Corrupt);
    }
    let mut length: usize = 1 + 32 + 4;
    let mut previous = None;
    let mut time = None;
    for mutation in &envelope.mutations {
        let parts = parts(mutation)?;
        if previous.is_some_and(|previous| previous >= parts.path) {
            return Err(LocalStreamError::Corrupt);
        }
        previous = Some(parts.path);
        for (offset, record) in parts.records.iter().enumerate() {
            if record.commit_id != envelope.commit_id
                || record.sequence != parts.start + offset as u64
                || time.is_some_and(|time| time != record.committed_at_micros)
            {
                return Err(LocalStreamError::Corrupt);
            }
            time = Some(record.committed_at_micros);
        }
        let size = 1
            + 4
            + parts.path.as_str().len()
            + 8
            + 4
            + parts.records.len() * 52
            + parts.source.map_or(0, |source| 4 + source.as_str().len());
        length = length
            .checked_add(size)
            .ok_or(LocalStreamError::InvalidLimits)?;
    }
    if length > crate::MAX_COMMAND_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    Ok(length)
}

fn blob(bytes: &mut Vec<u8>, text: &str) -> Result<(), LocalStreamError> {
    bytes.extend_from_slice(
        &u32::try_from(text.len())
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(text.as_bytes());
    Ok(())
}

pub(super) fn append(
    io: &mut impl PageIo,
    envelope: &CommittedEnvelope,
    mut record_reference: impl FnMut(usize, usize) -> Result<PageRef, LocalStreamError>,
) -> Result<PageRef, LocalStreamError> {
    let length = encoded_len(envelope)?;
    let mut bytes = Buffer::new(length, io.work())?;
    page::codec(io.work(), length as u64, 0, true)?;
    let encoded = (|| {
        bytes.bytes.push(TAG);
        bytes.bytes.extend_from_slice(envelope.commit_id.as_bytes());
        bytes.bytes.extend_from_slice(
            &u32::try_from(envelope.mutations.len())
                .map_err(|_| LocalStreamError::InvalidLimits)?
                .to_le_bytes(),
        );
        for (position, mutation) in envelope.mutations.iter().enumerate() {
            let parts = parts(mutation)?;
            bytes.bytes.push(u8::from(parts.source.is_some()));
            blob(&mut bytes.bytes, parts.path.as_str())?;
            if let Some(source) = parts.source {
                blob(&mut bytes.bytes, source.as_str())?;
            }
            bytes.bytes.extend_from_slice(&parts.start.to_le_bytes());
            bytes.bytes.extend_from_slice(
                &u32::try_from(parts.records.len())
                    .map_err(|_| LocalStreamError::InvalidLimits)?
                    .to_le_bytes(),
            );
            for offset in 0..parts.records.len() {
                bytes
                    .bytes
                    .extend_from_slice(&record_reference(position, offset)?.encoded());
            }
        }
        Ok::<(), LocalStreamError>(())
    })();
    // The producer callback can fail after a physical prefix was copied.
    // Preserve the completed copy observation before returning that failure.
    page::codec(io.work(), bytes.bytes.len() as u64, 0, false)?;
    encoded?;
    if bytes.bytes.len() != length {
        return Err(LocalStreamError::Corrupt);
    }
    io.append(PageKind::Fact, &bytes.bytes)
}

/// Each partially decoded container co-owns its funding. Moving a nested
/// vector into its parent also transfers its original allocation guard.
pub(super) struct Funded<T> {
    pub(super) value: T,
    allocation: Option<ReadAllocation>,
}

impl<T> Funded<Vec<T>> {
    pub(super) fn vector(
        length: usize,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        if let Some(work) = work {
            let mut allocation = ReadAllocation::empty(&work.meter);
            let value = page::observe(
                Some(work),
                crate::native::vector(length, &work.meter, &mut allocation),
            )?;
            return Ok(Self {
                value,
                allocation: Some(allocation),
            });
        }
        let mut value = Vec::new();
        value
            .try_reserve_exact(length)
            .map_err(|_| LocalStreamError::InvalidLimits)?;
        Ok(Self {
            value,
            allocation: None,
        })
    }
}

impl<T> Funded<T> {
    pub(super) fn into_parts(self) -> (T, Option<ReadAllocation>) {
        (self.value, self.allocation)
    }

    pub(super) fn scalar(value: T, work: Option<&Arc<crate::memory::Work>>) -> Self {
        Self {
            value,
            allocation: work.map(|work| ReadAllocation::empty(&work.meter)),
        }
    }

    pub(super) fn map<U>(self, convert: impl FnOnce(T) -> U) -> Funded<U> {
        Funded {
            value: convert(self.value),
            allocation: self.allocation,
        }
    }
    pub(super) fn absorb<U>(&mut self, mut child: Funded<U>) -> Result<U, LocalStreamError> {
        if let Some(allocation) = &mut self.allocation {
            allocation.absorb(child.allocation.as_mut().ok_or(LocalStreamError::Corrupt)?)?;
        } else if child.allocation.is_some() {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(child.value)
    }
}
