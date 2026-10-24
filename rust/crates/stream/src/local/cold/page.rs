//! Immutable cold pages. Their physical address is part of their digest.
use super::*;
use acyclic_native_runtime::ReadAllocation;

const PAGE_MAGIC: &[u8; 8] = b"ACYCLPG1";
const PAGE_HEADER_BYTES: usize = 13;
pub(super) const HEADER_BYTES: usize = PAGE_HEADER_BYTES;
const PAGE_DOMAIN: &[u8] = b"acyclic.stream.cold.page/v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PageRef {
    pub(super) segment: u64,
    pub(super) offset: u64,
    pub(super) bytes: u32,
    pub(super) digest: [u8; 32],
}

impl PageRef {
    pub(super) fn precedes(self, parent: Self) -> bool {
        self.segment < parent.segment
            || (self.segment == parent.segment
                && self
                    .offset
                    .checked_add(u64::from(self.bytes))
                    .is_some_and(|end| end <= parent.offset))
    }

    pub(super) fn encoded(self) -> [u8; 52] {
        let mut output = [0; 52];
        output[..8].copy_from_slice(&self.segment.to_le_bytes());
        output[8..16].copy_from_slice(&self.offset.to_le_bytes());
        output[16..20].copy_from_slice(&self.bytes.to_le_bytes());
        output[20..].copy_from_slice(&self.digest);
        output
    }

    pub(super) fn decode(input: &mut &[u8]) -> Result<Self, LocalStreamError> {
        let (bytes, rest) = input
            .split_first_chunk::<52>()
            .ok_or(LocalStreamError::Corrupt)?;
        let segment = u64::from_le_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| LocalStreamError::Corrupt)?,
        );
        let offset = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| LocalStreamError::Corrupt)?,
        );
        let length = u32::from_le_bytes(
            bytes[16..20]
                .try_into()
                .map_err(|_| LocalStreamError::Corrupt)?,
        );
        let digest = bytes[20..]
            .try_into()
            .map_err(|_| LocalStreamError::Corrupt)?;
        checked_length(length)?;
        offset
            .checked_add(u64::from(length))
            .ok_or(LocalStreamError::Corrupt)?;
        *input = rest;
        Ok(Self {
            segment,
            offset,
            bytes: length,
            digest,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum PageKind {
    Leaf = 1,
    Branch = 2,
    Fact = 3,
    Root = 4,
}

impl TryFrom<u8> for PageKind {
    type Error = LocalStreamError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Leaf),
            2 => Ok(Self::Branch),
            3 => Ok(Self::Fact),
            4 => Ok(Self::Root),
            _ => Err(LocalStreamError::Corrupt),
        }
    }
}

/// Bytes are destroyed before the original allocation that funds them.
pub(super) struct Buffer {
    pub(super) bytes: Vec<u8>,
    _allocation: Option<ReadAllocation>,
}

impl Buffer {
    pub(super) fn new(
        length: usize,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        if let Some(work) = work {
            let mut allocation = ReadAllocation::empty(&work.meter);
            let bytes = observe(
                Some(work),
                crate::native::vector(length, &work.meter, &mut allocation),
            )?;
            return Ok(Self {
                bytes,
                _allocation: Some(allocation),
            });
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| LocalStreamError::InvalidLimits)?;
        Ok(Self {
            bytes,
            _allocation: None,
        })
    }

    fn freeze(
        mut self,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<crate::OwnedBytes, LocalStreamError> {
        let Some(allocation) = self._allocation.as_ref() else {
            return Ok(std::mem::take(&mut self.bytes).into());
        };
        let meter = Arc::clone(allocation.meter());
        observe(
            work,
            crate::OwnedBytes::from_vec_with_guard_in_place(
                &mut self.bytes,
                &mut self._allocation,
                &meter,
            ),
        )
    }
}

pub(super) struct Page {
    pub(super) kind: PageKind,
    encoded: Buffer,
}

impl Page {
    pub(super) fn body(&self) -> Result<&[u8], LocalStreamError> {
        self.encoded
            .bytes
            .get(PAGE_HEADER_BYTES..)
            .ok_or(LocalStreamError::Corrupt)
    }

    pub(super) fn into_body(
        self,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<crate::OwnedBytes, LocalStreamError> {
        let bytes = self.encoded.freeze(work)?;
        if bytes.len() < PAGE_HEADER_BYTES {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(bytes.slice(PAGE_HEADER_BYTES..))
    }
}

fn checked_length(length: u32) -> Result<usize, LocalStreamError> {
    usize::try_from(length)
        .ok()
        .filter(|length| {
            *length >= PAGE_HEADER_BYTES && *length - PAGE_HEADER_BYTES <= MAX_COMMAND_BYTES
        })
        .ok_or(LocalStreamError::Corrupt)
}

pub(super) fn observe<T>(
    work: Option<&Arc<crate::memory::Work>>,
    result: std::io::Result<T>,
) -> Result<T, LocalStreamError> {
    match work {
        Some(work) => work
            .record(result)
            .map_err(|_| LocalStreamError::Io(std::io::ErrorKind::Other.into())),
        None => result.map_err(Into::into),
    }
}

fn digest(segment: u64, offset: u64, bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PAGE_DOMAIN);
    hash.update(segment.to_le_bytes());
    hash.update(offset.to_le_bytes());
    hash.update(bytes);
    hash.finalize().into()
}

pub(super) fn codec(
    work: Option<&Arc<crate::memory::Work>>,
    copied: u64,
    hashed: u64,
    before: bool,
) -> Result<(), LocalStreamError> {
    if let Some(work) = work {
        let result = if before {
            work.meter.admit_codec(0, copied, hashed)
        } else {
            work.meter.record_codec(0, copied, hashed)
        };
        observe(Some(work), result)?;
    }
    Ok(())
}

fn read_chunk(
    work: Option<&Arc<crate::memory::Work>>,
    bytes: usize,
    operation: impl FnOnce() -> std::io::Result<usize>,
) -> Result<usize, LocalStreamError> {
    let Some(work) = work else {
        return operation().map_err(Into::into);
    };
    observe(Some(work), work.meter.check())?;
    let mut ticket = observe(
        Some(work),
        acyclic_native_runtime::NativeOperationReservation::new(
            Arc::clone(&work.meter),
            acyclic_native_runtime::NativeOperationKind::Read,
            1,
        ),
    )?;
    let mut payload = observe(
        Some(work),
        acyclic_native_runtime::NativeByteReservation::new(
            Arc::clone(&work.meter),
            acyclic_native_runtime::NativeOperationKind::Read,
            bytes,
        ),
    )?;
    let (physical, observed) = ticket.attempt(operation);
    let transferred = payload.record(physical.as_ref().copied().unwrap_or(0));
    // Observe every completed effect even when another observation fails. In
    // particular, late cancellation cannot hide a successful short read.
    let physical = observe(Some(work), physical);
    let observed = observe(Some(work), observed);
    let transferred = observe(Some(work), transferred);
    observed?;
    transferred?;
    let output = physical?;
    observe(Some(work), work.meter.check())?;
    Ok(output)
}

pub(super) struct PendingPage {
    encoded: Buffer,
    reference: PageRef,
    end: u64,
}

impl PendingPage {
    pub(super) fn write(
        self,
        file: &File,
        end: &mut u64,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<PageRef, LocalStreamError> {
        if *end != self.reference.offset {
            return Err(LocalStreamError::Corrupt);
        }
        observe(
            work,
            acyclic_native_runtime::write_all_at(
                file,
                *end,
                &self.encoded.bytes,
                work.map(|work| (work.meter.as_ref(), false)),
            ),
        )?;
        *end = self.end;
        Ok(self.reference)
    }
}

pub(super) fn prepare(
    segment: u64,
    offset: u64,
    kind: PageKind,
    body: &[u8],
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<PendingPage, LocalStreamError> {
    if body.len() > MAX_COMMAND_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    let length = body
        .len()
        .checked_add(PAGE_HEADER_BYTES)
        .ok_or(LocalStreamError::InvalidLimits)?;
    let bytes = u32::try_from(length).map_err(|_| LocalStreamError::InvalidLimits)?;
    let next = offset
        .checked_add(u64::from(bytes))
        .ok_or(LocalStreamError::InvalidLimits)?;
    let mut buffer = Buffer::new(length, work)?;
    let hash_bytes = length as u64 + PAGE_DOMAIN.len() as u64 + 16;
    codec(work, length as u64, hash_bytes, true)?;
    buffer.bytes.extend_from_slice(PAGE_MAGIC);
    buffer.bytes.push(kind as u8);
    buffer.bytes.extend_from_slice(&bytes.to_le_bytes());
    buffer.bytes.extend_from_slice(body);
    let hash = digest(segment, offset, &buffer.bytes);
    codec(work, length as u64, hash_bytes, false)?;
    let reference = PageRef {
        segment,
        offset,
        bytes,
        digest: hash,
    };
    Ok(PendingPage {
        encoded: buffer,
        reference,
        end: next,
    })
}

pub(super) fn append(
    file: &File,
    segment: u64,
    end: &mut u64,
    kind: PageKind,
    body: &[u8],
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<PageRef, LocalStreamError> {
    prepare(segment, *end, kind, body, work)?.write(file, end, work)
}

pub(super) fn read(
    file: &File,
    reference: PageRef,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<Page, LocalStreamError> {
    let length = checked_length(reference.bytes)?;
    let mut encoded = Buffer::new(length, work)?;
    encoded.bytes.resize(length, 0);
    read_exact(file, reference.offset, &mut encoded.bytes, work)?;
    let hash_bytes = length as u64 + PAGE_DOMAIN.len() as u64 + 16;
    codec(work, 0, hash_bytes, true)?;
    let found = digest(reference.segment, reference.offset, &encoded.bytes);
    codec(work, 0, hash_bytes, false)?;
    if found != reference.digest
        || !encoded.bytes.starts_with(PAGE_MAGIC)
        || encoded.bytes.get(9..13) != Some(reference.bytes.to_le_bytes().as_slice())
    {
        return Err(LocalStreamError::Corrupt);
    }
    let kind = PageKind::try_from(*encoded.bytes.get(8).ok_or(LocalStreamError::Corrupt)?)?;
    Ok(Page { kind, encoded })
}

/// Preserve each physical prefix and original failure for fixed metadata and
/// page bodies through the same read boundary.
pub(super) fn read_exact(
    file: &File,
    start: u64,
    bytes: &mut [u8],
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<(), LocalStreamError> {
    let mut filled = 0;
    while filled < bytes.len() {
        let offset = start
            .checked_add(filled as u64)
            .ok_or(LocalStreamError::Corrupt)?;
        let destination = bytes.get_mut(filled..).ok_or(LocalStreamError::Corrupt)?;
        let count = read_chunk(work, destination.len(), || {
            acyclic_native_runtime::read_at(file, offset, destination)
        })?;
        if count == 0 {
            return observe(
                work,
                Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            );
        }
        filled = filled
            .checked_add(count)
            .filter(|end| *end <= bytes.len())
            .ok_or(LocalStreamError::Corrupt)?;
    }
    Ok(())
}
