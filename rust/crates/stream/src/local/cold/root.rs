//! Fixed-size publication state. A root names its indexes directly, so opening
//! it does not walk earlier publications or rebuild an in-memory projection.
use super::*;

const STATE_BYTES: usize = 16 + 32 + 2 * 53;
pub(super) const ROOT_BYTES: u64 = (STATE_BYTES + page::HEADER_BYTES) as u64;
const SLOT_BYTES: usize = 8 + 52 + 32;
const SLOT_DOMAIN: &[u8] = b"acyclic.stream.cold.root-slot/v1\0";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Root {
    pub(super) decision: u64,
    pub(super) committed_at_micros: u64,
    pub(super) hierarchy_version: crate::CommitId,
    pub(super) points: Option<PageRef>,
    pub(super) children: Option<PageRef>,
}

fn put_optional(destination: &mut [u8], reference: Option<PageRef>) {
    if let Some(reference) = reference {
        let (tag, rest) = destination.split_at_mut(1);
        tag.copy_from_slice(&[1]);
        rest.copy_from_slice(&reference.encoded());
    }
}

fn optional(input: &mut &[u8], parent: PageRef) -> Result<Option<PageRef>, LocalStreamError> {
    let (bytes, rest) = input
        .split_first_chunk::<53>()
        .ok_or(LocalStreamError::Corrupt)?;
    *input = rest;
    match bytes.split_first() {
        Some((&0, rest)) if rest.iter().all(|byte| *byte == 0) => Ok(None),
        Some((&1, mut rest)) => {
            let reference = PageRef::decode(&mut rest)?;
            if !rest.is_empty() || !reference.precedes(parent) {
                return Err(LocalStreamError::Corrupt);
            }
            Ok(Some(reference))
        }
        _ => Err(LocalStreamError::Corrupt),
    }
}

impl Root {
    pub(super) fn append(self, io: &mut impl PageIo) -> Result<PageRef, LocalStreamError> {
        page::codec(io.work(), STATE_BYTES as u64, 0, true)?;
        let mut bytes = [0; STATE_BYTES];
        let (decision, rest) = bytes.split_at_mut(8);
        decision.copy_from_slice(&self.decision.to_le_bytes());
        let (time, rest) = rest.split_at_mut(8);
        time.copy_from_slice(&self.committed_at_micros.to_le_bytes());
        let (version, rest) = rest.split_at_mut(32);
        version.copy_from_slice(self.hierarchy_version.as_bytes());
        let (points, children) = rest.split_at_mut(53);
        put_optional(points, self.points);
        put_optional(children, self.children);
        page::codec(io.work(), STATE_BYTES as u64, 0, false)?;
        let reference = io.append(PageKind::Root, &bytes)?;
        if [self.points, self.children]
            .into_iter()
            .flatten()
            .any(|child| !child.precedes(reference))
        {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(reference)
    }

    pub(super) fn load(
        io: &mut impl PageRead,
        reference: PageRef,
    ) -> Result<Self, LocalStreamError> {
        let page = io.read(reference)?;
        if page.kind != PageKind::Root {
            return Err(LocalStreamError::Corrupt);
        }
        let mut input = page.body()?;
        let (decision, rest) = input
            .split_first_chunk::<8>()
            .ok_or(LocalStreamError::Corrupt)?;
        let decision = u64::from_le_bytes(*decision);
        let (time, rest) = rest
            .split_first_chunk::<8>()
            .ok_or(LocalStreamError::Corrupt)?;
        let committed_at_micros = u64::from_le_bytes(*time);
        let (version, rest) = rest
            .split_first_chunk::<32>()
            .ok_or(LocalStreamError::Corrupt)?;
        let hierarchy_version = crate::CommitId::from_bytes(*version);
        input = rest;
        let points = optional(&mut input, reference)?;
        let children = optional(&mut input, reference)?;
        if !input.is_empty() {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(Self {
            decision,
            committed_at_micros,
            hierarchy_version,
            points,
            children,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Slot {
    pub(super) revision: u64,
    pub(super) root: PageRef,
}

fn checksum(segment: u64, ordinal: u8, body: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(SLOT_DOMAIN);
    hash.update(segment.to_le_bytes());
    hash.update([ordinal]);
    hash.update(body);
    hash.finalize().into()
}

impl Slot {
    pub(super) const fn hash_bytes() -> u64 {
        (SLOT_DOMAIN.len() + 8 + 1 + SLOT_BYTES - 32) as u64
    }

    pub(super) fn encoded(self, segment: u64, ordinal: u8) -> [u8; SLOT_BYTES] {
        let mut bytes = [0; SLOT_BYTES];
        let (body, digest) = bytes.split_at_mut(SLOT_BYTES - 32);
        let (revision, reference) = body.split_at_mut(8);
        revision.copy_from_slice(&self.revision.to_le_bytes());
        reference.copy_from_slice(&self.root.encoded());
        digest.copy_from_slice(&checksum(segment, ordinal, body));
        bytes
    }

    /// A checksum-invalid slot is an interrupted overwrite. The selected valid
    /// slot must still load its root successfully; callers may never fall back
    /// to another slot after that root or its referenced data fails validation.
    pub(super) fn decode(
        bytes: &[u8; SLOT_BYTES],
        segment: u64,
        ordinal: u8,
    ) -> Result<Option<Self>, LocalStreamError> {
        let (body, found) = bytes.split_at(SLOT_BYTES - 32);
        if found != checksum(segment, ordinal, body) {
            return Ok(None);
        }
        let (revision, mut reference) = body
            .split_first_chunk::<8>()
            .ok_or(LocalStreamError::Corrupt)?;
        let revision = u64::from_le_bytes(*revision);
        let root = PageRef::decode(&mut reference)?;
        if !reference.is_empty() || root.segment > segment {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(Some(Self { revision, root }))
    }

    pub(super) fn select(
        left: Option<Self>,
        right: Option<Self>,
    ) -> Result<Self, LocalStreamError> {
        match (left, right) {
            (Some(left), Some(right))
                if left.revision == right.revision && left.root != right.root =>
            {
                Err(LocalStreamError::Corrupt)
            }
            (Some(left), Some(right)) => Ok(if left.revision >= right.revision {
                left
            } else {
                right
            }),
            (Some(slot), None) | (None, Some(slot)) => Ok(slot),
            (None, None) => Err(LocalStreamError::Corrupt),
        }
    }
}

#[cfg(test)]
mod tests;
