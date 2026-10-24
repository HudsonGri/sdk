//! Retry identities and immutable envelopes share the disk point index with
//! path heads. Each fact validates its canonical identity after the hash lookup.
use super::*;
use crate::{CommitId, IdempotencyKey};
use root::Root;

#[cfg(test)]
mod tests;

const TAG: u8 = 5;
const DOMAIN: &[u8] = b"acyclic.stream.cold.retained-key/v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Family {
    Append,
    Fork,
    Commit,
}

impl Family {
    fn byte(self) -> u8 {
        match self {
            Self::Append => 0,
            Self::Fork => 1,
            Self::Commit => 2,
        }
    }

    fn decode(byte: u8) -> Result<Self, LocalStreamError> {
        match byte {
            0 => Ok(Self::Append),
            1 => Ok(Self::Fork),
            2 => Ok(Self::Commit),
            _ => Err(LocalStreamError::Corrupt),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Binding {
    pub(super) family: Family,
    pub(super) request_digest: [u8; 32],
    pub(super) outcome: PageRef,
}

fn key(
    kind: u8,
    identity: &[u8],
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<[u8; 32], LocalStreamError> {
    let length = DOMAIN.len() + 1 + 8 + identity.len();
    page::codec(work, 0, length as u64, true)?;
    let mut hash = Sha256::new();
    hash.update(DOMAIN);
    hash.update([kind]);
    hash.update((identity.len() as u64).to_le_bytes());
    hash.update(identity);
    let result = hash.finalize().into();
    page::codec(work, 0, length as u64, false)?;
    Ok(result)
}

pub(super) fn get(
    io: &mut impl PageRead,
    state: Root,
    identity: &IdempotencyKey,
) -> Result<Option<Binding>, LocalStreamError> {
    let key = key(0, identity.as_bytes(), io.work())?;
    let Some(reference) = index::get(io, state.points, &key)? else {
        return Ok(None);
    };
    let page = io.read(reference)?;
    if page.kind != PageKind::Fact {
        return Err(LocalStreamError::Corrupt);
    }
    let mut bytes = page.body()?;
    let (header, rest) = bytes
        .split_first_chunk::<4>()
        .ok_or(LocalStreamError::Corrupt)?;
    let [tag, family, low, high] = *header;
    if tag != TAG {
        return Err(LocalStreamError::Corrupt);
    }
    let family = Family::decode(family)?;
    let (stored, rest) = rest
        .split_at_checked(usize::from(u16::from_le_bytes([low, high])))
        .ok_or(LocalStreamError::Corrupt)?;
    if stored != identity.as_bytes() {
        return Err(LocalStreamError::Corrupt);
    }
    let (digest, rest) = rest
        .split_first_chunk::<32>()
        .ok_or(LocalStreamError::Corrupt)?;
    let request_digest = *digest;
    bytes = rest;
    let outcome = PageRef::decode(&mut bytes)?;
    if !bytes.is_empty() || !outcome.precedes(reference) {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(Some(Binding {
        family,
        request_digest,
        outcome,
    }))
}

pub(super) fn put(
    io: &mut impl PageIo,
    state: &mut Root,
    identity: &IdempotencyKey,
    binding: Binding,
) -> Result<(), LocalStreamError> {
    // Account-scoped retry identities are immutable across request families.
    // A replay never writes another fact; replacing an outcome is impossible.
    if let Some(existing) = get(io, *state, identity)? {
        return if existing == binding {
            Ok(())
        } else {
            Err(LocalStreamError::Corrupt)
        };
    }
    let length = 4 + identity.as_bytes().len() + 32 + 52;
    let mut bytes = Buffer::new(length, io.work())?;
    let key = key(0, identity.as_bytes(), io.work())?;
    page::codec(io.work(), length as u64, 0, true)?;
    bytes.bytes.extend_from_slice(&[TAG, binding.family.byte()]);
    bytes.bytes.extend_from_slice(
        &u16::try_from(identity.as_bytes().len())
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes(),
    );
    bytes.bytes.extend_from_slice(identity.as_bytes());
    bytes.bytes.extend_from_slice(&binding.request_digest);
    bytes.bytes.extend_from_slice(&binding.outcome.encoded());
    page::codec(io.work(), length as u64, 0, false)?;
    let reference = io.append(PageKind::Fact, &bytes.bytes)?;
    if !binding.outcome.precedes(reference) {
        return Err(LocalStreamError::Corrupt);
    }
    state.points = Some(index::put(io, state.points, &key, reference)?);
    Ok(())
}

pub(super) fn put_envelope(
    io: &mut impl PageIo,
    state: &mut Root,
    commit: CommitId,
    reference: PageRef,
) -> Result<(), LocalStreamError> {
    let key = key(1, commit.as_bytes(), io.work())?;
    if let Some(existing) = index::get(io, state.points, &key)? {
        return if existing == reference {
            Ok(())
        } else {
            Err(LocalStreamError::Corrupt)
        };
    }
    state.points = Some(index::put(io, state.points, &key, reference)?);
    Ok(())
}

pub(super) fn envelope(
    io: &mut impl PageRead,
    state: Root,
    commit: CommitId,
) -> Result<Option<envelope::Funded<crate::CommittedEnvelope>>, LocalStreamError> {
    let key = key(1, commit.as_bytes(), io.work())?;
    let Some(reference) = index::get(io, state.points, &key)? else {
        return Ok(None);
    };
    // The descriptor authenticates the full commit identity before following
    // its record references, including a forged point-index binding.
    envelope::read(io, reference, commit).map(Some)
}

pub(super) fn observe(
    io: &mut impl PageRead,
    state: Root,
    identity: &IdempotencyKey,
) -> Result<Option<envelope::Funded<crate::IdempotencyObservation>>, LocalStreamError> {
    let Some(binding) = get(io, state, identity)? else {
        return Ok(None);
    };
    let outcome = outcome::read(io, binding.outcome)?;
    let family = match &outcome.value {
        crate::IdempotencyOutcome::Append(_) => Family::Append,
        crate::IdempotencyOutcome::Fork(_) => Family::Fork,
        crate::IdempotencyOutcome::Commit(_) => Family::Commit,
    };
    if family != binding.family {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(Some(outcome.map(|outcome| crate::IdempotencyObservation {
        idempotency_key: identity.clone(),
        request_digest: binding.request_digest,
        outcome,
    })))
}
