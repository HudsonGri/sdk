use super::*;

const MAGIC: &[u8; 8] = b"ACYCLSG1";
const DOMAIN: &[u8] = b"acyclic.stream.cold.segment/v1\0";
pub(super) const PREFIX_BYTES: usize = 8 + 8 + 32;
pub(super) const SLOT_BYTES: usize = 8 + 52 + 32;
pub(super) const HEADER_BYTES: usize = PREFIX_BYTES + 2 * SLOT_BYTES;

pub(super) fn hash_bytes(slots: u64) -> u64 {
    (DOMAIN.len() + 16) as u64 + slots * Slot::hash_bytes()
}

fn digest(prefix: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(DOMAIN);
    hash.update(prefix);
    hash.finalize().into()
}

pub(super) fn encode(
    number: u64,
    inherited: Option<Slot>,
) -> Result<[u8; HEADER_BYTES], LocalStreamError> {
    let mut bytes = [0; HEADER_BYTES];
    let (prefix, slots) = bytes.split_at_mut(PREFIX_BYTES);
    let (body, checksum) = prefix.split_at_mut(16);
    let (magic, encoded_number) = body.split_at_mut(8);
    magic.copy_from_slice(MAGIC);
    encoded_number.copy_from_slice(&number.to_le_bytes());
    checksum.copy_from_slice(&digest(body));
    if let Some(slot) = inherited {
        let ordinal = u8::from(slot.revision & 1 != 0);
        let start = usize::from(ordinal) * SLOT_BYTES;
        slots
            .get_mut(start..start + SLOT_BYTES)
            .ok_or(LocalStreamError::Corrupt)?
            .copy_from_slice(&slot.encoded(number, ordinal));
    }
    Ok(bytes)
}

pub(super) fn decode(bytes: &[u8; HEADER_BYTES], number: u64) -> Result<Slot, LocalStreamError> {
    let (prefix, slots) = bytes.split_at(PREFIX_BYTES);
    let (body, checksum) = prefix.split_at(16);
    let prefix_digest = digest(body);
    let (first, second) = slots.split_at(SLOT_BYTES);
    let first = Slot::decode(
        first.try_into().map_err(|_| LocalStreamError::Corrupt)?,
        number,
        0,
    );
    let second = Slot::decode(
        second.try_into().map_err(|_| LocalStreamError::Corrupt)?,
        number,
        1,
    );
    if !body.starts_with(MAGIC)
        || body.get(8..) != Some(number.to_le_bytes().as_slice())
        || checksum != prefix_digest
    {
        return Err(LocalStreamError::Corrupt);
    }
    Slot::select(first?, second?)
}
