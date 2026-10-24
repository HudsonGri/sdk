//! Fixed metadata selects one segment directly; earlier segments are never
//! walked to discover the current root.
use super::*;

const MAGIC: &[u8; 8] = b"ACYCLHD1";
const DOMAIN: &[u8] = b"acyclic.stream.cold.locator/v1\0";
const PREFIX: usize = 16 + 32;
const SLOT: usize = 8 + 32;
pub(super) const BYTES: usize = PREFIX + 2 * SLOT;

fn checksum(ordinal: u8, body: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(DOMAIN);
    hash.update([ordinal]);
    hash.update(body);
    hash.finalize().into()
}

fn hash_bytes() -> u64 {
    (DOMAIN.len() + 1 + 16 + 2 * (DOMAIN.len() + 1 + 8)) as u64
}

fn prefix(maximum: u64) -> [u8; PREFIX] {
    let mut bytes = [0; PREFIX];
    let (body, digest) = bytes.split_at_mut(16);
    let (magic, maximum_bytes) = body.split_at_mut(8);
    magic.copy_from_slice(MAGIC);
    maximum_bytes.copy_from_slice(&maximum.to_le_bytes());
    digest.copy_from_slice(&checksum(2, body));
    bytes
}

fn slot(number: u64, ordinal: u8) -> [u8; SLOT] {
    let mut bytes = [0; SLOT];
    let (body, digest) = bytes.split_at_mut(8);
    body.copy_from_slice(&number.to_le_bytes());
    digest.copy_from_slice(&checksum(ordinal, body));
    bytes
}

pub(super) fn create(file: &File, maximum: u64) -> Result<(), LocalStreamError> {
    let mut bytes = [0; BYTES];
    bytes
        .get_mut(..PREFIX)
        .ok_or(LocalStreamError::Corrupt)?
        .copy_from_slice(&prefix(maximum));
    bytes
        .get_mut(PREFIX..PREFIX + SLOT)
        .ok_or(LocalStreamError::Corrupt)?
        .copy_from_slice(&slot(0, 0));
    acyclic_native_runtime::write_all_at(file, 0, &bytes, None)?;
    Ok(())
}

pub(super) fn load(
    file: &File,
    maximum: u64,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<u64, LocalStreamError> {
    let mut bytes = [0; BYTES];
    page::read_exact(file, 0, &mut bytes, work)?;
    page::codec(work, 0, hash_bytes(), true)?;
    let result = decode(&bytes, maximum);
    page::codec(work, 0, hash_bytes(), false)?;
    result
}

fn decode(bytes: &[u8; BYTES], maximum: u64) -> Result<u64, LocalStreamError> {
    let (actual_prefix, slots) = bytes.split_at(PREFIX);
    // Compute all three fixed hashes before validation, including on bad input.
    let expected_prefix = prefix(maximum);
    let (first, second) = slots.split_at(SLOT);
    let first = decode_slot(first, 0);
    let second = decode_slot(second, 1);
    if actual_prefix != expected_prefix {
        return Err(LocalStreamError::Corrupt);
    }
    match (first?, second?) {
        (Some(first), Some(second)) if first.abs_diff(second) != 1 => {
            Err(LocalStreamError::Corrupt)
        }
        (Some(first), Some(second)) => Ok(first.max(second)),
        (Some(number), None) | (None, Some(number)) => Ok(number),
        (None, None) => Err(LocalStreamError::Corrupt),
    }
}

fn decode_slot(bytes: &[u8], ordinal: u8) -> Result<Option<u64>, LocalStreamError> {
    let (body, digest) = bytes.split_at_checked(8).ok_or(LocalStreamError::Corrupt)?;
    if digest != checksum(ordinal, body) {
        return Ok(None);
    }
    let number = u64::from_le_bytes(body.try_into().map_err(|_| LocalStreamError::Corrupt)?);
    if u8::from(number & 1 != 0) != ordinal {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(Some(number))
}

pub(super) fn advance(
    file: &File,
    from: u64,
    to: u64,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<(), LocalStreamError> {
    if from.checked_add(1) != Some(to) {
        return Err(LocalStreamError::InvalidLimits);
    }
    let ordinal = u8::from(to & 1 != 0);
    let hashed = (DOMAIN.len() + 1 + 8) as u64;
    page::codec(work, SLOT as u64, hashed, true)?;
    let bytes = slot(to, ordinal);
    page::codec(work, SLOT as u64, hashed, false)?;
    page::observe(
        work,
        acyclic_native_runtime::write_all_at(
            file,
            (PREFIX + usize::from(ordinal) * SLOT) as u64,
            &bytes,
            work.map(|work| (work.meter.as_ref(), false)),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locator_binds_policy_position_and_consecutive_segment_numbers()
    -> Result<(), Box<dyn std::error::Error>> {
        let file = tempfile::tempfile()?;
        create(&file, 65_536)?;
        for number in 0..1_000 {
            assert_eq!(load(&file, 65_536, None)?, number);
            if number < 999 {
                advance(&file, number, number + 1, None)?;
            }
        }
        assert!(load(&file, 65_537, None).is_err());
        assert!(advance(&file, 999, 1_001, None).is_err());
        let mut bytes = [0; BYTES];
        page::read_exact(&file, 0, &mut bytes, None)?;
        for position in 0..PREFIX {
            let mut corrupt = bytes;
            *corrupt.get_mut(position).ok_or("bad position")? ^= 1;
            assert!(decode(&corrupt, 65_536).is_err());
        }
        let (first, second) = bytes
            .get_mut(PREFIX..)
            .ok_or("no slots")?
            .split_at_mut(SLOT);
        first.copy_from_slice(&slot(800, 0));
        second.copy_from_slice(&slot(999, 1));
        let max_only = decode_slot(first, 0)?
            .ok_or("missing first slot")?
            .max(decode_slot(second, 1)?.ok_or("missing second slot")?);
        assert!(decode(&bytes, 65_536).is_err());
        // A max-number-only control would accept the nonconsecutive pair.
        assert_eq!(max_only, 999);
        first_slot_control(&mut bytes)?;
        Ok(())
    }

    fn first_slot_control(bytes: &mut [u8; BYTES]) -> Result<(), LocalStreamError> {
        let slots = bytes.get_mut(PREFIX..).ok_or(LocalStreamError::Corrupt)?;
        let (first, second) = slots.split_at_mut(SLOT);
        first.copy_from_slice(&slot(998, 0));
        second.copy_from_slice(&slot(999, 1));
        assert_eq!(decode(bytes, 65_536)?, 999);
        let slots = bytes.get_mut(PREFIX..).ok_or(LocalStreamError::Corrupt)?;
        let (first, second) = slots.split_at_mut(SLOT);
        first.swap_with_slice(second);
        assert!(decode(bytes, 65_536).is_err());
        Ok(())
    }
}
