//! Bounded record facts. Decoding retains one authenticated page allocation;
//! values, keys and attribute values are slices of that original owner.
use super::*;
use crate::{Attribute, OwnedBytes, Record, RecordHeaders};

mod controls;
mod layout;
mod prepared;
use layout::Layout;
pub(super) use prepared::PreparedRecord;

const RECORD_TAG: u8 = 1;
const FIXED_BYTES: usize = 1 + 8 + 8 + 32 + 4 + 2 + 2;

fn blob(output: &mut page::BufferBytes, bytes: &[u8]) -> Result<(), LocalStreamError> {
    let length = u32::try_from(bytes.len()).map_err(|_| LocalStreamError::InvalidLimits)?;
    output.extend_from_slice(&length.to_le_bytes())?;
    output.extend_from_slice(bytes)?;
    Ok(())
}

pub(super) fn append(io: &mut impl PageIo, record: &Record) -> Result<PageRef, LocalStreamError> {
    let payload = record
        .value
        .len()
        .saturating_add(record.headers.payload_bytes());
    if payload > crate::MAX_RECORD_BYTES || record.sequence == u64::MAX {
        return Err(LocalStreamError::InvalidLimits);
    }
    let keys = record.headers.keys();
    let attributes = record.headers.attributes();
    let length = FIXED_BYTES + payload + keys.len() * 4 + attributes.len() * 8;
    let mut output = Buffer::new(length, io.work())?;
    page::codec(io.work(), length as u64, 0, true)?;
    output.bytes.push(RECORD_TAG)?;
    output
        .bytes
        .extend_from_slice(&record.sequence.to_le_bytes())?;
    output
        .bytes
        .extend_from_slice(&record.committed_at_micros.to_le_bytes())?;
    output
        .bytes
        .extend_from_slice(record.commit_id.as_bytes())?;
    blob(&mut output.bytes, &record.value)?;
    output.bytes.extend_from_slice(
        &u16::try_from(keys.len())
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes(),
    )?;
    for key in keys {
        blob(&mut output.bytes, key)?;
    }
    output.bytes.extend_from_slice(
        &u16::try_from(attributes.len())
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes(),
    )?;
    for attribute in attributes {
        blob(&mut output.bytes, attribute.name.as_bytes())?;
        blob(&mut output.bytes, attribute.value)?;
    }
    page::codec(io.work(), length as u64, 0, false)?;
    if output.bytes.len() != length {
        return Err(LocalStreamError::Corrupt);
    }
    io.append(PageKind::Fact, &output.bytes)
}

pub(super) fn read(io: &mut impl PageRead, reference: PageRef) -> Result<Record, LocalStreamError> {
    let work = io.work().cloned();
    let page = io.read(reference)?;
    if page.kind != PageKind::Fact {
        return Err(LocalStreamError::Corrupt);
    }
    let bytes = page.into_body()?;
    let layout = Layout::read(&bytes)?;
    let headers = controls::headers(&bytes, &layout, work.as_ref())?;
    let record = Record {
        sequence: layout.sequence,
        committed_at_micros: layout.time,
        commit_id: layout.commit,
        value: layout.value.slice(&bytes),
        headers,
    };
    crate::changes::validate(&record.value, &record.headers)
        .map_err(|_| LocalStreamError::Corrupt)?;
    Ok(record)
}

#[cfg(test)]
mod tests;
