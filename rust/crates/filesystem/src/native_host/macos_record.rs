//! Checked slices for getattrlistbulk records, shared with the fuzz target.
use std::{ffi::CStr, io};

fn malformed() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "malformed directory record")
}
pub(crate) fn field<const N: usize>(bytes: &[u8], at: usize) -> io::Result<[u8; N]> {
    bytes
        .get(at..at.checked_add(N).ok_or_else(malformed)?)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(malformed)
}
pub(crate) fn u32_at(bytes: &[u8], at: usize) -> io::Result<u32> {
    field(bytes, at).map(u32::from_ne_bytes)
}
pub(crate) fn i64_at(bytes: &[u8], at: usize) -> io::Result<i64> {
    field(bytes, at).map(i64::from_ne_bytes)
}
pub(crate) fn record(bytes: &[u8], start: usize) -> io::Result<&[u8]> {
    let tail = bytes.get(start..).ok_or_else(malformed)?;
    let length = u32::from_ne_bytes(field(tail, 0)?) as usize;
    // getattrlistbulk guarantees eight-byte aligned groups (Apple man page).
    if length < 32 || !length.is_multiple_of(8) || !start.is_multiple_of(8) {
        return Err(malformed());
    }
    tail.get(..length).ok_or_else(malformed)
}
pub(crate) fn name(record: &[u8], fixed: usize) -> io::Result<&CStr> {
    if u32::from_ne_bytes(field(record, 4)?) & 1 == 0 {
        return Err(malformed());
    }
    let offset =
        usize::try_from(i32::from_ne_bytes(field(record, 24)?)).map_err(|_| malformed())?;
    let start = 24_usize.checked_add(offset).ok_or_else(malformed)?;
    let length = u32::from_ne_bytes(field(record, 28)?) as usize;
    let end = start.checked_add(length).ok_or_else(malformed)?;
    if start < fixed {
        return Err(malformed());
    }
    let name = CStr::from_bytes_with_nul(record.get(start..end).ok_or_else(malformed)?)
        .map_err(|_| malformed())?;
    if name.is_empty() || name.to_bytes().contains(&b'/') {
        return Err(malformed());
    }
    Ok(name)
}
