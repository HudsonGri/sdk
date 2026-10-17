//! Entry facts packed by `getattrlistbulk` beside each listed name, so a
//! macOS listing needs no per-entry `fstatat`.
//!
//! Directories, and any entry whose file system leaves a needed attribute
//! unreturned, still take `fstatat`: a directory's bulk link count is
//! `va_dirlinkcount`, not the `st_nlink` that `lstat` reports.

use super::HostStatReader;
use crate::native_host::HostStat;
use std::io;

/// Requested common attributes. They pack in bit order after the returned
/// set, so the fixed offsets below follow from this list.
pub(super) const COMMON: libc::attrgroup_t = libc::ATTR_CMN_RETURNED_ATTRS
    | libc::ATTR_CMN_NAME
    | libc::ATTR_CMN_DEVID
    | libc::ATTR_CMN_OBJTYPE
    | libc::ATTR_CMN_CRTIME
    | libc::ATTR_CMN_MODTIME
    | libc::ATTR_CMN_CHGTIME
    | libc::ATTR_CMN_ACCTIME
    | libc::ATTR_CMN_OWNERID
    | libc::ATTR_CMN_GRPID
    | libc::ATTR_CMN_ACCESSMASK
    | libc::ATTR_CMN_FLAGS
    | libc::ATTR_CMN_FILEID;
/// Requested file attributes, packed after the common ones.
pub(super) const FILE: libc::attrgroup_t =
    libc::ATTR_FILE_LINKCOUNT | libc::ATTR_FILE_DEVTYPE | libc::ATTR_FILE_DATALENGTH;

const DEVID: usize = 32;
const OBJTYPE: usize = 36;
const CRTIME: usize = 40;
const MODTIME: usize = 56;
const CHGTIME: usize = 72;
const ACCTIME: usize = 88;
const OWNERID: usize = 104;
const GRPID: usize = 108;
const ACCESSMASK: usize = 112;
const FLAGS: usize = 116;
const FILEID: usize = 120;
const LINKCOUNT: usize = 128;
const DEVTYPE: usize = 132;
const DATALENGTH: usize = 136;
/// End of the fixed section; variable-length names follow it.
pub(super) const FIXED: usize = 144;

fn field<const N: usize>(record: &[u8], at: usize) -> io::Result<[u8; N]> {
    record
        .get(at..at + N)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed directory record"))
}
fn u32_at(record: &[u8], at: usize) -> io::Result<u32> {
    field(record, at).map(u32::from_ne_bytes)
}
fn i64_at(record: &[u8], at: usize) -> io::Result<i64> {
    field(record, at).map(i64::from_ne_bytes)
}

impl HostStatReader {
    /// The packed facts of the record at the cursor, or `None` when this
    /// entry must be examined with `fstatat`.
    pub(super) fn listed_stat(&self) -> io::Result<Option<HostStat>> {
        #[cfg(test)]
        if crate::native_host::reader_work::restoring_old_work_active() {
            return Ok(None);
        }
        // SAFETY: only initialized bytes from the last bounded query are exposed.
        let bytes =
            unsafe { std::slice::from_raw_parts(self.buffer.as_ptr().cast::<u8>(), self.filled) };
        let record = Self::macos_record(bytes, self.next)?;
        let returned = [
            u32_at(record, 4)?,
            u32_at(record, 8)?,
            u32_at(record, 12)?,
            u32_at(record, 16)?,
        ];
        let kind = match u32_at(record, OBJTYPE)? {
            1 => 0o100_000,
            3 => 0o060_000,
            4 => 0o020_000,
            5 => 0o120_000,
            6 => 0o140_000,
            7 => 0o010_000,
            _ => return Ok(None),
        };
        // The returned set's groups are common, volume, directory, file.
        let common = COMMON & !libc::ATTR_CMN_RETURNED_ATTRS;
        if returned[0] & common != common || returned[3] & FILE != FILE {
            return Ok(None);
        }
        let (Ok(mode), Ok(nlink)) = (
            u16::try_from(kind | (u32_at(record, ACCESSMASK)? & 0o7777)),
            u16::try_from(u32_at(record, LINKCOUNT)?),
        ) else {
            return Ok(None);
        };
        // SAFETY: native stat consists only of integers and integer arrays.
        let mut raw: libc::stat = unsafe { std::mem::zeroed() };
        raw.st_dev = field(record, DEVID).map(i32::from_ne_bytes)?;
        raw.st_mode = mode;
        raw.st_nlink = nlink;
        raw.st_ino = field(record, FILEID).map(u64::from_ne_bytes)?;
        raw.st_uid = u32_at(record, OWNERID)?;
        raw.st_gid = u32_at(record, GRPID)?;
        raw.st_rdev = field(record, DEVTYPE).map(i32::from_ne_bytes)?;
        (raw.st_atime, raw.st_atime_nsec) =
            (i64_at(record, ACCTIME)?, i64_at(record, ACCTIME + 8)?);
        (raw.st_mtime, raw.st_mtime_nsec) =
            (i64_at(record, MODTIME)?, i64_at(record, MODTIME + 8)?);
        (raw.st_ctime, raw.st_ctime_nsec) =
            (i64_at(record, CHGTIME)?, i64_at(record, CHGTIME + 8)?);
        (raw.st_birthtime, raw.st_birthtime_nsec) =
            (i64_at(record, CRTIME)?, i64_at(record, CRTIME + 8)?);
        raw.st_size = i64_at(record, DATALENGTH)?;
        raw.st_flags = u32_at(record, FLAGS)?;
        HostStat::from_stat(raw).map(Some)
    }
}
