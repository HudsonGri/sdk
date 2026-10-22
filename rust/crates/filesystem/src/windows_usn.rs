//! Conservative durable Windows USN restart continuity.
//!
//! The adapter proves only the strongest inexpensive case: the exact root,
//! volume journal, and volume-wide next USN are unchanged. Any journal advance
//! or ambiguity requires the canonical watcher baseline; it is never treated
//! as proof that the watched subtree was unchanged. A same-volume checkpoint
//! write advances the journal itself, so it cannot use this fast path.

#![allow(
    unsafe_code,
    reason = "reads the USN journal through Win32 volume control calls"
)]

use crate::NativeRootIdentity;
use crate::native_host::HostRoot;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::Path;
use thiserror::Error;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAGS_AND_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    FILE_TRAVERSE, GetFinalPathNameByHandleW, OPEN_EXISTING, VOLUME_NAME_GUID,
};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::Ioctl::{FSCTL_QUERY_USN_JOURNAL, USN_JOURNAL_DATA_V0};

const CHECKPOINT_MAGIC: &[u8; 8] = b"ACYUSN\0\x01";
const CHECKPOINT_BYTES: usize = 40;

/// Durable conservative USN marker captured before a full authenticated scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowsUsnCheckpoint {
    root_identity: NativeRootIdentity,
    journal_id: u64,
    next_usn: u64,
}

impl WindowsUsnCheckpoint {
    /// Returns the root identity to which this marker is confined.
    #[must_use]
    pub const fn root_identity(self) -> NativeRootIdentity {
        self.root_identity
    }

    /// Returns the volume change-journal identity.
    #[must_use]
    pub const fn journal_id(self) -> u64 {
        self.journal_id
    }

    /// Returns the first USN not represented by the marker.
    #[must_use]
    pub const fn next_usn(self) -> u64 {
        self.next_usn
    }

    /// Encodes the stable versioned 40-byte checkpoint.
    #[must_use]
    pub fn to_bytes(self) -> [u8; CHECKPOINT_BYTES] {
        let mut bytes = [0_u8; CHECKPOINT_BYTES];
        bytes[..8].copy_from_slice(CHECKPOINT_MAGIC);
        bytes[8..24].copy_from_slice(&self.root_identity.to_bytes());
        bytes[24..32].copy_from_slice(&self.journal_id.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.next_usn.to_le_bytes());
        bytes
    }

    /// Decodes one exact canonical checkpoint.
    ///
    /// # Errors
    ///
    /// Rejects a wrong length, version, or USN outside the Windows signed-USN
    /// domain.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WindowsUsnError> {
        if bytes.len() != CHECKPOINT_BYTES || !bytes.starts_with(CHECKPOINT_MAGIC) {
            return Err(WindowsUsnError::InvalidCheckpoint);
        }
        let mut root = [0_u8; 16];
        root.copy_from_slice(bytes.get(8..24).ok_or(WindowsUsnError::InvalidCheckpoint)?);
        let journal_id = decode_u64(
            bytes
                .get(24..32)
                .ok_or(WindowsUsnError::InvalidCheckpoint)?,
        );
        let next_usn = decode_u64(
            bytes
                .get(32..40)
                .ok_or(WindowsUsnError::InvalidCheckpoint)?,
        );
        if next_usn > i64::MAX as u64 {
            return Err(WindowsUsnError::InvalidCheckpoint);
        }
        Ok(Self {
            root_identity: NativeRootIdentity::from_bytes(root),
            journal_id,
            next_usn,
        })
    }
}

/// Why a restart cannot skip the canonical authenticated baseline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowsUsnDiscontinuity {
    /// The watched directory identity changed.
    RootChanged,
    /// Windows replaced the volume journal.
    JournalChanged,
    /// The retained marker fell behind the journal's valid range.
    JournalTruncated,
    /// The marker is ahead of the current journal and cannot be authentic.
    MarkerAhead,
    /// At least one change occurred anywhere on the volume.
    VolumeAdvanced,
}

/// Conservative restart validation outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowsUsnContinuity {
    /// No volume change occurred after the checkpoint.
    Unchanged,
    /// The caller must perform the ordinary authenticated baseline handshake.
    BaselineRequired(WindowsUsnDiscontinuity),
}

/// Captures a marker before an authenticated baseline scan begins.
///
/// A consumer persists the marker only after the baseline and every queued
/// watcher change have been durably published. Capturing after publication is
/// unsafe because a change could enter the journal before its watcher callback
/// is observed.
///
/// # Errors
///
/// Rejects unsupported paths, unavailable journals, invalid journal values,
/// root identity failures, or native I/O errors.
pub fn capture_windows_usn_checkpoint(
    root: &Path,
) -> Result<WindowsUsnCheckpoint, WindowsUsnError> {
    let (root_identity, journal) = observe_root(root)?;
    Ok(WindowsUsnCheckpoint {
        root_identity,
        journal_id: journal.journal_id,
        next_usn: journal.next_usn,
    })
}

/// Proves unchanged restart continuity or requires a baseline.
///
/// # Errors
///
/// Returns only environmental/native failures. Every semantic mismatch is a
/// successful `BaselineRequired` outcome so callers cannot confuse baseline
/// with transport failure.
pub fn validate_windows_usn_checkpoint(
    root: &Path,
    checkpoint: WindowsUsnCheckpoint,
) -> Result<WindowsUsnContinuity, WindowsUsnError> {
    let (observed, journal) = observe_root(root)?;
    Ok(classify_continuity(observed, checkpoint, &journal))
}

fn classify_continuity(
    observed: NativeRootIdentity,
    checkpoint: WindowsUsnCheckpoint,
    journal: &JournalSnapshot,
) -> WindowsUsnContinuity {
    if observed != checkpoint.root_identity {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::RootChanged);
    }
    if journal.journal_id != checkpoint.journal_id {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalChanged);
    }
    if checkpoint.next_usn < journal.lowest_valid_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalTruncated);
    }
    if checkpoint.next_usn > journal.next_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::MarkerAhead);
    }
    if checkpoint.next_usn != journal.next_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced);
    }
    WindowsUsnContinuity::Unchanged
}

struct JournalSnapshot {
    journal_id: u64,
    lowest_valid_usn: u64,
    next_usn: u64,
}

/// Resolve the journal's volume from the held root, including mounted-folder
/// volumes. USN control calls require a volume handle, not a directory handle.
fn observe_root(root: &Path) -> Result<(NativeRootIdentity, JournalSnapshot), WindowsUsnError> {
    let canonical = root
        .canonicalize()
        .map_err(|error| WindowsUsnError::Io(error.to_string()))?;
    let root = HostRoot::open(&canonical, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)
        .map_err(|_| WindowsUsnError::RootIdentityUnavailable)?;
    Ok((root.identity(), query_journal(&root)?))
}

fn query_journal(root: &HostRoot) -> Result<JournalSnapshot, WindowsUsnError> {
    let volume = open_journal_volume(root)?;
    let mut data = USN_JOURNAL_DATA_V0::default();
    let mut returned = 0_u32;
    let output_size = u32::try_from(size_of::<USN_JOURNAL_DATA_V0>())
        .map_err(|_| WindowsUsnError::InvalidJournal)?;
    let handle = HANDLE(volume.as_raw_handle());
    // SAFETY: `volume` holds a valid volume handle for the duration of the call;
    // the output pointer references a correctly sized initialized structure;
    // no overlapped operation outlives these stack values.
    unsafe {
        DeviceIoControl(
            handle,
            FSCTL_QUERY_USN_JOURNAL,
            None,
            0,
            Some((&raw mut data).cast()),
            output_size,
            Some(&raw mut returned),
            None,
        )
    }
    .map_err(|error| WindowsUsnError::Native(error.to_string()))?;
    if returned < output_size || data.NextUsn < 0 || data.LowestValidUsn < 0 {
        return Err(WindowsUsnError::InvalidJournal);
    }
    Ok(JournalSnapshot {
        journal_id: data.UsnJournalID,
        lowest_valid_usn: u64::try_from(data.LowestValidUsn)
            .map_err(|_| WindowsUsnError::InvalidJournal)?,
        next_usn: u64::try_from(data.NextUsn).map_err(|_| WindowsUsnError::InvalidJournal)?,
    })
}

fn open_journal_volume(root: &HostRoot) -> Result<std::fs::File, WindowsUsnError> {
    let handle = HANDLE(root.directory_handle().as_raw_handle());
    let mut path = Vec::new();
    loop {
        // SAFETY: the held directory and initialized output buffer remain live
        // through the synchronous call. An empty buffer queries its size.
        let count = unsafe { GetFinalPathNameByHandleW(handle, &mut path, VOLUME_NAME_GUID) };
        if count == 0 {
            return Err(WindowsUsnError::Io(
                std::io::Error::last_os_error().to_string(),
            ));
        }
        let count = count as usize;
        if count < path.len() {
            path.truncate(count);
            break;
        }
        path.resize(
            count
                .checked_add(1)
                .ok_or(WindowsUsnError::InvalidJournal)?,
            0,
        );
    }
    let name = volume_device_name(&path)?;
    // SAFETY: the GUID volume name is terminated and contains no embedded NUL.
    // FILE_TRAVERSE selects the filesystem's volume object. An access-zero
    // open can bypass the filesystem and reach the storage device, which
    // rejects USN controls. No raw file-data read or write access is requested.
    // CreateFileW transfers a new synchronous handle, which File owns below.
    let volume = unsafe {
        CreateFileW(
            windows::core::PCWSTR(name.as_ptr()),
            FILE_TRAVERSE.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            None,
        )
    }
    .map_err(|error| WindowsUsnError::Native(error.to_string()))?;
    // SAFETY: this fresh handle is now owned by File and closes on every return.
    Ok(unsafe { std::fs::File::from_raw_handle(volume.0) })
}

fn volume_device_name(path: &[u16]) -> Result<Vec<u16>, WindowsUsnError> {
    const PREFIX: &[u16] = &[92, 92, 63, 92, 86, 111, 108, 117, 109, 101, 123];
    // GetFinalPathNameByHandleW(VOLUME_NAME_GUID) returns
    // \\?\Volume{xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}\..., independent of
    // drive letters and the spelling used to reach a mounted-folder volume.
    if !path.starts_with(PREFIX)
        || path.get(47).copied() != Some(125)
        || path.get(48).copied() != Some(92)
    {
        return Err(WindowsUsnError::InvalidJournal);
    }
    for (ordinal, unit) in path[11..47].iter().copied().enumerate() {
        let valid = if matches!(ordinal, 8 | 13 | 18 | 23) {
            unit == u16::from(b'-')
        } else {
            u8::try_from(unit).is_ok_and(|byte| byte.is_ascii_hexdigit())
        };
        if !valid {
            return Err(WindowsUsnError::InvalidJournal);
        }
    }
    let mut device = path[..48].to_vec();
    device.push(0);
    Ok(device)
}

fn decode_u64(bytes: &[u8]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(bytes);
    u64::from_le_bytes(value)
}

/// Windows USN checkpoint failures.
#[derive(Debug, Error)]
pub enum WindowsUsnError {
    /// Checkpoint bytes are not the exact current canonical format.
    #[error("the Windows USN checkpoint is invalid")]
    InvalidCheckpoint,
    /// The native root identity cannot be proven.
    #[error("the Windows USN root identity is unavailable")]
    RootIdentityUnavailable,
    /// The journal returned impossible or truncated metadata.
    #[error("the Windows USN journal metadata is invalid")]
    InvalidJournal,
    /// Ordinary path or volume I/O failed.
    #[error("Windows USN I/O failed: {0}")]
    Io(String),
    /// The native journal query failed.
    #[error("Windows USN query failed: {0}")]
    Native(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_search_usn_device_comes_from_the_held_volume_guid() {
        let guid = r"\\?\Volume{12345678-1234-abcd-ABCD-1234567890ab}";
        for suffix in [r"\", r"\mounted\child", "\\非ASCII"] {
            let input: Vec<_> = format!("{guid}{suffix}").encode_utf16().collect();
            let device = volume_device_name(&input).unwrap();
            assert_eq!(device, guid.encode_utf16().chain([0]).collect::<Vec<_>>());
        }
        // Negative controls: a drive or share spelling cannot identify a
        // mounted-folder volume, and unvalidated text cannot select a device.
        for invalid in [
            r"C:\mounted",
            r"\\?\C:\mounted",
            r"\\server\share",
            r"\\?\Volume{invalid}\",
        ] {
            assert!(volume_device_name(&invalid.encode_utf16().collect::<Vec<_>>()).is_err());
        }
    }

    #[test]
    fn checkpoint_encoding_is_canonical_versioned_and_bounded() {
        let checkpoint = WindowsUsnCheckpoint {
            root_identity: NativeRootIdentity::from_bytes([7; 16]),
            journal_id: 19,
            next_usn: 23,
        };
        let bytes = checkpoint.to_bytes();
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&bytes),
            Ok(decoded) if decoded == checkpoint
        ));
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&bytes[..39]),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
        let mut wrong_version = bytes;
        wrong_version[7] = 2;
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&wrong_version),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
        let mut impossible = bytes;
        impossible[32..40].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&impossible),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
    }

    #[test]
    fn every_journal_transition_is_total_and_fail_closed() {
        let root = NativeRootIdentity::from_bytes([1; 16]);
        let checkpoint = WindowsUsnCheckpoint {
            root_identity: root,
            journal_id: 2,
            next_usn: 20,
        };
        let snapshot = |journal_id, lowest_valid_usn, next_usn| JournalSnapshot {
            journal_id,
            lowest_valid_usn,
            next_usn,
        };
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(2, 10, 20)),
            WindowsUsnContinuity::Unchanged
        );
        assert_eq!(
            classify_continuity(
                NativeRootIdentity::from_bytes([3; 16]),
                checkpoint,
                &snapshot(2, 10, 20)
            ),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::RootChanged)
        );
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(3, 10, 20)),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalChanged)
        );
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(2, 21, 21)),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalTruncated)
        );
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(2, 10, 19)),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::MarkerAhead)
        );
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(2, 10, 21)),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced)
        );
    }

    #[test]
    #[ignore = "requires a live local Windows volume with a readable USN journal"]
    fn live_journal_uses_the_held_volume_and_fences_a_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let checkpoint = capture_windows_usn_checkpoint(root.path())?;
        // The system volume can advance for unrelated writes between calls.
        // Deterministic unchanged/mismatch controls are exercised above.
        assert!(matches!(
            validate_windows_usn_checkpoint(root.path(), checkpoint)?,
            WindowsUsnContinuity::Unchanged
                | WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced)
        ));
        let changed = root.path().join("changed");
        std::fs::write(&changed, b"changed")?;
        // FlushFileBuffers requires write access to the flushed handle.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&changed)?
            .sync_all()?;
        assert_eq!(
            validate_windows_usn_checkpoint(root.path(), checkpoint)?,
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced)
        );
        Ok(())
    }
}
