//! Compact terminal outcomes. Successful coordinated commits point to their
//! existing envelope; append/fork receipts never reload historical payloads.
use super::*;
use crate::{AppendOutcome, CommitConflict, CommitOutcome, IdempotencyOutcome, StreamPath};

mod decode;
#[cfg(test)]
mod tests;
pub(super) use decode::read;

const TAG: u8 = 6;

fn path_len(path: &StreamPath) -> usize {
    4 + path.as_str().len()
}

fn conflict_path(conflict: &CommitConflict) -> &StreamPath {
    match conflict {
        CommitConflict::Head { path, .. }
        | CommitConflict::Tail { path, .. }
        | CommitConflict::Exists { path } => path,
    }
}

fn conflicts_len(conflicts: &[CommitConflict]) -> Result<usize, LocalStreamError> {
    if conflicts.is_empty() || conflicts.len() > crate::MAX_ITEMS {
        return Err(LocalStreamError::Corrupt);
    }
    let mut length = 4_usize;
    let mut previous = None;
    for conflict in conflicts {
        let path = conflict_path(conflict);
        if previous.is_some_and(|previous| previous >= path) {
            return Err(LocalStreamError::Corrupt);
        }
        previous = Some(path);
        let extra = match conflict {
            CommitConflict::Head {
                expected, actual, ..
            } => {
                if expected == actual {
                    return Err(LocalStreamError::Corrupt);
                }
                64
            }
            CommitConflict::Tail {
                expected, actual, ..
            } => {
                if Some(*expected) == *actual {
                    return Err(LocalStreamError::Corrupt);
                }
                9 + usize::from(actual.is_some()) * 8
            }
            CommitConflict::Exists { .. } => 0,
        };
        length = length
            .checked_add(1 + path_len(path) + extra)
            .ok_or(LocalStreamError::InvalidLimits)?;
    }
    Ok(length)
}

fn encoded_len(
    outcome: &IdempotencyOutcome,
    envelope: Option<PageRef>,
) -> Result<usize, LocalStreamError> {
    let committed = matches!(
        outcome,
        IdempotencyOutcome::Commit(CommitOutcome::Committed(_))
    );
    if committed != envelope.is_some() {
        return Err(LocalStreamError::Corrupt);
    }
    let payload = match outcome {
        IdempotencyOutcome::Append(AppendOutcome::Committed(receipt)) => {
            if receipt.start >= receipt.end || receipt.end != receipt.tail {
                return Err(LocalStreamError::Corrupt);
            }
            16 + 32
        }
        IdempotencyOutcome::Append(AppendOutcome::TailConflict { .. }) => 8,
        IdempotencyOutcome::Fork(receipt) => {
            if receipt.source == receipt.destination || receipt.forked_at != receipt.tail {
                return Err(LocalStreamError::Corrupt);
            }
            path_len(&receipt.source) + path_len(&receipt.destination) + 8 + 32
        }
        IdempotencyOutcome::Commit(CommitOutcome::Committed(_)) => 32 + 52,
        IdempotencyOutcome::Commit(CommitOutcome::Conflict(conflicts)) => conflicts_len(conflicts)?,
    };
    let length = 2_usize
        .checked_add(payload)
        .ok_or(LocalStreamError::InvalidLimits)?;
    if length > MAX_COMMAND_BYTES {
        return Err(LocalStreamError::InvalidLimits);
    }
    Ok(length)
}

fn path(bytes: &mut page::BufferBytes, path: &StreamPath) -> Result<(), LocalStreamError> {
    bytes.extend_from_slice(
        &u32::try_from(path.as_str().len())
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes(),
    )?;
    bytes.extend_from_slice(path.as_str().as_bytes())?;
    Ok(())
}

fn conflicts(
    bytes: &mut page::BufferBytes,
    conflicts: &[CommitConflict],
) -> Result<(), LocalStreamError> {
    bytes.extend_from_slice(
        &u32::try_from(conflicts.len())
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes(),
    )?;
    for conflict in conflicts {
        bytes.push(match conflict {
            CommitConflict::Head { .. } => 0,
            CommitConflict::Tail { .. } => 1,
            CommitConflict::Exists { .. } => 2,
        })?;
        path(bytes, conflict_path(conflict))?;
        match conflict {
            CommitConflict::Head {
                expected, actual, ..
            } => {
                bytes.extend_from_slice(expected.as_bytes())?;
                bytes.extend_from_slice(actual.as_bytes())?;
            }
            CommitConflict::Tail {
                expected, actual, ..
            } => {
                bytes.extend_from_slice(&expected.to_le_bytes())?;
                bytes.push(u8::from(actual.is_some()))?;
                if let Some(actual) = actual {
                    bytes.extend_from_slice(&actual.to_le_bytes())?;
                }
            }
            CommitConflict::Exists { .. } => {}
        }
    }
    Ok(())
}

pub(super) fn append(
    io: &mut impl PageIo,
    outcome: &IdempotencyOutcome,
    envelope: Option<PageRef>,
) -> Result<PageRef, LocalStreamError> {
    let length = encoded_len(outcome, envelope)?;
    let mut bytes = Buffer::new(length, io.work())?;
    page::codec(io.work(), length as u64, 0, true)?;
    bytes.bytes.push(TAG)?;
    match outcome {
        IdempotencyOutcome::Append(AppendOutcome::Committed(receipt)) => {
            bytes.bytes.push(0)?;
            bytes
                .bytes
                .extend_from_slice(&receipt.start.to_le_bytes())?;
            bytes.bytes.extend_from_slice(&receipt.end.to_le_bytes())?;
            bytes
                .bytes
                .extend_from_slice(receipt.commit_id.as_bytes())?;
        }
        IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail }) => {
            bytes.bytes.push(1)?;
            bytes.bytes.extend_from_slice(&actual_tail.to_le_bytes())?;
        }
        IdempotencyOutcome::Fork(receipt) => {
            bytes.bytes.push(2)?;
            path(&mut bytes.bytes, &receipt.source)?;
            path(&mut bytes.bytes, &receipt.destination)?;
            bytes
                .bytes
                .extend_from_slice(&receipt.forked_at.to_le_bytes())?;
            bytes
                .bytes
                .extend_from_slice(receipt.commit_id.as_bytes())?;
        }
        IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope_value)) => {
            bytes.bytes.push(3)?;
            bytes
                .bytes
                .extend_from_slice(envelope_value.commit_id.as_bytes())?;
            bytes
                .bytes
                .extend_from_slice(&envelope.ok_or(LocalStreamError::Corrupt)?.encoded())?;
        }
        IdempotencyOutcome::Commit(CommitOutcome::Conflict(values)) => {
            bytes.bytes.push(4)?;
            conflicts(&mut bytes.bytes, values)?;
        }
    }
    page::codec(io.work(), bytes.bytes.len() as u64, 0, false)?;
    if bytes.bytes.len() != length {
        return Err(LocalStreamError::Corrupt);
    }
    let reference = io.append(PageKind::Fact, &bytes.bytes)?;
    if envelope.is_some_and(|child| !child.precedes(reference)) {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(reference)
}
