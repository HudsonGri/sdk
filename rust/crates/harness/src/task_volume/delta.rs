use super::*;

#[cfg(all(test, acyclic_harness_loom))]
mod models;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

pub(super) fn validate(writes: &[Write]) -> Result<()> {
    let Some(first) = writes.first() else {
        return Err(Error::InvalidSegment);
    };
    let FileKey::Task {
        task,
        file: TaskFile::Chunk { turn, sequence },
    } = &first.key
    else {
        return Err(Error::InvalidSegment);
    };
    if writes.len() > acyclic_stream::MAX_ITEMS
        || sequence % MODEL_CHUNK_STRIDE != 0
        || first.channel.is_none()
    {
        return Err(Error::InvalidSegment);
    }
    for (part, write) in writes.iter().enumerate() {
        let expected = sequence
            .checked_add(part as u64)
            .ok_or(Error::LimitExceeded)?;
        if !matches!(&write.key, FileKey::Task {
            task: candidate, file: TaskFile::Chunk { turn: attempt, sequence: offset }
        } if candidate == task && attempt == turn && *offset == expected)
            || write.channel != first.channel
            || write.publication.is_some()
            || write.bytes.len() > MODEL_CHUNK_BYTES
            || (writes.len() > 1 && write.bytes.is_empty())
        {
            return Err(Error::InvalidSegment);
        }
    }
    Ok(())
}
