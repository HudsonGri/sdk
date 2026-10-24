use super::*;
use crate::{CommittedAppend, CommittedFork};

use crate::local::cold::decode::Cursor;

fn records(
    io: &mut impl PageRead,
    cursor: &mut Cursor<'_>,
    parent: PageRef,
    start: u64,
    count: usize,
    commit_id: CommitId,
    time: &mut Option<u64>,
) -> Result<Funded<Vec<Record>>, LocalStreamError> {
    // Validate the complete reference extent before allocating from a forged
    // count, and validate every child's ordering before following that child.
    if count > crate::MAX_ITEMS
        || cursor.0.len() / 52 < count
        || start.checked_add(count as u64).is_none()
    {
        return Err(LocalStreamError::Corrupt);
    }
    let work = io.work().cloned();
    let mut records = Funded::vector(count, work.as_ref())?;
    for offset in 0..count {
        let mut bytes = cursor.fixed::<52>()?.as_slice();
        let reference = PageRef::decode(&mut bytes)?;
        if !reference.precedes(parent) {
            return Err(LocalStreamError::Corrupt);
        }
        let record = record::read(io, reference)?;
        if record.sequence != start + offset as u64
            || record.commit_id != commit_id
            || time.is_some_and(|time| time != record.committed_at_micros)
        {
            return Err(LocalStreamError::Corrupt);
        }
        *time = Some(record.committed_at_micros);
        page::codec(work.as_ref(), std::mem::size_of::<Record>() as u64, 0, true)?;
        records.value.push(record);
        page::codec(
            work.as_ref(),
            std::mem::size_of::<Record>() as u64,
            0,
            false,
        )?;
    }
    Ok(records)
}

pub(in crate::local::cold) fn read(
    io: &mut impl PageRead,
    reference: PageRef,
    expected: CommitId,
) -> Result<Funded<CommittedEnvelope>, LocalStreamError> {
    let work = io.work().cloned();
    let page = io.read(reference)?;
    if page.kind != PageKind::Fact {
        return Err(LocalStreamError::Corrupt);
    }
    let mut cursor = Cursor(page.body()?);
    if cursor.byte()? != TAG || *cursor.fixed::<32>()? != *expected.as_bytes() {
        return Err(LocalStreamError::Corrupt);
    }
    let count = cursor.count()?;
    if count == 0 || count > crate::MAX_ITEMS || count > cursor.0.len() / 17 {
        return Err(LocalStreamError::Corrupt);
    }
    let mut mutations = Funded::vector(count, work.as_ref())?;
    let mut time = None;
    for _ in 0..count {
        let kind = cursor.byte()?;
        if kind > 1 {
            return Err(LocalStreamError::Corrupt);
        }
        let path = cursor.path(work.as_ref())?;
        let source = (kind == 1)
            .then(|| cursor.path(work.as_ref()))
            .transpose()?;
        if source.as_ref() == Some(&path) {
            return Err(LocalStreamError::Corrupt);
        }
        let start = u64::from_le_bytes(*cursor.fixed()?);
        let count = cursor.count()?;
        if kind == 0 && count == 0 {
            return Err(LocalStreamError::Corrupt);
        }
        let records = records(
            io,
            &mut cursor,
            reference,
            start,
            count,
            expected,
            &mut time,
        )?;
        let end = start
            .checked_add(count as u64)
            .ok_or(LocalStreamError::Corrupt)?;
        let records = mutations.absorb(records)?;
        page::codec(
            work.as_ref(),
            std::mem::size_of::<CommittedMutation>() as u64,
            0,
            true,
        )?;
        mutations.value.push(match source {
            Some(source) => CommittedMutation::Fork(CommittedFork {
                source,
                destination: path,
                forked_at: start,
                tail: end,
                records,
            }),
            None => CommittedMutation::Append(CommittedAppend {
                path,
                start,
                end,
                tail: end,
                records,
            }),
        });
        page::codec(
            work.as_ref(),
            std::mem::size_of::<CommittedMutation>() as u64,
            0,
            false,
        )?;
    }
    if !cursor.0.is_empty() {
        return Err(LocalStreamError::Corrupt);
    }
    let envelope = mutations.map(|mutations| CommittedEnvelope {
        commit_id: expected,
        mutations,
    });
    if encoded_len(&envelope.value)? != page.body()?.len() {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(envelope)
}
