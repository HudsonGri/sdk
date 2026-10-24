//! Captured read cuts retain only a head and two sequence numbers. Payloads
//! are authenticated on demand, and never while opening or checking a cursor.
use super::*;
use crate::{CommitId, ReadRequest, Record, StreamError, StreamPath, StreamWatermarks};
use catalog::Head;
use root::Root;

#[cfg(test)]
mod tests;

fn semantic(error: StreamError) -> LocalStreamError {
    LocalStreamError::Replay(error)
}

pub(super) fn head(
    io: &mut impl PageRead,
    state: Root,
    path: &StreamPath,
) -> Result<Head, LocalStreamError> {
    catalog::get(io, state, path)?.ok_or_else(|| semantic(StreamError::NotFound))
}

pub(super) fn watermarks(
    io: &mut impl PageRead,
    accepted: Root,
    durable: Root,
    path: &StreamPath,
) -> Result<StreamWatermarks, LocalStreamError> {
    let accepted = head(io, accepted, path)?.tail;
    let durable = catalog::get(io, durable, path)?.map_or(0, |head| head.tail);
    if durable > accepted {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(StreamWatermarks { accepted, durable })
}

/// Check only the authenticated preceding identity. Historical payload bytes
/// cannot affect admission of a resumed cursor at this immutable root.
pub(super) fn anchor(
    io: &mut impl PageRead,
    head: Head,
    from: u64,
    preceding: Option<CommitId>,
) -> Result<(), LocalStreamError> {
    if from > head.tail {
        return Err(semantic(StreamError::OutOfRange));
    }
    if let Some(preceding) = preceding {
        let sequence = from
            .checked_sub(1)
            .ok_or_else(|| semantic(StreamError::InvalidArgument))?;
        if head.commit_id(io, sequence)? != Some(preceding) {
            return Err(semantic(StreamError::PrefixNotRetained));
        }
    }
    Ok(())
}

pub(super) struct Records {
    head: Head,
    next: u64,
    end: u64,
}

impl Records {
    pub(super) fn open(
        io: &mut impl PageRead,
        state: Root,
        request: &ReadRequest,
    ) -> Result<Self, LocalStreamError> {
        crate::memory::validate_limit(request.limit).map_err(semantic)?;
        let head = head(io, state, &request.path)?;
        anchor(io, head, request.from, None)?;
        let end = head
            .tail
            .min(request.from.saturating_add(u64::from(request.limit)));
        Ok(Self {
            head,
            next: request.from,
            end,
        })
    }

    pub(super) fn next(
        &mut self,
        io: &mut impl PageRead,
    ) -> Result<Option<Record>, LocalStreamError> {
        if self.next == self.end {
            return Ok(None);
        }
        let record = self
            .head
            .record(io, self.next)?
            .ok_or(LocalStreamError::Corrupt)?;
        self.next = self.next.checked_add(1).ok_or(LocalStreamError::Corrupt)?;
        Ok(Some(record))
    }
}
