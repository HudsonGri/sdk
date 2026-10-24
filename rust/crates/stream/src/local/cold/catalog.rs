//! Bounded point facts for permanent paths. Forks share a sequence-index root
//! and an exclusive cut; a read never follows a chain of parent paths.
use super::root::Root;
use super::*;

#[cfg(test)]
mod tests;

const TAG: u8 = 2;
const FIXED: usize = 1 + 8 + 8 + 53 + 4;
const PATH_DOMAIN: &[u8] = b"acyclic.stream.cold.path-key/v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Head {
    pub(super) tail: u64,
    pub(super) created_decision: u64,
    pub(super) records: Option<PageRef>,
}

fn path_key(
    path: &crate::StreamPath,
    work: Option<&Arc<crate::memory::Work>>,
) -> Result<[u8; 32], LocalStreamError> {
    let length = PATH_DOMAIN.len() + 8 + path.as_str().len();
    page::codec(work, 0, length as u64, true)?;
    let mut hash = Sha256::new();
    hash.update(PATH_DOMAIN);
    hash.update((path.as_str().len() as u64).to_le_bytes());
    hash.update(path.as_str().as_bytes());
    let key = hash.finalize().into();
    page::codec(work, 0, length as u64, false)?;
    Ok(key)
}

impl Head {
    pub(super) fn fork(self, at: u64, decision: u64) -> Result<Self, crate::StreamError> {
        if at > self.tail {
            return Err(crate::StreamError::PrefixNotRetained);
        }
        if decision == 0 {
            return Err(crate::StreamError::Unavailable);
        }
        Ok(Self {
            tail: at,
            created_decision: decision,
            records: self.records,
        })
    }

    fn append(
        self,
        io: &mut impl PageIo,
        path: &crate::StreamPath,
    ) -> Result<PageRef, LocalStreamError> {
        if self.created_decision == 0 || self.tail != 0 && self.records.is_none() {
            return Err(LocalStreamError::Corrupt);
        }
        let length = FIXED + path.as_str().len();
        let mut bytes = Buffer::new(length, io.work())?;
        page::codec(io.work(), length as u64, 0, true)?;
        bytes.bytes.push(TAG);
        bytes.bytes.extend_from_slice(&self.tail.to_le_bytes());
        bytes
            .bytes
            .extend_from_slice(&self.created_decision.to_le_bytes());
        match self.records {
            Some(reference) => {
                bytes.bytes.push(1);
                bytes.bytes.extend_from_slice(&reference.encoded());
            }
            None => bytes.bytes.extend_from_slice(&[0; 53]),
        }
        bytes.bytes.extend_from_slice(
            &u32::try_from(path.as_str().len())
                .map_err(|_| LocalStreamError::InvalidLimits)?
                .to_le_bytes(),
        );
        bytes.bytes.extend_from_slice(path.as_str().as_bytes());
        page::codec(io.work(), length as u64, 0, false)?;
        let reference = io.append(PageKind::Fact, &bytes.bytes)?;
        if self.records.is_some_and(|child| !child.precedes(reference)) {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(reference)
    }

    fn load(
        io: &mut impl PageRead,
        reference: PageRef,
        path: &crate::StreamPath,
    ) -> Result<Self, LocalStreamError> {
        let page = io.read(reference)?;
        if page.kind != PageKind::Fact {
            return Err(LocalStreamError::Corrupt);
        }
        let mut bytes = page.body()?;
        let (tag, rest) = bytes.split_first().ok_or(LocalStreamError::Corrupt)?;
        if *tag != TAG {
            return Err(LocalStreamError::Corrupt);
        }
        bytes = rest;
        let (tail, rest) = bytes
            .split_first_chunk::<8>()
            .ok_or(LocalStreamError::Corrupt)?;
        let tail = u64::from_le_bytes(*tail);
        let (created, rest) = rest
            .split_first_chunk::<8>()
            .ok_or(LocalStreamError::Corrupt)?;
        let created_decision = u64::from_le_bytes(*created);
        let (optional, rest) = rest
            .split_first_chunk::<53>()
            .ok_or(LocalStreamError::Corrupt)?;
        let records = match optional.split_first() {
            Some((&0, rest)) if rest.iter().all(|byte| *byte == 0) => None,
            Some((&1, mut rest)) => {
                let records = PageRef::decode(&mut rest)?;
                if !rest.is_empty() || !records.precedes(reference) {
                    return Err(LocalStreamError::Corrupt);
                }
                Some(records)
            }
            _ => return Err(LocalStreamError::Corrupt),
        };
        let (length, rest) = rest
            .split_first_chunk::<4>()
            .ok_or(LocalStreamError::Corrupt)?;
        let length =
            usize::try_from(u32::from_le_bytes(*length)).map_err(|_| LocalStreamError::Corrupt)?;
        if length != rest.len()
            || rest != path.as_str().as_bytes()
            || created_decision == 0
            || tail != 0 && records.is_none()
        {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(Self {
            tail,
            created_decision,
            records,
        })
    }

    pub(super) fn record(
        self,
        io: &mut impl PageRead,
        sequence: u64,
    ) -> Result<Option<crate::Record>, LocalStreamError> {
        if sequence >= self.tail {
            return Ok(None);
        }
        let reference = index::get(io, self.records, &sequence.to_be_bytes())?
            .ok_or(LocalStreamError::Corrupt)?;
        let record = record::read(io, reference)?;
        if record.sequence != sequence {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(Some(record))
    }

    pub(super) fn append_record(
        &mut self,
        io: &mut impl PageIo,
        record: &crate::Record,
    ) -> Result<(), LocalStreamError> {
        if record.sequence != self.tail {
            return Err(LocalStreamError::Corrupt);
        }
        let tail = self
            .tail
            .checked_add(1)
            .ok_or(LocalStreamError::InvalidLimits)?;
        let reference = record::append(io, record)?;
        let records = index::put(io, self.records, &record.sequence.to_be_bytes(), reference)?;
        self.records = Some(records);
        self.tail = tail;
        Ok(())
    }
}

pub(super) fn get(
    io: &mut impl PageRead,
    state: Root,
    path: &crate::StreamPath,
) -> Result<Option<Head>, LocalStreamError> {
    let key = path_key(path, io.work())?;
    let Some(reference) = index::get(io, state.points, &key)? else {
        return Ok(None);
    };
    let head = Head::load(io, reference, path)?;
    if head.created_decision > state.decision {
        return Err(LocalStreamError::Corrupt);
    }
    Ok(Some(head))
}

pub(super) fn put(
    io: &mut impl PageIo,
    state: &mut Root,
    path: &crate::StreamPath,
    head: Head,
) -> Result<(), LocalStreamError> {
    if head.created_decision > state.decision {
        return Err(LocalStreamError::Corrupt);
    }
    let key = path_key(path, io.work())?;
    let reference = head.append(io, path)?;
    state.points = Some(index::put(io, state.points, &key, reference)?);
    Ok(())
}
