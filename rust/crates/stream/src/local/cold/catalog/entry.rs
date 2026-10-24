//! Small, independently authenticated sequence metadata. Head checks must not
//! fetch a record's body simply to inspect its commit identity.
use super::*;

const TAG: u8 = 7;
const LENGTH: usize = 1 + 8 + 32 + 8 + 52;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Entry {
    pub(super) sequence: u64,
    pub(super) commit: crate::CommitId,
    pub(super) committed_at_micros: u64,
    pub(super) body: PageRef,
}

impl Entry {
    pub(super) fn append(self, io: &mut impl PageIo) -> Result<PageRef, LocalStreamError> {
        if self.sequence == u64::MAX {
            return Err(LocalStreamError::Corrupt);
        }
        page::codec(io.work(), LENGTH as u64, 0, true)?;
        let mut bytes = [0; LENGTH];
        let (tag, rest) = bytes.split_at_mut(1);
        tag.copy_from_slice(&[TAG]);
        let (sequence, rest) = rest.split_at_mut(8);
        sequence.copy_from_slice(&self.sequence.to_le_bytes());
        let (commit, rest) = rest.split_at_mut(32);
        commit.copy_from_slice(self.commit.as_bytes());
        let (time, body) = rest.split_at_mut(8);
        time.copy_from_slice(&self.committed_at_micros.to_le_bytes());
        body.copy_from_slice(&self.body.encoded());
        page::codec(io.work(), LENGTH as u64, 0, false)?;
        let reference = io.append(PageKind::Fact, &bytes)?;
        if !self.body.precedes(reference) {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(reference)
    }

    pub(super) fn load(
        io: &mut impl PageRead,
        reference: PageRef,
    ) -> Result<Self, LocalStreamError> {
        let page = io.read(reference)?;
        if page.kind != PageKind::Fact {
            return Err(LocalStreamError::Corrupt);
        }
        let mut cursor = decode::Cursor(page.body()?);
        if cursor.byte()? != TAG {
            return Err(LocalStreamError::Corrupt);
        }
        let sequence = cursor.number()?;
        let commit = crate::CommitId::from_bytes(*cursor.fixed()?);
        let committed_at_micros = cursor.number()?;
        let body = PageRef::decode(&mut cursor.0)?;
        if sequence == u64::MAX || !cursor.0.is_empty() || !body.precedes(reference) {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(Self {
            sequence,
            commit,
            committed_at_micros,
            body,
        })
    }
}
