use super::*;

#[derive(Clone, Copy, Default)]
pub(super) struct Span {
    start: usize,
    end: usize,
}
impl Span {
    pub(super) fn range(self) -> std::ops::Range<usize> {
        self.start..self.end
    }
    pub(super) fn slice(self, bytes: &OwnedBytes) -> OwnedBytes {
        bytes.slice(self.start..self.end)
    }
    pub(super) fn text(self, bytes: &OwnedBytes) -> Result<&str, LocalStreamError> {
        std::str::from_utf8(
            bytes
                .get(self.start..self.end)
                .ok_or(LocalStreamError::Corrupt)?,
        )
        .map_err(|_| LocalStreamError::Corrupt)
    }
    fn len(self) -> usize {
        self.end - self.start
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], LocalStreamError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(LocalStreamError::Corrupt)?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(LocalStreamError::Corrupt)?;
        self.offset = end;
        Ok(bytes)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], LocalStreamError> {
        self.take(N)?
            .try_into()
            .map_err(|_| LocalStreamError::Corrupt)
    }
    fn blob(&mut self) -> Result<Span, LocalStreamError> {
        let length = usize::try_from(u32::from_le_bytes(self.array()?))
            .map_err(|_| LocalStreamError::Corrupt)?;
        let start = self.offset;
        self.take(length)?;
        Ok(Span {
            start,
            end: self.offset,
        })
    }
}

pub(super) struct Layout {
    pub(super) sequence: u64,
    pub(super) time: u64,
    pub(super) commit: crate::CommitId,
    pub(super) value: Span,
    keys: [Span; crate::MAX_RECORD_KEYS],
    attributes: [(Span, Span); crate::MAX_RECORD_ATTRIBUTES],
    key_count: usize,
    attribute_count: usize,
}
impl Layout {
    pub(super) fn keys(&self) -> Result<&[Span], LocalStreamError> {
        self.keys
            .get(..self.key_count)
            .ok_or(LocalStreamError::Corrupt)
    }
    pub(super) fn attributes(&self) -> Result<&[(Span, Span)], LocalStreamError> {
        self.attributes
            .get(..self.attribute_count)
            .ok_or(LocalStreamError::Corrupt)
    }
    pub(super) fn read(bytes: &[u8]) -> Result<Self, LocalStreamError> {
        let mut input = Cursor { bytes, offset: 0 };
        if input.take(1)? != [RECORD_TAG] {
            return Err(LocalStreamError::Corrupt);
        }
        let sequence = u64::from_le_bytes(input.array()?);
        if sequence == u64::MAX {
            return Err(LocalStreamError::Corrupt);
        }
        let time = u64::from_le_bytes(input.array()?);
        let commit = crate::CommitId::from_bytes(input.array()?);
        let value = input.blob()?;
        let mut layout = Self {
            sequence,
            time,
            commit,
            value,
            keys: [Span::default(); crate::MAX_RECORD_KEYS],
            attributes: [(Span::default(), Span::default()); crate::MAX_RECORD_ATTRIBUTES],
            key_count: usize::from(u16::from_le_bytes(input.array()?)),
            attribute_count: 0,
        };
        let mut payload = value.len();
        for slot in layout
            .keys
            .get_mut(..layout.key_count)
            .ok_or(LocalStreamError::Corrupt)?
        {
            *slot = input.blob()?;
            if slot.len() > crate::MAX_RECORD_KEY_BYTES {
                return Err(LocalStreamError::Corrupt);
            }
            payload = payload
                .checked_add(slot.len())
                .ok_or(LocalStreamError::Corrupt)?;
        }
        layout.attribute_count = usize::from(u16::from_le_bytes(input.array()?));
        for (name, value) in layout
            .attributes
            .get_mut(..layout.attribute_count)
            .ok_or(LocalStreamError::Corrupt)?
        {
            *name = input.blob()?;
            *value = input.blob()?;
            payload = payload
                .checked_add(name.len())
                .and_then(|payload| payload.checked_add(value.len()))
                .ok_or(LocalStreamError::Corrupt)?;
        }
        if input.offset != bytes.len() || payload > crate::MAX_RECORD_BYTES {
            return Err(LocalStreamError::Corrupt);
        }
        Ok(layout)
    }
}
