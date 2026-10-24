//! Checked field decoding shared by durable metadata facts.
use super::*;

pub(super) struct Cursor<'a>(pub(super) &'a [u8]);

impl<'a> Cursor<'a> {
    pub(super) fn fixed<const N: usize>(&mut self) -> Result<&'a [u8; N], LocalStreamError> {
        let (bytes, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or(LocalStreamError::Corrupt)?;
        self.0 = rest;
        Ok(bytes)
    }

    pub(super) fn byte(&mut self) -> Result<u8, LocalStreamError> {
        Ok(u8::from_le_bytes(*self.fixed()?))
    }

    pub(super) fn number(&mut self) -> Result<u64, LocalStreamError> {
        Ok(u64::from_le_bytes(*self.fixed()?))
    }

    pub(super) fn count(&mut self) -> Result<usize, LocalStreamError> {
        usize::try_from(u32::from_le_bytes(*self.fixed()?)).map_err(|_| LocalStreamError::Corrupt)
    }

    pub(super) fn path(
        &mut self,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<crate::StreamPath, LocalStreamError> {
        let length = self.count()?;
        let (bytes, rest) = self
            .0
            .split_at_checked(length)
            .ok_or(LocalStreamError::Corrupt)?;
        self.0 = rest;
        let text = std::str::from_utf8(bytes).map_err(|_| LocalStreamError::Corrupt)?;
        match work {
            Some(work) => {
                page::observe(Some(work), crate::StreamPath::new_native(text, &work.meter))?
                    .map_err(|_| LocalStreamError::Corrupt)
            }
            None => crate::StreamPath::new(text).map_err(|_| LocalStreamError::Corrupt),
        }
    }
}
