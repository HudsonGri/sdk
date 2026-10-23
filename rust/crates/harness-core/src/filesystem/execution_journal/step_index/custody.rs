//! A query owns its temporary claims; only its completed output reaches callers.
use super::*;

pub(super) struct QueryIo<'a> {
    original: Option<&'a PreparationIo>,
    local: Option<PreparationIo>,
}

impl<'a> QueryIo<'a> {
    pub(super) fn new(original: Option<&'a PreparationIo>) -> Result<Self> {
        Ok(Self {
            original,
            local: original.map(PreparationIo::retained_pool).transpose()?,
        })
    }

    pub(super) fn io(&self) -> Option<&PreparationIo> {
        self.local.as_ref()
    }

    pub(super) fn finish(self) -> Result<()> {
        let (Some(original), Some(local)) = (self.original, &self.local) else {
            return Ok(());
        };
        match local {
            PreparationIo::Owned { custody, .. } => {
                let target = match original {
                    PreparationIo::Owned { custody, .. } => custody,
                    #[cfg(not(target_arch = "wasm32"))]
                    PreparationIo::Native { .. } => return Err(invalid(Some(original))),
                };
                let mut source = custody
                    .as_ref()
                    .ok_or_else(|| invalid(Some(local)))?
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // This private child can transfer only to the parent used at
                // construction. The child never escapes, so transfers cannot form
                // cycles. The shared allocation ledger remains unchanged.
                if let Some(target) = target {
                    let mut target = target
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    *target = target
                        .checked_add(*source)
                        .ok_or_else(|| invalid(Some(original)))?;
                }
                *source = 0;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            PreparationIo::Native { .. } => {
                original.absorb_native_custody(local.take_native_custody(local.owned_bytes()?)?)
            }
        }
    }
}

impl Drop for QueryIo<'_> {
    fn drop(&mut self) {
        if let Some(io) = self.local.take() {
            io.release_retained_pool();
        }
    }
}
