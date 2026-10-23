//! Isolated ownership for a fallible preparation, on its caller's ledger.
use super::*;

pub(crate) struct PreparationScope<'a> {
    original: Option<&'a PreparationIo>,
    local: Option<PreparationIo>,
}

impl<'a> PreparationScope<'a> {
    pub(crate) fn new(original: Option<&'a PreparationIo>) -> Result<Self> {
        Ok(Self {
            original,
            local: original.map(PreparationIo::retained_pool).transpose()?,
        })
    }

    pub(crate) fn io(&self) -> Option<&PreparationIo> {
        self.local.as_ref()
    }

    /// Release scratch and transfer only the inventoried output. Admission
    /// already happened on this ledger; transfer never allocates a second copy.
    pub(crate) fn finish(self, retained: u64) -> Result<()> {
        let (Some(original), Some(local)) = (self.original, &self.local) else {
            return Ok(());
        };
        let mut hold = local.guard_owned(local.owned_bytes()?)?;
        hold.substitute(retained)?;
        hold.into_retained()?;
        match local {
            PreparationIo::Owned { custody, .. } => {
                let target = match original {
                    PreparationIo::Owned { custody, .. } => custody,
                    #[cfg(not(target_arch = "wasm32"))]
                    PreparationIo::Native { .. } => {
                        return Err(local.invalid("preparation scope ledger differs"));
                    }
                };
                let mut source = custody
                    .as_ref()
                    .ok_or_else(|| local.invalid("preparation scope has no custody"))?
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // The private child can transfer only to its construction
                // parent; no transfer cycle can acquire these locks in reverse.
                if let Some(target) = target {
                    let mut target = target
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    *target = target
                        .checked_add(*source)
                        .ok_or_else(|| local.invalid("preparation scope ownership overflows"))?;
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

impl Drop for PreparationScope<'_> {
    fn drop(&mut self) {
        if let Some(local) = self.local.take() {
            local.release_retained_pool();
        }
    }
}
