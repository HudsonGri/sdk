use super::*;
use crate::meter::ReadBuffer;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

/// Small, stable storage for an originally funded process-tree owner.
///
/// Allocate this before attempting spawn. Pass its borrowed slot to
/// [`ProcessTree::spawn_controlled`] or
/// [`ProcessTree::spawn_in_custody_controlled`]; both retain partial owners in
/// this same allocation on failure. Moving storage between host states never
/// recharges or reallocates the tree. Its contents retire before its credit.
pub struct ProcessTreeStorage {
    slot: ReadBuffer<Option<ProcessTree>>,
}

impl ProcessTreeStorage {
    /// Funds the exact slot on the original invocation before allocating it.
    pub fn new(meter: &Arc<dyn crate::NativeReadMeter>) -> io::Result<Self> {
        let mut slot = ReadBuffer::new(1, meter)?;
        slot.data.push(None);
        Ok(Self { slot })
    }
}

impl Deref for ProcessTreeStorage {
    type Target = Option<ProcessTree>;
    fn deref(&self) -> &Self::Target {
        &self.slot.data[0]
    }
}

impl DerefMut for ProcessTreeStorage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slot.data[0]
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
