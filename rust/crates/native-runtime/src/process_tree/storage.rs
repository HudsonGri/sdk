use super::*;
use crate::meter::ReadBuffer;
use std::sync::Arc;

/// Small, stable storage for an originally funded process-tree owner.
///
/// Allocate this before attempting spawn. All operations use the original
/// invocation's meter and retain partial owners in the same allocation on
/// failure. Moving storage between host states never recharges or reallocates
/// the tree. Its contents retire before its credit. There is no public mutable
/// tree reference: extracting a tree would separate its storage from its credit.
///
/// ```compile_fail
/// use acyclic_native_runtime::ProcessTreeStorage;
/// fn escape(mut storage: ProcessTreeStorage) {
///     let _unfunded = storage.take();
/// }
/// ```
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

    /// Starts at most one tree. A failed start can still retain physical work;
    /// that slot cannot be replaced by a later spawn, even after termination.
    pub fn spawn(
        &mut self,
        command: &mut Command,
        custody: Option<&ProcessCustody>,
    ) -> io::Result<()> {
        let meter = Arc::clone(self.slot.meter());
        let slot = self.slot_mut()?;
        match custody {
            Some(custody) => ProcessTree::spawn_in_custody_controlled(
                command,
                custody,
                Some(meter.as_ref()),
                slot,
            ),
            None => ProcessTree::spawn_controlled(command, Some(meter.as_ref()), slot),
        }
    }

    /// Captures through the same retained tree and original meter. Callers fund
    /// the supplied capture buffers before entering this operation.
    pub fn wait_with_output(
        &mut self,
        timeout: Duration,
        max_bytes: usize,
        input: Option<CaptureInput<'_>>,
        check: impl FnMut() -> io::Result<()>,
        observe: impl FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
        buffers: [Vec<u8>; 3],
    ) -> Result<Output, ProcessCaptureFailure> {
        let meter = Arc::clone(self.slot.meter());
        let tree = self
            .slot_mut()
            .map_err(ProcessCaptureFailure::before_capture)?
            .as_mut()
            .ok_or_else(|| ProcessCaptureFailure::before_capture(io::ErrorKind::NotFound.into()))?;
        tree.wait_with_output(
            timeout,
            max_bytes,
            input,
            check,
            observe,
            Some(meter.as_ref()),
            Some(buffers),
        )
    }

    /// Cleans up using the original invocation. Failure preserves the tree and
    /// its allocation in this owner; it is not a quiescence acknowledgment.
    pub fn terminate(&mut self) -> io::Result<()> {
        let meter = Arc::clone(self.slot.meter());
        if let Some(tree) = self.slot_mut()?.as_mut() {
            tree.terminate(Some(meter.as_ref()))?;
        }
        Ok(())
    }

    /// Whether this owner has no remaining physical containment or record work.
    /// This observation alone does not authenticate a harness child-turn claim.
    #[must_use]
    pub fn is_reaped(&self) -> bool {
        matches!(self.slot.data.as_slice(), [slot] if slot.as_ref().is_none_or(ProcessTree::is_reaped))
    }

    fn slot_mut(&mut self) -> io::Result<&mut Option<ProcessTree>> {
        let [slot] = self.slot.data.as_mut_slice() else {
            return Err(io::ErrorKind::InvalidData.into());
        };
        Ok(slot)
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
