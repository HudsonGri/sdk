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
    slot: ReadBuffer<ProcessSlot>,
}

struct ProcessSlot {
    tree: Option<ProcessTree>,
    retirement: Option<Retirement>,
}

/// Concrete kill and wait SDK calls are withheld before physical admission.
/// Cancellation cannot consume the credit needed to stop an abandoned owner.
pub(super) struct Retirement {
    pub(super) writes: crate::NativeOperationReservation<Arc<dyn crate::NativeReadMeter>>,
    pub(super) wait: crate::NativeOperationReservation<Arc<dyn crate::NativeReadMeter>>,
}

impl Retirement {
    fn new(meter: &Arc<dyn crate::NativeReadMeter>) -> io::Result<Self> {
        Ok(Self {
            writes: crate::NativeOperationReservation::new(
                Arc::clone(meter),
                crate::NativeOperationKind::Write,
                platform::RETIREMENT_WRITES,
            )?,
            wait: crate::NativeOperationReservation::new(
                Arc::clone(meter),
                crate::NativeOperationKind::Read,
                1,
            )?,
        })
    }
}

impl ProcessTreeStorage {
    /// Funds the exact slot on the original invocation before allocating it.
    pub fn new(meter: &Arc<dyn crate::NativeReadMeter>) -> io::Result<Self> {
        let mut slot = ReadBuffer::new(1, meter)?;
        slot.data.push(ProcessSlot {
            tree: None,
            retirement: None,
        });
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
        let [state] = self.slot.data.as_mut_slice() else {
            return Err(io::ErrorKind::InvalidData.into());
        };
        if state.tree.is_some() || state.retirement.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        state.retirement = Some(Retirement::new(&meter)?);
        let slot = &mut state.tree;
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
        matches!(self.slot.data.as_slice(), [slot] if slot.tree.as_ref().is_none_or(ProcessTree::is_reaped))
    }

    /// Queues one native prompt on the retained process without giving away its pipe.
    pub fn queue_stdin(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        self.slot.meter().check()?;
        self.tree_mut()?.queue_stdin(bytes)
    }

    /// Advances the shared stdin pump on the original invocation's meter.
    pub fn pump_stdin(&mut self) -> io::Result<bool> {
        let meter = Arc::clone(self.slot.meter());
        self.tree_mut()?.pump_stdin_controlled(Some(meter.as_ref()))
    }

    /// Refuses completion until all queued input has been delivered.
    pub fn require_stdin_complete(&self) -> io::Result<()> {
        let [
            ProcessSlot {
                tree: Some(tree), ..
            },
        ] = self.slot.data.as_slice()
        else {
            return Err(io::ErrorKind::NotFound.into());
        };
        tree.require_stdin_complete()
    }

    /// Reads bounded native output through the retained invocation's meter.
    pub fn read_stdout(&mut self, buffer: &mut [u8]) -> io::Result<Option<usize>> {
        let meter = Arc::clone(self.slot.meter());
        self.tree_mut()?
            .read_stdout_controlled(buffer, Some(meter.as_ref()))
    }

    /// Observes exit while retaining containment and recovery ownership.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        let meter = Arc::clone(self.slot.meter());
        self.tree_mut()?.try_wait(Some(meter.as_ref()))
    }

    /// Stops descendants through the shared containment implementation.
    pub fn terminate_descendants(&mut self) -> io::Result<()> {
        let meter = Arc::clone(self.slot.meter());
        self.tree_mut()?
            .capture_terminate_descendants(Some(meter.as_ref()))
    }

    fn tree_mut(&mut self) -> io::Result<&mut ProcessTree> {
        self.slot_mut()?
            .as_mut()
            .ok_or_else(|| io::ErrorKind::NotFound.into())
    }

    fn slot_mut(&mut self) -> io::Result<&mut Option<ProcessTree>> {
        let [slot] = self.slot.data.as_mut_slice() else {
            return Err(io::ErrorKind::InvalidData.into());
        };
        Ok(&mut slot.tree)
    }
}

impl Drop for ProcessTreeStorage {
    fn drop(&mut self) {
        let [
            ProcessSlot {
                tree: Some(tree),
                retirement: Some(retirement),
            },
        ] = self.slot.data.as_mut_slice()
        else {
            return;
        };
        if !tree.is_reaped() {
            // The original work funds actual kill/wait attempts even after a
            // cancellation or exhausted allowance. Keep the durable record:
            // only the shared custody fence may declare descendants quiescent.
            tree.retire_owner(retirement);
        }
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
