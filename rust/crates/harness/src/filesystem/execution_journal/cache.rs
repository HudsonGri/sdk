use super::*;

/// Disposable, authenticated prefix state. Field order drops maps before their
/// original allocation custody. The journal lock is acquired before provider,
/// task-file index or preparation-ledger locks; none may reenter this journal.
#[derive(Default)]
pub(super) struct VerifiedSummary {
    pub summary: ExecutionSummary,
    pub funding: Option<PreparationIo>,
}
impl VerifiedSummary {
    pub fn bind(&mut self, operation: OperationId, io: Option<&PreparationIo>) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let original = io.and_then(PreparationIo::original_native_work);
        #[cfg(not(target_arch = "wasm32"))]
        let same = match (
            original,
            self.funding
                .as_ref()
                .and_then(PreparationIo::original_native_work),
        ) {
            (None, None) => true,
            (Some(a), Some(b)) => a.shares_allocation_context(b),
            _ => false,
        };
        #[cfg(target_arch = "wasm32")]
        let same = self.funding.is_none();
        if !same
            || self
                .summary
                .operation
                .is_some_and(|found| found != operation)
        {
            self.clear();
        }
        if self.funding.is_none() {
            self.funding = PreparationIo::temporary(io)?;
        }
        Ok(())
    }
    pub fn clear(&mut self) {
        self.summary = ExecutionSummary::default();
        self.funding = None;
    }
}
