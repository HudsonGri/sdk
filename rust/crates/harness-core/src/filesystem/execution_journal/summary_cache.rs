//! The authenticated suffix and its original accounting context live together.
use super::*;

#[derive(Default)]
pub(super) struct SummaryCache {
    summary: ExecutionSummary,
    io: Option<PreparationIo>,
}

impl SummaryCache {
    pub(super) fn bind(
        &mut self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io
            && let Err(error) = io.check_cancellation()
        {
            self.clear();
            return Err(error);
        }
        let same_ledger = match (&self.io, io) {
            (None, None) => true,
            (Some(retained), Some(io)) => retained.shares_ledger(io),
            _ => false,
        };
        if self.summary.operation != Some(operation) || !same_ledger {
            self.clear();
            self.io = io.map(PreparationIo::retained_pool).transpose()?;
            self.summary.operation = Some(operation);
        } else if let (Some(retained), Some(current)) = (&mut self.io, io) {
            retained.use_current_context(current);
        }
        Ok(())
    }

    fn clear(&mut self) {
        // Free values before their claims, including when an advance is cancelled.
        self.summary = ExecutionSummary::default();
        if let Some(io) = self.io.take() {
            io.release_retained_pool();
        }
    }

    pub(super) fn summary(&self) -> &ExecutionSummary {
        &self.summary
    }

    /// The proof includes the entire event, so altered references cannot reuse it.
    pub(super) fn authenticates(
        &self,
        operation: OperationId,
        sequence: u64,
        retry: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        if self.summary.operation != Some(operation) || sequence > self.summary.tail {
            return Ok(false);
        }
        let key = crate::model::hash_bytes(retry.as_bytes(), io)?;
        let digest = crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                event,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })?;
        if self.summary.retries.get(&key, io)? == Some(&(sequence, digest)) {
            Ok(true)
        } else {
            Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "authenticated execution prefix changed",
            ))
        }
    }

    pub(super) fn edit(&mut self) -> SummaryEdit<'_> {
        SummaryEdit {
            cache: self,
            complete: false,
        }
    }
}

impl Drop for SummaryCache {
    fn drop(&mut self) {
        self.clear();
    }
}

pub(super) struct SummaryEdit<'a> {
    cache: &'a mut SummaryCache,
    complete: bool,
}

impl SummaryEdit<'_> {
    pub(super) fn parts(&mut self) -> (&mut ExecutionSummary, Option<&PreparationIo>) {
        (&mut self.cache.summary, self.cache.io.as_ref())
    }

    pub(super) fn finish(mut self) {
        self.complete = true;
    }
}

impl Drop for SummaryEdit<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.cache.clear();
        }
    }
}

#[cfg(test)]
#[path = "summary_cache_tests.rs"]
mod tests;
