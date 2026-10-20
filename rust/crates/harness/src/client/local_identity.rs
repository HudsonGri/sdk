//! Launch identity belongs to a submission, independent of its prompt bytes.
use crate::OperationId;

/// Allocate a new launch unless the caller explicitly resumes a submission.
/// Clients retain this identity in their request/outbox across reconnects;
/// entering the same text in another invocation is a new submission.
#[must_use]
pub fn local_identity(explicit: Option<OperationId>) -> OperationId {
    explicit.unwrap_or_else(OperationId::new)
}
