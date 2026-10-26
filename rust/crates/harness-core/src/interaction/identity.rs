//! The one derivation of an interaction's identity.
use crate::{InteractionId, OperationId, Result};

/// Domain of every interaction identity. Retained interactions and agent
/// approval files are keyed by it, so it never changes.
const DOMAIN: &[u8] = b"harness/v2/task-interaction\0";

/// The interaction that call `operation` opens under durable owner `owner`.
///
/// `owner` is the 16-byte identity of the execution that owns the call: a task
/// id in the task loop; for an agent turn, its execution operation, which
/// `agent::turn_id` derives from the agent path and the turn input. An agent's
/// interactions are therefore keyed by (agent path, turn, call), and every replay
/// of a call reopens the same interaction and the same approval files.
pub fn interaction_id(
    owner: [u8; 16],
    operation: OperationId,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<InteractionId> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
        io.hashed_bytes((DOMAIN.len() + 32) as u64)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    hasher.update(&owner);
    hasher.update(&operation.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(InteractionId::from_bytes(bytes))
}
