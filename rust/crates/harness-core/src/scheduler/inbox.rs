use super::*;

/// One gapless committed task-inbox item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct InboxItem {
    /// Owning task.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub task_id: TaskId,
    /// One-based sequence.
    pub sequence: u64,
    /// Sender's idempotency identity.
    pub message_id: String,
    /// Pinned payload staged before publication.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub payload: FileRef,
}
