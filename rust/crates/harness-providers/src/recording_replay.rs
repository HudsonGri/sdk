//! Recorded bodies enter the same decoder as live dispatch.
use crate::{Adapter, stream};
use acyclic_harness_core::{
    OperationId, Result,
    http::recording::Exchange,
    model::{ModelDispatch, ModelEvent},
};
use futures::TryStreamExt as _;

/// Decode an admitted exchange using its declared adapter and the caller's
/// original event bound. This performs no HTTP request or credential lookup.
pub async fn decode_recording(exchange: &Exchange, maximum: usize) -> Result<Vec<ModelEvent>> {
    let adapter: Adapter = serde_json::from_value(serde_json::Value::String(
        exchange.provenance().identity.adapter.clone(),
    ))
    .map_err(|_| crate::invalid("recording has an unknown provider adapter"))?;
    let body = Box::pin(futures::stream::iter(
        exchange
            .chunks()
            .iter()
            .map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk))),
    ));
    stream::events_named(
        adapter,
        body,
        maximum,
        ModelDispatch {
            operation_id: OperationId::from_bytes([0; 16]),
            step: 0,
            request_digest: [0; 32],
        },
        None,
        None,
    )
    .try_collect()
    .await
}
