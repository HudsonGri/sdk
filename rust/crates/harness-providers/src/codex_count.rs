//! Codex has no public preflight count route. Keep usage plus a local delta.
use acyclic_harness_core::{Result, model::PreparedModelRequest};

use crate::usage_snapshot::{Snapshot, Usage};

#[derive(Default)]
pub(crate) struct Counter(Snapshot);

impl Counter {
    pub(crate) fn count(&self, request: &PreparedModelRequest) -> Result<u64> {
        let snapshot = self
            .0
            .get()
            .map_err(|_| crate::invalid("Codex usage lock poisoned"))?;
        let (prefix, total) = footprint(request, snapshot.map_or(0, |s| s.messages))?;
        Ok(estimate(snapshot, prefix, total))
    }
    pub(crate) fn observe(
        &self,
        request: &PreparedModelRequest,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        let usage = &metadata["usage"];
        // The footprint covers the request input, not the generated response.
        // Its output is charged once when it becomes input on a later request.
        if let Some(tokens) = usage["input_tokens"].as_u64() {
            let messages = request.request().messages.len();
            let (digest, bytes) = footprint(request, messages)?;
            self.0
                .set(Usage {
                    digest,
                    messages,
                    bytes,
                    tokens,
                })
                .map_err(|_| crate::invalid("Codex usage lock poisoned"))?;
        }
        Ok(())
    }
}

// If a resume, compaction or catalog change replaced the prefix, estimate the
// complete input. Never subtract an unrelated request's byte count.
fn estimate(snapshot: Option<Usage>, prefix: [u8; 32], total: u64) -> u64 {
    let (tokens, bytes) = snapshot
        .filter(|s| s.digest == prefix && s.bytes <= total)
        .map_or((0, 0), |s| (s.tokens, s.bytes));
    tokens.saturating_add(total.saturating_sub(bytes).div_ceil(3))
}

fn footprint(request: &PreparedModelRequest, prefix: usize) -> Result<([u8; 32], u64)> {
    use acyclic_harness_core::executor::json::{JsonBounds, json_to_writer_bounded};
    let bounds = JsonBounds {
        // Every serialized component belongs to this already admitted request.
        maximum_bytes: request.bytes().len() as u64,
        maximum_items: request.bytes().len() as u64,
        maximum_depth: 256,
    };
    let input = request.request();
    let mut hash = blake3::Hasher::new();
    let mut bytes = json_to_writer_bounded(
        &(&input.model, &input.tools),
        &mut hash,
        bounds,
        &mut |_| Ok(()),
    )?;
    let mut digest = *hash.finalize().as_bytes();
    for (index, message) in input.messages.iter().enumerate() {
        bytes = bytes
            .checked_add(json_to_writer_bounded(
                message,
                &mut hash,
                bounds,
                &mut |_| Ok(()),
            )?)
            .ok_or_else(|| crate::invalid("Codex input size overflow"))?;
        bytes = bytes
            .checked_add(crate::files::extra_bytes(&message.content, u64::MAX)? as u64)
            .ok_or_else(|| crate::invalid("Codex file input size overflow"))?;
        if index + 1 == prefix {
            digest = *hash.finalize().as_bytes();
        }
    }
    Ok((digest, bytes))
}

#[cfg(test)]
#[path = "codex_count_tests.rs"]
mod tests;
