//! Checked ownership admission for history event and snapshot decoding.
use super::{JsonBounds, JsonCodecCharge, JsonValueShape, Result, layout::LayoutCost};
use crate::{
    conversation::{Attachment, ConversationMessage, FileRef},
    core::{ExtensionConfiguration, ExtensionDependency},
    fork::{CapturedResource, ForkOmission, ForkSeed, ReferenceGrant, SharedGrant},
    merge::ProjectMergeReceipt,
};
use serde::__private229::de::Content;
use serde_json::Value;
use std::mem::size_of;
const EVENT_LABEL_BYTES: u64 = 3994;
const COMMON_LABEL_BYTES: u64 = 317;
/// Static diagnostic allowance for the SDK's preparation records.
pub const fn preparation_serde_diagnostic_label_bytes() -> u64 {
    9862
}
/// Owned diagnostic quota for an admitted typed value.
pub fn json_typed_diagnostic_owned_bytes(
    shape: JsonValueShape,
    labels: u64,
    code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    json_typed_diagnostic_source_owned_bytes(shape.input_bytes(), labels, code)
}
pub(crate) fn json_typed_diagnostic_source_owned_bytes(
    source_bytes: u64,
    labels: u64,
    code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    multiply_with_code(diagnostic_bytes(source_bytes, labels, code)?, 2, code)
}
pub(crate) fn audited_serde_diagnostic_bytes(input: u64, labels: u64) -> Result<u64> {
    diagnostic_bytes(input, labels, None)
}
fn diagnostic_bytes(input: u64, labels: u64, code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    let fixed = sum_with_code(
        &[labels, COMMON_LABEL_BYTES, 2 * u64::from(usize::BITS)],
        code,
    )?;
    sum_with_code(
        &[
            multiply_with_code(input, 12, code)?,
            multiply_with_code(fixed, 2, code)?,
        ],
        code,
    )
}
fn multiply_with_code(
    value: u64,
    factor: u64,
    code: Option<crate::wire::ErrorCode>,
) -> Result<u64> {
    value
        .checked_mul(factor)
        .ok_or_else(|| super::boundary("event JSON admission overflow", code))
}
fn sum_with_code(values: &[u64], code: Option<crate::wire::ErrorCode>) -> Result<u64> {
    values.iter().try_fold(0u64, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| super::boundary("event JSON admission overflow", code))
    })
}
/// Maximum owned bytes and diagnostic bytes of one typed decode.
///
/// A snapshot passes the heap its layout walk charged for every described
/// position; other records keep the slot bound over their lexical items.
pub(crate) fn json_decode_owned_bytes(
    shape: JsonValueShape,
    layout: Option<LayoutCost>,
    code: Option<crate::wire::ErrorCode>,
) -> Result<(u64, u64)> {
    let mul = |value, factor| multiply_with_code(value, factor, code);
    let sum = |values: &[u64]| sum_with_code(values, code);
    let input = shape.input_bytes();
    let virtual_bytes =
        super::json_value_virtual_owned_bytes(shape).map_err(|error| match (code, error) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            (Some(code), crate::Error::NativeBoundary { reason, .. }) => {
                crate::Error::NativeBoundary { code, reason }
            }
            (_, error) => error,
        })?;
    if let Some(layout) = layout {
        // The layout walk charged every string and the escape scratch. The
        // decoder stops at its first error, which quotes at most one string
        // or key of the document beside the static labels.
        let diagnostic = sum(&[
            diagnostic_bytes(layout.longest(), EVENT_LABEL_BYTES, code)?,
            mul(SNAPSHOT_DIAGNOSTIC_BYTES + 4, 2)?,
        ])?;
        let maximum = sum(&[layout.bytes(), virtual_bytes, diagnostic])?;
        return Ok((maximum, diagnostic));
    }
    let diagnostic = diagnostic_bytes(input, EVENT_LABEL_BYTES, code)?;
    // Decoded strings, escape scratch and number text stay within these
    // multiples of the source bytes.
    let source = sum(&[mul(input, 6)?, mul(input.max(8), 2)?])?;
    let items = shape.lexical_items();
    let slots = mul(items, 10)?;
    let content_slot = 4 * size_of::<(Content<'static>, Content<'static>)>()
        .max(size_of::<Content<'static>>()) as u64;
    let typed_slot = [
        size_of::<ExtensionDependency>(),
        size_of::<ExtensionConfiguration>(),
        size_of::<CapturedResource>(),
        size_of::<ForkOmission>(),
        size_of::<SharedGrant>(),
        size_of::<ReferenceGrant>(),
        size_of::<Attachment>(),
        size_of::<(String, FileRef)>(),
    ]
    .into_iter()
    .fold(0, usize::max) as u64;
    // Event collection elements larger than a Value are the object-shaped
    // records audited above. Digest bytes, UUIDs, strings and enum labels do
    // not each allocate a CapturedResource/ReferenceGrant slot. Bound those
    // larger slots by the original object/array openings, retaining the
    // existing growth factor. Content buffering and scalar collections still
    // use every lexical item.
    let typed_items = if shape.unresolved_raw() {
        items
    } else {
        shape.lexical_containers().unwrap_or(items)
    };
    let value_slot = size_of::<Value>().max(size_of::<(String, Value)>()) as u64;
    let boxed_slot = [
        size_of::<ForkSeed>(),
        size_of::<ProjectMergeReceipt>(),
        size_of::<ConversationMessage>(),
        size_of::<FileRef>(),
    ]
    .into_iter()
    .fold(0, usize::max) as u64;
    let maximum = sum(&[
        mul(slots, content_slot + value_slot)?,
        mul(mul(typed_items, 10)?, typed_slot)?,
        virtual_bytes,
        boxed_slot,
        source,
        diagnostic,
    ])?;
    Ok((maximum, diagnostic))
}

pub(super) const SNAPSHOT_DIAGNOSTIC_BYTES: u64 = 784;
pub(crate) fn snapshot_json_admission<F: FnMut(JsonCodecCharge) -> crate::Result<()> + ?Sized>(
    bytes: &[u8],
    bounds: JsonBounds,
    charge: &mut F,
) -> crate::Result<u64> {
    let (value_shape, layout) = super::admission::admit_json_layout(
        bytes,
        JsonBounds {
            maximum_depth: bounds.maximum_depth.min(256),
            ..bounds
        },
        super::JsonValueSchema::Snapshot,
        &crate::core::SNAPSHOT_LAYOUT,
        &mut |event| charge(event),
    )?;
    let (maximum_owned_bytes, _) = json_decode_owned_bytes(value_shape, Some(layout), None)?;
    charge(JsonCodecCharge::Admission {
        maximum_owned_bytes,
        value_shape,
    })?;
    Ok(maximum_owned_bytes)
}
