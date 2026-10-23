use super::*;

pub(crate) fn stock_boundary(
    io: Option<&PreparationIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return crate::Error::NativeBoundary { code, reason };
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

pub(crate) fn stock_string_copy(value: &str, io: Option<&PreparationIo>) -> Result<String> {
    let mut copied = String::new();
    stock_string_push(&mut copied, value, u64::MAX, "stock text size overflow", io)?;
    Ok(copied)
}

pub(in crate::executor) fn stock_selection_copy(
    value: &ModelContextSelection,
    io: Option<&PreparationIo>,
) -> Result<ModelContextSelection> {
    let mut ids = Vec::new();
    for id in &value.message_ids {
        stock_vec_push(&mut ids, *id, io)?;
    }
    let checkpoint = value
        .checkpoint
        .as_ref()
        .map(|reference| crate::model::clone_file(reference, io))
        .transpose()?;
    Ok(ModelContextSelection {
        conversation_revision: value.conversation_revision,
        message_ids: ids,
        checkpoint,
    })
}

pub(crate) fn stock_vec_push<T>(
    values: &mut Vec<T>,
    value: T,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            crate::store::history::reserve(values, values.len() + 1, work)?;
        }
        Ok::<_, Error>(())
    })?;
    values.push(value);
    Ok(())
}

/// Reserves retained index storage, including a relocating allocation's peak.
pub(crate) fn stock_vec_reserve<T>(
    values: &mut Vec<T>,
    capacity: usize,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if capacity <= values.capacity() {
        return Ok(());
    }
    let overflow = || {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "index storage overflows",
        )
    };
    let copied = values
        .len()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(overflow)? as u64;
    #[cfg(feature = "filesystem")]
    let _peak = io
        .map(|io| {
            let bytes = values
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(overflow)? as u64;
            io.reserve_decode(bytes)
        })
        .transpose()?;
    with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.admit_copy(copied)?;
            work.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                bytes: 0,
                items: values.len() as u64,
            })?;
            crate::store::history::reserve(values, capacity, work)?;
            work.copy_bytes(copied)?;
        } else {
            values
                .try_reserve_exact(capacity - values.len())
                .map_err(Error::Allocation)?;
        }
        Ok(())
    })
}

pub(crate) fn stock_string_push(
    text: &mut String,
    suffix: &str,
    maximum: u64,
    reason: &'static str,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let target = text
        .len()
        .checked_add(suffix.len())
        .filter(|target| *target as u64 <= maximum)
        .ok_or_else(|| stock_boundary(io, crate::wire::ErrorCode::Invalid, reason))?;
    with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.admit_copy(suffix.len() as u64)?;
            work.reserve_text(text, target)?;
            text.push_str(suffix);
            work.copy_bytes(suffix.len() as u64)
        } else {
            text.push_str(suffix);
            Ok(())
        }
    })
}
