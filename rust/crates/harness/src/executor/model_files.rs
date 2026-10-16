use super::*;

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

pub(crate) const TEXT_DELTA: &str = "text/x-acyclic-model-text";
pub(crate) const REASONING_DELTA: &str = "text/x-acyclic-model-reasoning";

pub(crate) async fn stage_event_json<J: ExecutionJournal + ?Sized>(
    journal: &J,
    operation: OperationId,
    key: &str,
    event: &ModelEvent,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let bytes: acyclic_stream::OwnedBytes = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => io.encode(event, io.remaining()?.bytes_encoded, true)?,
        _ => crate::contract::canonical_json_bytes(event)?.into(),
    };
    #[cfg(feature = "filesystem")]
    let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
    let reference = journal
        .stage(operation, key, &bytes, "application/json", io)?
        .await?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "model event is not private JSON",
        ));
    }
    with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
    Ok(reference)
}

pub(crate) async fn load_model_event<'a>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&'a PreparationIo>,
) -> Result<(ModelEvent, Option<crate::conversation::ContentHold<'a>>)> {
    if reference.volume().class() != VolumeClass::AgentPrivate {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "model event is not private content",
        ));
    }
    let bytes = journal.load(reference, io)?.await?;
    #[cfg(feature = "filesystem")]
    let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
    with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
    decode_model_event(reference, &bytes, io)
}

pub(crate) fn decode_model_event<'a>(
    reference: &FileRef,
    bytes: &[u8],
    io: Option<&'a PreparationIo>,
) -> Result<(ModelEvent, Option<crate::conversation::ContentHold<'a>>)> {
    let reasoning = match reference.descriptor().media_type() {
        "application/json" => return super::payload::decode_json(bytes, io),
        TEXT_DELTA => false,
        REASONING_DELTA => true,
        _ => {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "unknown model file media type",
            ));
        }
    };
    let text = std::str::from_utf8(bytes).map_err(|_| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "model delta is not UTF-8",
        )
    })?;
    #[cfg(feature = "filesystem")]
    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.retain_owned(std::mem::size_of::<ModelEvent>() as u64)?;
    }
    let delta = stock_string_copy(text, io)?;
    #[cfg(feature = "filesystem")]
    let hold = io
        .map(|io| {
            let owned = io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "model delta ownership underflows",
                )
            })?;
            io.guard_owned(owned)
        })
        .transpose()?;
    #[cfg(not(feature = "filesystem"))]
    let hold = None;
    Ok((
        if reasoning {
            ModelEvent::Reasoning { delta }
        } else {
            ModelEvent::Content { delta }
        },
        hold,
    ))
}
