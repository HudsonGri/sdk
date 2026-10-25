use super::*;

pub(in crate::executor) async fn stage_bytes(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    bytes: &[u8],
    bind_content: bool,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    validate_journal(journal, io)?;
    let bound = bind_payload(key, bytes, bind_content, io)?;
    let reference = journal
        .stage(
            operation,
            bound.key.as_deref().unwrap_or(key),
            bytes,
            "application/json",
            io,
        )?
        .await?;
    check_staged(&reference, bytes, &bound.digest, io)?;
    Ok(reference)
}

/// Stages canonical JSON payloads as one group commit; see
/// [`ExecutionJournal::stage_all`]. Each payload's key binds its content.
pub(in crate::executor) async fn stage_json_all(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    payloads: &[(&str, &[u8])],
    io: Option<&PreparationIo>,
) -> Result<Vec<FileRef>> {
    validate_journal(journal, io)?;
    let mut bound = crate::model::vec_with_capacity(payloads.len(), io)?;
    for (key, bytes) in payloads {
        bound.push(bind_payload(key, bytes, true, io)?);
    }
    let mut staged = crate::model::vec_with_capacity(payloads.len(), io)?;
    for ((key, bytes), bound) in payloads.iter().zip(&bound) {
        staged.push(crate::executor::StagedPayload {
            idempotency_key: bound.key.as_deref().unwrap_or(key),
            bytes,
            media_type: "application/json",
        });
    }
    let references = journal.stage_all(operation, &staged, io)?.await?;
    if references.len() != payloads.len() {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "execution journal staged a different payload count",
        ));
    }
    for ((reference, (_, bytes)), bound) in references.iter().zip(payloads).zip(&bound) {
        check_staged(reference, bytes, &bound.digest, io)?;
    }
    Ok(references)
}

/// A payload's retry key, optionally bound to its content, and its digest.
struct BoundPayload<'io> {
    key: Option<String>,
    digest: [u8; 32],
    _key_hold: Hold<'io>,
}

/// A ledger hold on bytes a staging step retains.
#[cfg(feature = "filesystem")]
pub(in crate::executor) type Hold<'io> = Option<crate::filesystem::DecodeReservation<'io>>;
/// A ledger hold on bytes a staging step retains.
#[cfg(not(feature = "filesystem"))]
pub(in crate::executor) type Hold<'io> = Option<std::marker::PhantomData<&'io ()>>;

fn bind_payload<'io>(
    key: &str,
    bytes: &[u8],
    bind_content: bool,
    io: Option<&'io PreparationIo>,
) -> Result<BoundPayload<'io>> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.preflight(acyclic_fs::WorkCounters {
            bytes_hashed: bytes.len() as u64 * (1 + u64::from(bind_content)),
            ..Default::default()
        })?;
    }
    use sha2::Digest as _;
    let digest: [u8; 32] = sha2::Sha256::digest(bytes).into();
    let identity = bind_content.then(|| blake3::hash(bytes));
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.spent(acyclic_fs::WorkCounters {
            bytes_hashed: bytes.len() as u64 * (1 + u64::from(bind_content)),
            ..Default::default()
        })?;
    }
    let copied = identity
        .map(|identity| {
            with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!("{key}:{}", identity.to_hex()),
                    work,
                )
            })
        })
        .transpose()?;
    #[cfg(feature = "filesystem")]
    let _key_hold = io
        .zip(copied.as_ref())
        .map(|(io, copied)| io.guard_owned(copied.capacity() as u64))
        .transpose()?;
    #[cfg(not(feature = "filesystem"))]
    let _key_hold = None;
    Ok(BoundPayload {
        key: copied,
        digest,
        _key_hold,
    })
}

fn check_staged(
    reference: &FileRef,
    bytes: &[u8],
    digest: &[u8; 32],
    io: Option<&PreparationIo>,
) -> Result<()> {
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
        || reference.descriptor().byte_length() != bytes.len() as u64
        || reference.descriptor().sha256() != digest
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "execution journal staged different private JSON bytes",
        ));
    }
    Ok(())
}

/// A value's canonical JSON bytes, held against the preparation ledger.
pub(in crate::executor) fn encode_canonical<'io, T: Serialize>(
    value: &T,
    io: Option<&'io PreparationIo>,
) -> Result<(acyclic_stream::OwnedBytes, Hold<'io>)> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let bytes = io.encode(value, io.remaining()?.bytes_encoded, true)?;
        let hold = io.guard_bytes(&bytes)?;
        return Ok((bytes, hold));
    }
    let _ = io;
    Ok((
        acyclic_stream::OwnedBytes::from(crate::contract::canonical_json_bytes(value)?),
        None,
    ))
}

pub(crate) async fn stage_json<T: Serialize>(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    value: &T,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    validate_journal(journal, io)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let hold;
        let bytes = io.encode(value, io.remaining()?.bytes_encoded, true)?;
        hold = io.guard_bytes(&bytes)?;
        let result = stage_bytes(journal, operation, key, &bytes, true, Some(io)).await;
        drop(bytes);
        drop(hold);
        return result;
    }
    let bytes = crate::contract::canonical_json_bytes(value)?;
    stage_bytes(journal, operation, key, &bytes, true, None).await
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_size_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native stock logical payload size overflow",
        })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_size_slots(capacity: usize, size: usize) -> Result<u64> {
    (capacity as u64)
        .checked_mul(size as u64)
        .ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native stock logical payload size overflow",
        })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_events_owned_bytes(events: &Vec<ModelEvent>) -> Result<u64> {
    let slots = native_size_slots(events.capacity(), std::mem::size_of::<ModelEvent>())?;
    events.iter().try_fold(
        native_size_add(std::mem::size_of::<Vec<ModelEvent>>() as u64, slots)?,
        |bytes, event| {
            native_size_add(
                bytes,
                event
                    .logical_owned_bytes()?
                    .checked_sub(std::mem::size_of::<ModelEvent>() as u64)
                    .ok_or_else(|| crate::Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "native event metadata extent differs",
                    })?,
            )
        },
    )
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_output_payload_bytes(output: &TurnOutput) -> Result<u64> {
    let slots = native_size_slots(
        output.attachments.capacity(),
        std::mem::size_of::<Attachment>(),
    )?;
    let mut bytes = native_size_add(output.text.capacity() as u64, slots)?;
    bytes = native_size_add(
        bytes,
        crate::executor::json::json_value_retained_owned_bytes(&output.metadata)?,
    )?;
    for attachment in &output.attachments {
        bytes = native_size_add(bytes, attachment.file.logical_owned_bytes()?)?;
        if let Some(label) = &attachment.label {
            bytes = native_size_add(bytes, label.capacity() as u64)?;
        }
    }
    Ok(bytes)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_progress_owned_bytes(
    progress: &ExecutionProgress<TurnOutput>,
) -> Result<u64> {
    let root = std::mem::size_of::<ExecutionProgress<TurnOutput>>() as u64;
    match progress {
        ExecutionProgress::PreDispatchFailed(failure) => {
            native_size_add(root, failure.owned_bytes())
        }
        ExecutionProgress::Ready(output) => {
            native_size_add(root, native_output_payload_bytes(output)?)
        }
        ExecutionProgress::Pending(_)
        | ExecutionProgress::Rejected(_)
        | ExecutionProgress::Failed(_) => Ok(root),
    }
}
