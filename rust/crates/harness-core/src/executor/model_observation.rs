//! Inline small model observations without a separate staged generation.
use super::*;

pub(crate) fn matches_turn(event: &ExecutionEvent, step: u32, purpose: ModelPurpose) -> bool {
    matches!(event,
        ExecutionEvent::Model { step: recorded, purpose: recorded_purpose, .. }
        | ExecutionEvent::ModelInline { step: recorded, purpose: recorded_purpose, .. }
        if *recorded == step && *recorded_purpose == purpose)
}

/// Records one observation; see [`append_all`].
#[cfg(test)]
pub(crate) async fn append(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    turn: (u32, ModelPurpose),
    model: &ModelEvent,
    original: Option<&PreparationIo>,
) -> Result<()> {
    append_all(
        journal,
        operation,
        turn,
        (&[key], std::slice::from_ref(model)),
        original,
        &mut 0,
    )
    .await
}

/// Records model observations that are ready together, in order. Each is
/// inline when the journal admits its size; the rest are staged as one
/// group commit (one generation) before any of them is appended, so a crash
/// leaves staged-but-unappended payloads that recovery adopts. `appended`
/// counts the observations appended so far, for a caller that resumes.
pub(crate) async fn append_all(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    turn: (u32, ModelPurpose),
    (keys, models): (&[&str], &[ModelEvent]),
    original: Option<&PreparationIo>,
    appended: &mut usize,
) -> Result<()> {
    validate_journal(journal, original)?;
    if keys.len() != models.len() {
        return Err(stock_boundary(
            original,
            crate::wire::ErrorCode::Invalid,
            "model observations and keys differ in number",
        ));
    }
    if models.is_empty() {
        return Ok(());
    }
    let maximum = journal.inline_model_event_bytes();
    let (step, purpose) = turn;
    #[cfg(feature = "filesystem")]
    let scope = crate::filesystem::PreparationScope::new(original)?;
    #[cfg(feature = "filesystem")]
    let io = scope.io();
    #[cfg(not(feature = "filesystem"))]
    let io = original;
    let mut encoded = crate::model::vec_with_capacity(models.len(), io)?;
    for model in models {
        encoded.push(encode_canonical(model, io)?);
    }
    let inline = |bytes: &[u8]| maximum != 0 && bytes.len() as u64 <= maximum;
    let mut payloads = crate::model::vec_with_capacity(models.len(), io)?;
    for (key, (bytes, _)) in keys.iter().zip(&encoded) {
        if !inline(bytes) {
            payloads.push((*key, bytes.as_ref()));
        }
    }
    let mut staged = if payloads.is_empty() {
        Vec::new()
    } else {
        stage_json_all(journal, operation, &payloads, io).await?
    }
    .into_iter();
    drop(payloads);
    super::model_group_faults::crash_point().await;
    for (key, (bytes, _)) in keys.iter().zip(&encoded) {
        let event = if inline(bytes) {
            let event = std::str::from_utf8(bytes).map_err(|_| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "canonical model event is not UTF-8",
                )
            })?;
            ExecutionEvent::ModelInline {
                step,
                purpose,
                event: stock_string_copy(event, io)?,
            }
        } else {
            ExecutionEvent::Model {
                step,
                purpose,
                event: staged.next().ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model observation group lost a member",
                    )
                })?,
            }
        };
        journal.append(operation, key, &event, io)?.await?;
        *appended += 1;
    }
    Ok(())
}

pub(crate) async fn load(
    journal: &dyn ExecutionJournal,
    event: &ExecutionEvent,
    io: Option<&PreparationIo>,
) -> Result<Option<ModelEvent>> {
    Ok(match event {
        ExecutionEvent::Model { event, .. } => Some(retain_preparation(
            load_json::<ModelEvent>(journal, event, io).await?,
        )?),
        ExecutionEvent::ModelInline { event, .. } => {
            if event.len() as u64 > journal.inline_model_event_bytes() {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "inline model observation exceeds journal capability",
                ));
            }
            Some(retain_preparation(super::payload::decode_json::<
                ModelEvent,
            >(event.as_bytes(), io)?)?)
        }
        _ => None,
    })
}
