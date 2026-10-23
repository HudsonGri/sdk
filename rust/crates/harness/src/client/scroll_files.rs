//! Navigation is an ordinary followed layout-file change.
use super::{CommandFileRoot, CommandSession};
use crate::{Error, Result, tui::TranscriptAnchor};
use acyclic_stream::StreamProvider;

fn modal(layout: &serde_json::Value) -> bool {
    layout.get("open_view").and_then(serde_json::Value::as_str) == Some("shortcuts")
}

fn layout<P: StreamProvider + ?Sized>(files: &CommandSession<P>) -> Result<serde_json::Value> {
    let value = files
        .state(CommandFileRoot::Session, "layout.json")?
        .unwrap_or_else(|| serde_json::json!({}));
    if !value.is_object() {
        return Err(Error::Invalid("layout must be an object".into()));
    }
    Ok(value)
}

fn modal_scroll(layout: &serde_json::Value) -> Result<usize> {
    if !modal(layout) {
        return Ok(0);
    }
    let Some(value) = layout.get("view_back") else {
        return Ok(usize::MAX);
    };
    let rows = value
        .as_u64()
        .ok_or_else(|| Error::Invalid("scroll offset must be a row count".into()))?;
    usize::try_from(rows).map_err(|error| Error::Invalid(error.to_string()))
}

fn anchor(layout: &serde_json::Value) -> Result<Option<TranscriptAnchor>> {
    layout
        .get("transcript_top")
        .map(|value| {
            serde_json::from_value(value.clone()).map_err(|error| Error::Invalid(error.to_string()))
        })
        .transpose()
}

/// Help owns a separate bounded row offset; transcript movement uses source identity.
pub fn tui_modal_scroll<P: StreamProvider + ?Sized>(files: &CommandSession<P>) -> Result<usize> {
    modal_scroll(&layout(files)?)
}

/// Missing source position follows the live tail.
pub fn tui_anchor<P: StreamProvider + ?Sized>(
    files: &CommandSession<P>,
) -> Result<Option<TranscriptAnchor>> {
    anchor(&layout(files)?)
}

/// Rows an input effect moves the focused view toward older output: a page is
/// the rendered transcript viewport, a wheel notch its fixed row count.
pub fn transcript_motion(effect: &crate::tui::InputEffect, page_rows: usize) -> Option<isize> {
    use crate::tui::{Action, InputEffect};
    let page = isize::try_from(page_rows).unwrap_or(isize::MAX);
    match effect {
        InputEffect::Host(Action::TranscriptUp) => Some(page),
        InputEffect::Host(Action::TranscriptDown) => Some(-page),
        InputEffect::Scroll(rows) => Some(*rows),
        _ => None,
    }
}

/// One movement applied against the bounds of the rendered viewport.
fn scroll_step(current: usize, rows: isize, maximum: usize) -> usize {
    if rows >= 0 {
        current.saturating_add(rows.unsigned_abs()).min(maximum)
    } else {
        current.saturating_sub(rows.unsigned_abs())
    }
}

/// Fold ordered motions against the current source window, publishing once.
pub async fn scroll_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    motions: impl IntoIterator<Item = isize>,
    maximum_modal: usize,
    mut move_view: impl FnMut(Option<TranscriptAnchor>, isize) -> Option<TranscriptAnchor>,
) -> Result<()> {
    let mut value = layout(files)?;
    if modal(&value) {
        let back = modal_scroll(&value)?;
        let next = motions
            .into_iter()
            .fold(back.min(maximum_modal), |current, rows| {
                scroll_step(current, rows, maximum_modal)
            });
        if next == back {
            return Ok(());
        }
        value
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("layout must be an object".into()))?
            .insert("view_back".into(), serde_json::json!(next));
    } else {
        let from = anchor(&value)?;
        let next = motions.into_iter().fold(from, |current, rows| {
            move_view(current, rows.saturating_neg())
        });
        if next == from {
            return Ok(());
        }
        if let Some(next) = next {
            value
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("layout must be an object".into()))?
                .insert(
                    "transcript_top".into(),
                    serde_json::to_value(next)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                );
        } else {
            value
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("layout must be an object".into()))?
                .remove("transcript_top");
        }
    }
    files
        .write_state(CommandFileRoot::Session, "layout.json", &value)
        .await?;
    Ok(())
}

/// Publish the track's resolved source identity through the same layout file.
pub(super) async fn seek_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    next: Option<TranscriptAnchor>,
) -> Result<()> {
    let mut value = layout(files)?;
    if modal(&value)
        || value.get("open_view").and_then(serde_json::Value::as_str) == Some("feedback")
        || anchor(&value)? == next
    {
        return Ok(());
    }
    if let Some(next) = next {
        value
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("layout must be an object".into()))?
            .insert(
                "transcript_top".into(),
                serde_json::to_value(next).map_err(|error| Error::Invalid(error.to_string()))?,
            );
    } else {
        value
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("layout must be an object".into()))?
            .remove("transcript_top");
    }
    files
        .write_state(CommandFileRoot::Session, "layout.json", &value)
        .await?;
    Ok(())
}

/// Effects for the next decoded input event plus the view motions decoded
/// in the same read behind it. A wheel flick or held `PageUp` arrives as one
/// input batch; its motions publish as one layout change and draw one frame.
/// Draining stops after the first event that is neither a motion nor an ignored
/// pointer. Events reading a presented viewport stay queued at the frame boundary.
/// Later events stay queued, and the next read is
/// never touched, so read boundaries stay frame boundaries.
pub fn input_burst<I: IntoIterator<Item = crate::tui::InputEffect>>(
    batch: &mut std::collections::VecDeque<crate::tui::key_event::InputEvent>,
    page_rows: usize,
    effects: &mut Vec<crate::tui::InputEffect>,
    mut route: impl FnMut(crate::tui::key_event::InputEvent) -> I,
) {
    effects.clear();
    while let Some(event) = batch.pop_front() {
        let ignored_pointer = matches!(
            &event,
            crate::tui::key_event::InputEvent::Mouse(mouse)
                if !crate::tui::PromptInput::reads_presented_viewport(mouse)
        );
        let first = effects.len();
        effects.extend(route(event));
        let foldable = (effects.len() > first || ignored_pointer)
            && effects[first..]
                .iter()
                .all(|effect| transcript_motion(effect, page_rows).is_some());
        if !foldable || frame_due(batch) {
            break;
        }
    }
}

/// Whether the frame must reach the terminal before the batch's next event is
/// routed. Only left-button down/drag can hit-test the presented prompt viewport;
/// all other events route against recomputed layout without presenting. A read
/// presents once at its end, plus a boundary before each queued left down/drag.
pub fn frame_due(batch: &std::collections::VecDeque<crate::tui::key_event::InputEvent>) -> bool {
    use crate::tui::{PromptInput, key_event::InputEvent};
    match batch.front() {
        None => true,
        Some(InputEvent::Mouse(mouse)) => PromptInput::reads_presented_viewport(mouse),
        Some(_) => false,
    }
}
