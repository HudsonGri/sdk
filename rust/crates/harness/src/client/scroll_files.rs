//! Transcript navigation is an ordinary followed layout-file change.
use super::{CommandFileRoot, CommandSession};
use crate::{Error, Result};
use acyclic_stream::StreamProvider;

fn modal(layout: &serde_json::Value) -> bool {
    layout.get("open_view").and_then(serde_json::Value::as_str) == Some("shortcuts")
}

/// Rows retained above the followed view tail. Transcript starts at its tail;
/// modal help starts at its first row and retains a separate navigation offset.
pub fn tui_scroll<P: StreamProvider + ?Sized>(files: &CommandSession<P>) -> Result<usize> {
    let layout = files.state(CommandFileRoot::Session, "layout.json")?;
    let is_modal = layout.as_ref().is_some_and(modal);
    let key = if is_modal {
        "view_back"
    } else {
        "transcript_back"
    };
    let Some(value) = layout.as_ref().and_then(|layout| layout.get(key)) else {
        return Ok(if is_modal { usize::MAX } else { 0 });
    };
    let rows = value
        .as_u64()
        .ok_or_else(|| Error::Invalid("scroll offset must be a row count".into()))?;
    usize::try_from(rows).map_err(|error| Error::Invalid(error.to_string()))
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

/// Publish a movement bounded by the actual rendered transcript viewport. A
/// movement that changes nothing appends nothing.
pub async fn scroll_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    rows: isize,
    maximum: usize,
) -> Result<()> {
    let back = tui_scroll(files)?;
    let current = back.min(maximum);
    let next = if rows >= 0 {
        current.saturating_add(rows.unsigned_abs()).min(maximum)
    } else {
        current.saturating_sub(rows.unsigned_abs())
    };
    if next != back {
        let mut layout = files
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or_else(|| serde_json::json!({}));
        let key = if modal(&layout) {
            "view_back"
        } else {
            "transcript_back"
        };
        layout[key] = serde_json::json!(next);
        files
            .write_state(CommandFileRoot::Session, "layout.json", &layout)
            .await?;
    }
    Ok(())
}

/// Effects for one input event plus the view motions the stream has already
/// decoded behind it, without waiting. A wheel flick or held `PageUp` arrives
/// as one input batch; its wheel notches and page keys merge into one scroll,
/// so the caller publishes one layout change and draws one frame for the
/// whole burst. Draining stops after the first event that is not purely a
/// motion; that event is routed here, in order, against the same layout,
/// which a motion never changes. Polling once is cancel-safe: a stream keeps
/// any in-flight read itself.
pub fn input_burst<S>(
    first: crate::tui::key_event::InputEvent,
    input: &mut S,
    page_rows: usize,
    mut route: impl FnMut(crate::tui::key_event::InputEvent) -> Vec<crate::tui::InputEffect>,
) -> Result<Vec<crate::tui::InputEffect>>
where
    S: futures::Stream<Item = Result<crate::tui::key_event::InputEvent>> + Unpin,
{
    use crate::tui::InputEffect;
    use futures::{FutureExt, StreamExt};
    let mut effects = Vec::new();
    let mut event = first;
    loop {
        let routed = route(event);
        let motion = !routed.is_empty()
            && routed
                .iter()
                .all(|effect| transcript_motion(effect, page_rows).is_some());
        effects.extend(routed.into_iter().map(|effect| {
            match transcript_motion(&effect, page_rows) {
                Some(rows) => InputEffect::Scroll(rows),
                None => effect,
            }
        }));
        if !motion {
            break;
        }
        match input.next().now_or_never() {
            Some(Some(next)) => event = next?,
            Some(None) | None => break,
        }
    }
    Ok(crate::tui::coalesce_scrolls(effects))
}
