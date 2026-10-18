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

/// Publish a page movement through the current physical layout, without
/// counting or wrapping archived rows to discover a global scroll maximum.
pub async fn scroll_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    up: bool,
    page_rows: usize,
    maximum_modal: usize,
    mut move_view: impl FnMut(Option<TranscriptAnchor>, isize) -> Option<TranscriptAnchor>,
) -> Result<()> {
    let mut layout = layout(files)?;
    if modal(&layout) {
        let back = modal_scroll(&layout)?;
        let current = back.min(maximum_modal);
        let next = if up {
            current.saturating_add(page_rows).min(maximum_modal)
        } else {
            current.saturating_sub(page_rows)
        };
        if next == back {
            return Ok(());
        }
        layout["view_back"] = serde_json::json!(next);
    } else {
        let from = anchor(&layout)?;
        let rows = isize::try_from(page_rows).unwrap_or(isize::MAX);
        let next = move_view(from, if up { -rows } else { rows });
        if next == from {
            return Ok(());
        }
        if let Some(next) = next {
            layout["transcript_top"] =
                serde_json::to_value(next).map_err(|error| Error::Invalid(error.to_string()))?;
        } else {
            layout
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("layout must be an object".into()))?
                .remove("transcript_top");
        }
    }
    files
        .write_state(CommandFileRoot::Session, "layout.json", &layout)
        .await?;
    Ok(())
}

/// Publish the track's resolved source identity through the same layout file.
pub(super) async fn seek_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    next: Option<TranscriptAnchor>,
) -> Result<()> {
    let mut value = layout(files)?;
    if modal(&value) || value["open_view"] == "feedback" || anchor(&value)? == next {
        return Ok(());
    }
    if let Some(next) = next {
        value["transcript_top"] =
            serde_json::to_value(next).map_err(|error| Error::Invalid(error.to_string()))?;
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
