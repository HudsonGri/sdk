//! Navigation is an ordinary followed layout-file change.
use super::{CommandFileRoot, CommandSession};
use crate::{Error, Result, tui::TranscriptAnchor};
use acyclic_stream::StreamProvider;

fn modal(layout: &serde_json::Value) -> bool {
    layout.get("open_view").and_then(serde_json::Value::as_str) == Some("shortcuts")
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
    files
        .state_ref(CommandFileRoot::Session, "layout.json")?
        .map(modal_scroll)
        .transpose()
        .map(|rows| rows.unwrap_or(0))
}

/// Missing source position follows the live tail.
pub fn tui_anchor<P: StreamProvider + ?Sized>(
    files: &CommandSession<P>,
) -> Result<Option<TranscriptAnchor>> {
    files
        .state_ref(CommandFileRoot::Session, "layout.json")?
        .map(anchor)
        .transpose()
        .map(Option::flatten)
}

/// One movement applied against the bounds of the rendered viewport.
fn scroll_step(current: usize, rows: isize, maximum: usize) -> usize {
    if rows >= 0 {
        current.saturating_add(rows.unsigned_abs()).min(maximum)
    } else {
        current.saturating_sub(rows.unsigned_abs())
    }
}

fn folded_motion(
    motions: impl IntoIterator<Item = isize>,
    maximum: usize,
) -> impl Fn(usize) -> usize {
    let (mut minimum, mut ceiling, mut forward, mut backward) = (0, maximum, 0_usize, 0_usize);
    for rows in motions {
        minimum = scroll_step(minimum, rows, maximum);
        ceiling = scroll_step(ceiling, rows, maximum);
        let distance = rows.unsigned_abs();
        let (same, opposite) = if rows >= 0 {
            (&mut forward, &mut backward)
        } else {
            (&mut backward, &mut forward)
        };
        let cancelled = distance.min(*opposite);
        *opposite -= cancelled;
        *same = same.saturating_add(distance - cancelled).min(maximum);
    }
    move |current| {
        current
            .min(maximum)
            .saturating_sub(backward)
            .saturating_add(forward)
            .clamp(minimum, ceiling)
    }
}

/// Fold ordered motions against the latest followed source window, publishing once.
pub async fn scroll_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    motions: impl IntoIterator<Item = isize, IntoIter: Clone>,
    maximum_modal: usize,
    mut move_view: impl FnMut(Option<TranscriptAnchor>, isize) -> Option<TranscriptAnchor>,
) -> Result<()> {
    let motions = motions.into_iter();
    let modal_motion = folded_motion(motions.clone(), maximum_modal);
    files
        .update_state(CommandFileRoot::Session, "layout.json", |value| {
            if modal(value) {
                let back = modal_scroll(value)?;
                let next = modal_motion(back);
                if next == back {
                    return Ok(false);
                }
                value
                    .as_object_mut()
                    .ok_or_else(|| Error::Invalid("layout must be an object".into()))?
                    .insert("view_back".into(), serde_json::json!(next));
            } else {
                let from = anchor(value)?;
                let next = motions.clone().fold(from, |current, rows| {
                    move_view(current, rows.saturating_neg())
                });
                if next == from {
                    return Ok(false);
                }
                set_anchor(value, next)?;
            }
            Ok(true)
        })
        .await
}

fn set_anchor(value: &mut serde_json::Value, next: Option<TranscriptAnchor>) -> Result<()> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("layout must be an object".into()))?;
    if let Some(next) = next {
        if next.origin.is_none() {
            return Err(Error::Invalid(
                "saved transcript anchor requires source identity".into(),
            ));
        }
        object.insert(
            "transcript_top".into(),
            serde_json::to_value(next).map_err(|error| Error::Invalid(error.to_string()))?,
        );
    } else {
        object.remove("transcript_top");
    }
    Ok(())
}

/// Publish the track's resolved source identity through the same layout file.
pub(super) async fn seek_tui<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
    next: Option<TranscriptAnchor>,
) -> Result<()> {
    files
        .update_state(CommandFileRoot::Session, "layout.json", |value| {
            if modal(value) || value["open_view"] == "feedback" || anchor(value)? == next {
                return Ok(false);
            }
            set_anchor(value, next)?;
            Ok(true)
        })
        .await
}

#[cfg(test)]
mod motion_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn folded_motions_equal_each_bounded_step(
            motions in prop::collection::vec(any::<isize>(), 0..64),
            maximum in any::<usize>(), current in any::<usize>(),
        ) {
            let expected = motions.iter().fold(current.min(maximum), |at, rows| scroll_step(at, *rows, maximum));
            prop_assert_eq!(folded_motion(motions, maximum)(current), expected);
        }
    }

    #[test]
    #[should_panic(expected = "each movement keeps its boundary")]
    fn summing_motions_loses_intermediate_bounds_negative_control() {
        assert_eq!(
            scroll_step(0, 100 - 1, 10),
            folded_motion([100, -1], 10)(0),
            "each movement keeps its boundary"
        );
    }
}

#[cfg(test)]
mod source_admission_tests {
    use super::*;

    #[test]
    fn bare_window_positions_cannot_cross_the_persistence_boundary() {
        let mut layout = serde_json::json!({"peer":"retained"});
        let before = layout.clone();
        assert!(set_anchor(&mut layout, Some(TranscriptAnchor::default())).is_err());
        assert_eq!(layout, before);
        layout["transcript_top"] = serde_json::to_value(TranscriptAnchor::default()).unwrap();
        assert!(anchor(&layout).is_err());
    }
}
