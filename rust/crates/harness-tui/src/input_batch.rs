//! One read/frame boundary shared by every terminal host.
/// Rows an input effect moves the focused view toward older output: a page is
/// the rendered transcript viewport, a wheel notch its fixed row count.
pub fn transcript_motion(effect: &crate::InputEffect, page_rows: usize) -> Option<isize> {
    use crate::{Action, InputEffect};
    let page = isize::try_from(page_rows).unwrap_or(isize::MAX);
    match effect {
        InputEffect::Host(Action::TranscriptUp) => Some(page),
        InputEffect::Host(Action::TranscriptDown) => Some(-page),
        InputEffect::Scroll(rows) => Some(*rows),
        _ => None,
    }
}

/// Effects for the next decoded input event plus the view motions decoded
/// in the same read behind it. A wheel flick or held `PageUp` arrives as one
/// input batch; its motions fold into one projection update and draw one frame.
/// Draining stops after the first event that is neither a motion nor an ignored
/// pointer. Events reading a presented viewport stay queued at the frame boundary.
/// Later events stay queued, and the next read is
/// never touched, so read boundaries stay frame boundaries.
pub fn input_burst<I: IntoIterator<Item = crate::InputEffect>>(
    batch: &mut std::collections::VecDeque<crate::key_event::InputEvent>,
    page_rows: usize,
    effects: &mut Vec<crate::InputEffect>,
    mut route: impl FnMut(crate::key_event::InputEvent) -> I,
) {
    effects.clear();
    while let Some(event) = batch.pop_front() {
        let ignored_pointer = matches!(
            &event,
            crate::key_event::InputEvent::Mouse(mouse)
                if !crate::PromptInput::reads_presented_viewport(mouse)
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
pub fn frame_due(batch: &std::collections::VecDeque<crate::key_event::InputEvent>) -> bool {
    use crate::{PromptInput, key_event::InputEvent};
    match batch.front() {
        None => true,
        Some(InputEvent::Mouse(mouse)) => PromptInput::reads_presented_viewport(mouse),
        Some(_) => false,
    }
}
