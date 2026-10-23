use crate::{RowPosition, Top, WrappedLines};
use serde::{Deserialize, Serialize};

/// Stable source sections, in their existing display order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptPart {
    Reasoning,
    #[default]
    Answer,
    Tools,
}

impl TranscriptPart {
    fn index(self) -> usize {
        match self {
            Self::Reasoning => 0,
            Self::Answer => 1,
            Self::Tools => 2,
        }
    }
    fn at(index: usize) -> Self {
        match index {
            0 => Self::Reasoning,
            1 => Self::Answer,
            2 => Self::Tools,
            _ => unreachable!(),
        }
    }
}

/// Persisted top row. Absence means follow the live tail.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptAnchor {
    pub part: TranscriptPart,
    pub position: RowPosition,
}

fn top(parts: &[&mut WrappedLines; 3]) -> Option<TranscriptAnchor> {
    parts.iter().enumerate().find_map(|(index, part)| {
        (!part.shown().is_empty()).then(|| TranscriptAnchor {
            part: TranscriptPart::at(index),
            position: part.window().top,
        })
    })
}

fn tail(parts: &mut [&mut WrappedLines; 3], height: usize, activity: usize) {
    let mut remaining = height.saturating_sub(activity);
    for part in parts.iter_mut().rev() {
        if remaining == 0 {
            part.hide();
        } else {
            part.view(Top::Back(0), remaining);
            remaining -= part.shown().len();
        }
    }
}

/// Select a physical window without computing a total wrapped-row count.
/// `activity` is the derived activity row below the source sections.
pub fn view_transcript(
    mut parts: [&mut WrappedLines; 3],
    anchor: Option<TranscriptAnchor>,
    height: usize,
    activity: usize,
) -> Option<TranscriptAnchor> {
    if height == 0 {
        for part in &mut parts {
            part.hide();
        }
        return anchor;
    }
    let Some(anchor) = anchor else {
        tail(&mut parts, height, activity);
        return None;
    };
    for part in &mut parts {
        part.hide();
    }
    let mut remaining = height;
    let mut end = true;
    for (index, part) in parts.iter_mut().enumerate().skip(anchor.part.index()) {
        if part.source_len() == 0 {
            continue;
        }
        if remaining == 0 {
            end = false;
            break;
        }
        let position = if index == anchor.part.index() {
            anchor.position
        } else {
            RowPosition::default()
        };
        let position = RowPosition {
            line: position.line.min(part.source_len() - 1),
            row: position.row,
        };
        part.view_from(position, remaining);
        remaining -= part.shown().len();
        end &= part.at_tail();
    }
    if end && remaining >= activity {
        tail(&mut parts, height, activity);
        None
    } else {
        top(&parts)
    }
}

/// Apply one ordered movement, clamped against the composed source plus
/// activity row. Call repeatedly for a burst, then publish its final anchor.
pub fn move_transcript(
    mut parts: [&mut WrappedLines; 3],
    from: Option<TranscriptAnchor>,
    mut rows: isize,
    height: usize,
    activity: usize,
) -> Option<TranscriptAnchor> {
    if height == 0 {
        return from;
    }
    let from = view_transcript(
        parts.each_mut().map(|part| &mut **part),
        from,
        height,
        activity,
    );
    let mut anchor = from.or_else(|| top(&parts));
    if anchor.is_none() && rows < 0 {
        anchor = parts
            .iter_mut()
            .enumerate()
            .rev()
            .find_map(|(index, part)| {
                part.last_position().map(|position| TranscriptAnchor {
                    part: TranscriptPart::at(index),
                    position,
                })
            });
        rows = rows.saturating_add(1);
    }
    let mut anchor = anchor?;
    loop {
        let index = anchor.part.index();
        if let Some((position, remaining)) = parts[index].step(anchor.position, rows) {
            anchor.position = position;
            rows = remaining;
        }
        if rows == 0 {
            break;
        }
        let next = if rows < 0 {
            (0..index).rev().find(|&at| parts[at].source_len() != 0)
        } else {
            (index + 1..parts.len()).find(|&at| parts[at].source_len() != 0)
        };
        let Some(next) = next else {
            break;
        };
        anchor = TranscriptAnchor {
            part: TranscriptPart::at(next),
            position: if rows < 0 {
                parts[next].last_position().unwrap()
            } else {
                RowPosition::default()
            },
        };
        rows -= rows.signum();
    }
    view_transcript(parts, Some(anchor), height, activity)
}

#[cfg(test)]
mod tests;
