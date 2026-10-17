use crate::config::{ThemeRole, ThemeStyles};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

/// Transcript scrollbar over the current rendered window, with no stored offset.
pub struct TranscriptScrollbar<'a> {
    /// Rows above the viewport.
    pub top: usize,
    /// Maximum rows above the viewport.
    pub maximum: usize,
    /// Semantic styles from the followed theme.
    pub theme: &'a ThemeStyles,
}

fn geometry(maximum: usize, height: u16) -> (u16, u16) {
    let height = u128::from(height);
    if height == 0 {
        return (0, 0);
    }
    let thumb = (height * height / (maximum as u128 + height)).max(1);
    (thumb as u16, (height - thumb) as u16)
}

impl TranscriptScrollbar<'_> {
    /// Map a captured click/drag to a bounded transcript row. Pointer positions
    /// outside the track clamp to its ends; intermediate multiplication is wide.
    pub fn row_at(area: Rect, row: u16, maximum: usize) -> usize {
        let (thumb, travel) = geometry(maximum, area.height);
        if travel == 0 {
            return 0;
        }
        let at = row
            .saturating_sub(area.y)
            .saturating_sub(thumb / 2)
            .min(travel);
        (u128::from(at) * maximum as u128 / u128::from(travel)) as usize
    }
}

impl Widget for TranscriptScrollbar<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        if self.maximum == 0 || area.width == 0 || area.height == 0 {
            return;
        }
        let (thumb, travel) = geometry(self.maximum, area.height);
        let start =
            (self.top.min(self.maximum) as u128 * u128::from(travel) / self.maximum as u128) as u16;
        for y in 0..area.height {
            let active = start <= y && y < start + thumb;
            buffer[(area.x, area.y + y)]
                .set_symbol(if active { "█" } else { "░" })
                .set_style(self.theme.style(if active {
                    ThemeRole::Text
                } else {
                    ThemeRole::Placeholder
                }));
        }
    }
}
