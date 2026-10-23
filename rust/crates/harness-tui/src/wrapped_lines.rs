use crate::{Editor, Layout, buffer::slice, widget::cells as cell_count};
use ratatui::{
    layout::Alignment,
    style::{Modifier, Style},
    text::{Line, Span},
};
use std::{collections::VecDeque, ops::Range};
#[cfg(any(test, feature = "test-support"))]
thread_local! { static WORK: std::cell::Cell<(usize,usize,usize)> = const { std::cell::Cell::new((0,0,0)) }; }

struct Source {
    offset: usize,
    styles: Vec<(usize, Style)>,
    style: Style,
    alignment: Option<Alignment>,
}

/// A displayed row: the `row`th wrapped row of source line `line`.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct RowPosition {
    /// Source line.
    pub line: usize,
    /// Wrapped row within the line.
    pub row: usize,
}

/// Where a frame's rows start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Top {
    /// Follow the tail. Off-tail positions always use a source anchor.
    Tail,
    /// At this row, or at the tail when fewer rows follow it.
    At(RowPosition),
}

/// The rows a frame shows: `rows()[start..end]`, beginning at `top`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RowWindow {
    /// First shown row in `rows()`.
    pub start: usize,
    /// End of the shown rows in `rows()`.
    pub end: usize,
    /// The first shown row's position.
    pub top: RowPosition,
}

/// One wrapping projection for ordinary and highlighted transcript rows,
/// wrapped only where a frame looks: source text, styles and the search index
/// follow every change, but rows exist only for a span of lines around the
/// shown window, so a resize or first frame wraps what is shown, not the
/// transcript.
#[derive(Default)]
pub struct WrappedLines {
    width: Option<u16>,
    sources: Vec<Source>,
    text: String,
    revision: u64,
    pub(crate) index: crate::transcript_search::index::Index,
    /// Source lines wrapped at `width`, and their rows in order.
    span: Range<usize>,
    layouts: VecDeque<Layout>,
    starts: Vec<usize>,
    rows: Vec<Line<'static>>,
    ranges: Vec<Range<usize>>,
    origins: Vec<(usize, usize)>,
    window: RowWindow,
}

impl WrappedLines {
    /// Return and clear this thread's actual painted rows, cells and layouts.
    #[cfg(any(test, feature = "test-support"))]
    pub fn take_work() -> (usize, usize, usize) {
        WORK.with(|work| work.replace((0, 0, 0)))
    }

    /// Apply a changed source suffix, then select the wrapping width.
    ///
    /// # Panics
    /// Panics if the projection revision overflows.
    pub fn update(&mut self, source: &[Line<'_>], changed: usize, width: u16) {
        self.replace(source, changed);
        self.reflow(width);
    }

    /// Ingest only changed source lines and update the search index. Rendering
    /// and resize never need the source slice or rebuild this index.
    ///
    /// # Panics
    /// Panics if the projection revision overflows.
    pub fn replace(&mut self, source: &[Line<'_>], changed: usize) {
        if changed >= source.len() && source.len() == self.sources.len() {
            return;
        }
        let changed = changed.min(source.len()).min(self.sources.len());
        assert!(self.revision < u64::MAX, "projection revision overflow");
        self.revision += 1;
        let at = self
            .sources
            .get(changed)
            .map_or(self.text.len(), |line| line.offset);
        self.text.truncate(at);
        self.sources.truncate(changed);
        self.truncate_span(changed);
        for line in source.get(changed..).unwrap_or_default() {
            let offset = self.text.len();
            let mut styles = Vec::new();
            let mut carriage_return = false;
            for span in &line.spans {
                let normalized = crate::buffer::normalize(&span.content);
                self.text
                    .push_str(if carriage_return && span.content.starts_with('\n') {
                        normalized.strip_prefix('\n').unwrap_or_default()
                    } else {
                        &normalized
                    });
                if !span.content.is_empty() {
                    carriage_return = span.content.ends_with('\r');
                }
                styles.push((self.text.len() - offset, span.style));
            }
            self.text.push('\n');
            self.sources.push(Source {
                offset,
                styles,
                style: line.style,
                alignment: line.alignment,
            });
        }
        let text = slice(&self.text, ..self.text.len().saturating_sub(1));
        self.index.replace_suffix(text, at.min(text.len()));
    }

    /// Change width without ingesting source text or walking the archive.
    ///
    /// # Panics
    /// Panics if the projection revision overflows.
    pub fn reflow(&mut self, width: u16) {
        if self.width != Some(width) {
            self.width = Some(width);
            self.clear_span();
            assert!(self.revision < u64::MAX, "projection revision overflow");
            self.revision += 1;
        }
    }

    /// Wrap only the lines a frame of `height` rows at `top` shows.
    /// Lines more than a frame away are released.
    pub fn view(&mut self, top: Top, height: usize) -> RowWindow {
        self.select(top, height, true)
    }

    /// Show rows starting at a source position, even when fewer than a page
    /// remain in this part. Composition clamps against all parts together.
    pub fn view_from(&mut self, position: RowPosition, height: usize) -> RowWindow {
        self.select(Top::At(position), height, false)
    }

    fn select(&mut self, top: Top, height: usize, clamp: bool) -> RowWindow {
        let len = self.sources.len();
        let mut window = None;
        if let Top::At(position) = top
            && position.line < len
        {
            if !(self.span.start..=self.span.end).contains(&position.line) || self.span.is_empty() {
                self.reset_span(position.line);
            }
            while position.line >= self.span.end {
                self.extend_back();
            }
            let line = position.line - self.span.start;
            let rows = self.line_rows(line);
            let first = rows.start + position.row.min(rows.len().saturating_sub(1));
            while self.rows.len() - first < height && self.span.end < len {
                self.extend_back();
            }
            if !clamp || self.rows.len() - first >= height {
                window = Some((first, first.saturating_add(height).min(self.rows.len())));
            }
        }
        let (start, end) = window.unwrap_or_else(|| {
            self.wrap_tail(height);
            let end = self.rows.len();
            if end >= height {
                (end - height, end)
            } else {
                (0, height.min(self.rows.len()))
            }
        });
        // Keep a frame of margin on each side, and every row to the tail
        // when the window is measured from it.
        let keep_back = matches!(top, Top::Tail) || window.is_none();
        let released = self.trim(
            start.saturating_sub(height),
            end.saturating_add(height),
            keep_back,
        );
        let (start, end) = (start - released, end - released);
        self.window = RowWindow {
            start,
            end,
            top: self.position_at(start),
        };
        self.window
    }

    /// Distance between two retained source rows; never wraps outside the cache.
    pub fn cached_distance(&self, from: RowPosition, to: RowPosition) -> Option<isize> {
        let rank = |position: RowPosition| {
            if !self.span.contains(&position.line) {
                return None;
            }
            let rows = self.line_rows(position.line - self.span.start);
            (position.row < rows.len()).then_some(rows.start + position.row)
        };
        isize::try_from(rank(to)?)
            .ok()?
            .checked_sub(isize::try_from(rank(from)?).ok()?)
    }

    /// The shown window, as selected by the last `view`.
    pub fn window(&self) -> RowWindow {
        self.window
    }

    /// Logical source lines; independent of which rows are wrapped or shown.
    pub fn source_len(&self) -> usize {
        self.sources.len()
    }

    /// Leave this part outside the composed window, retaining its small cache.
    pub fn hide(&mut self) {
        self.window = RowWindow::default();
    }

    /// Whether the shown window reaches this part's source tail.
    pub fn at_tail(&self) -> bool {
        self.span.end == self.sources.len() && self.window.end == self.rows.len()
    }

    /// Last source row, wrapping only its logical line when needed.
    pub fn last_position(&mut self) -> Option<RowPosition> {
        let line = self.sources.len().checked_sub(1)?;
        Some(RowPosition {
            line,
            row: self.row_count(line).saturating_sub(1),
        })
    }

    /// Walk within this part. Unconsumed motion crosses a part boundary in
    /// the composed view; no archive-wide row rank is computed.
    pub fn step(&mut self, from: RowPosition, rows: isize) -> Option<(RowPosition, isize)> {
        if self.sources.is_empty() {
            return None;
        }
        let from = RowPosition {
            line: from.line.min(self.sources.len() - 1),
            row: from.row,
        };
        let mut position = RowPosition {
            line: from.line,
            row: from.row.min(self.row_count(from.line).saturating_sub(1)),
        };
        let mut remaining = rows.unsigned_abs();
        while remaining != 0 {
            if rows < 0 {
                let moved = remaining.min(position.row);
                position.row -= moved;
                remaining -= moved;
                if remaining == 0 || position.line == 0 {
                    break;
                }
                position.line -= 1;
                position.row = self.row_count(position.line).saturating_sub(1);
            } else {
                let available = self
                    .row_count(position.line)
                    .saturating_sub(position.row + 1);
                let moved = remaining.min(available);
                position.row += moved;
                remaining -= moved;
                if remaining == 0 || position.line + 1 == self.sources.len() {
                    break;
                }
                position.line += 1;
                position.row = 0;
            }
            remaining -= 1;
        }
        let remaining = isize::try_from(remaining).unwrap_or(isize::MAX);
        Some((position, if rows < 0 { -remaining } else { remaining }))
    }

    #[cfg(test)]
    fn move_by(
        &mut self,
        from: Option<RowPosition>,
        rows: isize,
        height: usize,
    ) -> Option<RowPosition> {
        let top = from.map_or(Top::Tail, Top::At);
        let from = self.view(top, height).top;
        let (position, _) = self.step(from, rows)?;
        let window = self.view(Top::At(position), height);
        (!self.at_tail()).then_some(window.top)
    }

    fn row_count(&mut self, line: usize) -> usize {
        if !self.span.contains(&line) {
            if line.checked_add(1) == Some(self.span.start) {
                self.extend_front();
            } else {
                if line != self.span.end {
                    self.reset_span(line);
                }
                self.extend_back();
            }
        }
        self.line_rows(line - self.span.start).len()
    }

    /// Wrap every line and show every row: for short projections (reasoning,
    /// side panels) that are always shown whole.
    pub fn wrap_all(&mut self) -> &[Line<'static>] {
        self.view(Top::Tail, usize::MAX);
        self.shown()
    }

    /// Where `byte` of the normalized text is shown: its line and wrapped row.
    pub fn position_of(&self, byte: usize) -> RowPosition {
        self.locate(byte).0
    }

    /// The source range of the wrapped row showing `byte`.
    pub(crate) fn row_range_at(&self, byte: usize) -> Range<usize> {
        self.locate(byte).1
    }

    fn locate(&self, byte: usize) -> (RowPosition, Range<usize>) {
        let line = self
            .sources
            .partition_point(|source| source.offset <= byte)
            .saturating_sub(1);
        let Some(source) = self.sources.get(line) else {
            return (RowPosition::default(), 0..0);
        };
        if self.span.contains(&line) {
            let index = line - self.span.start;
            let rows = self.line_rows(index);
            let ranges = self.ranges.get(rows).unwrap_or_default();
            let row = ranges
                .partition_point(|range| range.start <= byte)
                .saturating_sub(1);
            let range = ranges.get(row).cloned().unwrap_or(byte..byte);
            return (RowPosition { line, row }, range);
        }
        // Outside the wrapped span: wrap this one line without retaining it.
        let layout = self.layout(line);
        let mut found = (RowPosition { line, row: 0 }, byte..byte);
        for (row, (_, wrapped)) in visible_rows(&layout).enumerate() {
            if source.offset + wrapped.start > byte && row > 0 {
                break;
            }
            found = (
                RowPosition { line, row },
                source.offset + wrapped.start..source.offset + wrapped.end,
            );
        }
        found
    }

    /// Normalized logical text, shared by wrapping and Unicode search.
    pub fn text(&self) -> &str {
        slice(&self.text, ..self.text.len().saturating_sub(1))
    }

    /// Changes only when source rows or wrapping change, never on frame ticks.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Source offsets of the wrapped rows (`rows()` indexing).
    pub fn ranges(&self) -> &[Range<usize>] {
        &self.ranges
    }

    /// The wrapped rows around the shown window; `window()` selects the shown
    /// ones. No per-frame transcript clone is needed.
    pub fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }

    /// The shown rows: `rows()[window]`.
    pub fn shown(&self) -> &[Line<'static>] {
        self.rows
            .get(self.window.start..self.window.end)
            .unwrap_or_default()
    }

    fn position_at(&self, row: usize) -> RowPosition {
        self.origins
            .get(row)
            .map_or(RowPosition::default(), |&(line, _)| RowPosition {
                line,
                row: row
                    - self
                        .starts
                        .get(line - self.span.start)
                        .copied()
                        .unwrap_or(0),
            })
    }

    fn line_rows(&self, index: usize) -> Range<usize> {
        self.starts.get(index).copied().unwrap_or(self.rows.len())
            ..self
                .starts
                .get(index + 1)
                .copied()
                .unwrap_or(self.rows.len())
    }

    fn layout(&self, line: usize) -> Layout {
        #[cfg(any(test, feature = "test-support"))]
        WORK.with(|work| {
            let (rows, cells, layouts) = work.get();
            work.set((rows, cells, layouts + 1));
        });
        let Some(source) = self.sources.get(line) else {
            return Layout::default();
        };
        let end = self
            .sources
            .get(line + 1)
            .map_or(self.text.len(), |next| next.offset)
            - 1;
        Layout::new(
            &Editor::new(slice(&self.text, source.offset..end)),
            usize::from(self.width.unwrap_or(1)),
        )
    }

    fn clear_span(&mut self) {
        self.reset_span(0);
    }

    fn reset_span(&mut self, line: usize) {
        self.span = line..line;
        self.layouts.clear();
        self.starts.clear();
        self.rows.clear();
        self.ranges.clear();
        self.origins.clear();
        self.window = RowWindow::default();
    }

    /// Release wrapped lines from `line` on, after their source changed.
    fn truncate_span(&mut self, line: usize) {
        if line <= self.span.start {
            self.clear_span();
            return;
        }
        if line >= self.span.end {
            return;
        }
        let index = line - self.span.start;
        let row = self.starts.get(index).copied().unwrap_or(self.rows.len());
        self.layouts.truncate(index);
        self.starts.truncate(index);
        self.rows.truncate(row);
        self.ranges.truncate(row);
        self.origins.truncate(row);
        self.span.end = line;
        self.window = RowWindow::default();
    }

    /// Wrap from the last line upwards until `rows` rows (or every line) are
    /// wrapped, re-starting at the tail when the span is far from it.
    fn wrap_tail(&mut self, rows: usize) {
        let len = self.sources.len();
        if len - self.span.end > rows || self.span.is_empty() {
            self.reset_span(len);
        }
        while self.span.end < len {
            self.extend_back();
        }
        while self.rows.len() < rows && self.span.start > 0 {
            self.extend_front();
        }
    }

    fn wrap(&self, line: usize) -> (Layout, Vec<Line<'static>>, Vec<Range<usize>>) {
        let layout = self.layout(line);
        let Some(source) = self.sources.get(line) else {
            return (layout, Vec::new(), Vec::new());
        };
        let mut rows = Vec::new();
        paint(source, &layout, &mut rows);
        let ranges = visible_rows(&layout)
            .map(|(_, row)| source.offset + row.start..source.offset + row.end)
            .collect();
        (layout, rows, ranges)
    }

    fn extend_back(&mut self) {
        let line = self.span.end;
        let (layout, rows, ranges) = self.wrap(line);
        self.starts.push(self.rows.len());
        self.origins
            .extend(visible_rows(&layout).map(|(index, _)| (line, index)));
        self.rows.extend(rows);
        self.ranges.extend(ranges);
        self.layouts.push_back(layout);
        self.span.end += 1;
    }

    fn extend_front(&mut self) {
        let line = self.span.start - 1;
        let (layout, rows, ranges) = self.wrap(line);
        let count = rows.len();
        for start in &mut self.starts {
            *start += count;
        }
        self.starts.insert(0, 0);
        self.origins.splice(
            0..0,
            visible_rows(&layout)
                .map(|(index, _)| (line, index))
                .collect::<Vec<_>>(),
        );
        self.rows.splice(0..0, rows);
        self.ranges.splice(0..0, ranges);
        self.layouts.push_front(layout);
        self.span.start = line;
    }

    /// Release lines wholly before row `keep` and, unless `keep_back`, wholly
    /// at or after row `until`. Returns the rows released before `keep`.
    fn trim(&mut self, keep: usize, until: usize, keep_back: bool) -> usize {
        let lines = self
            .starts
            .partition_point(|&start| start <= keep)
            .saturating_sub(1);
        let front = self.starts.get(lines).copied().unwrap_or(0);
        if !keep_back {
            let end = self
                .starts
                .partition_point(|&start| start < until)
                .max(lines + 1);
            if end < self.starts.len() {
                let row = self.starts.get(end).copied().unwrap_or(self.rows.len());
                self.layouts.truncate(end);
                self.starts.truncate(end);
                self.rows.truncate(row);
                self.ranges.truncate(row);
                self.origins.truncate(row);
                self.span.end = self.span.start + end;
            }
        }
        if lines == 0 {
            return 0;
        }
        self.layouts.drain(..lines);
        self.starts.drain(..lines);
        for start in &mut self.starts {
            *start -= front;
        }
        self.rows.drain(..front);
        self.ranges.drain(..front);
        self.origins.drain(..front);
        self.span.start += lines;
        front
    }

    pub(crate) fn render_ranges(
        &self,
        row: (usize, usize),
        matches: &[Range<usize>],
        selected: Option<&Range<usize>>,
        selection: Style,
        area: ratatui::layout::Rect,
        buffer: &mut ratatui::buffer::Buffer,
    ) {
        let (row, source_offset) = row;
        let area = area.intersection(buffer.area);
        if area.is_empty() {
            return;
        }
        let Some(base) = self.rows.get(row) else {
            return;
        };
        buffer.set_style(area, base.style);
        let Some(&(line, index)) = self.origins.get(row) else {
            return;
        };
        let Some(source) = self.sources.get(line) else {
            return;
        };
        let Some(layout_row) = self
            .layouts
            .get(line - self.span.start)
            .and_then(|layout| layout.rows.get(index))
        else {
            return;
        };
        let cells = &layout_row.cells;
        crate::transcript_search::view::charge(cells.len(), false);
        #[cfg(any(test, feature = "test-support"))]
        WORK.with(|work| {
            let (rows, count, layouts) = work.get();
            work.set((rows + 1, count + cells.len(), layouts));
        });
        let mut next = 0;
        let (mut displayed, mut base_span, mut base_end) = (0, 0, 0);
        let inset = match source.alignment.unwrap_or(ratatui::layout::Alignment::Left) {
            ratatui::layout::Alignment::Left => 0,
            ratatui::layout::Alignment::Center => {
                area.width.saturating_sub(cell_count(base.width())) / 2
            }
            ratatui::layout::Alignment::Right => {
                area.width.saturating_sub(cell_count(base.width()))
            }
        };
        for cell in cells {
            let at = source_offset + source.offset + cell.source.start;
            while matches.get(next).is_some_and(|range| range.end <= at) {
                next += 1;
            }
            while base
                .spans
                .get(base_span)
                .is_some_and(|span| displayed >= base_end + span.content.len())
            {
                base_end += base
                    .spans
                    .get(base_span)
                    .map_or(0, |span| span.content.len());
                base_span += 1;
            }
            let mut style = base.style.patch(
                base.spans
                    .get(base_span)
                    .map_or(Style::default(), |span| span.style),
            );
            displayed += cell.symbol.len();
            if matches.get(next).is_some_and(|range| {
                range.start < source_offset + source.offset + cell.source.end && range.end > at
            }) {
                style = style.patch(selection).add_modifier(Modifier::UNDERLINED);
            }
            if selected.is_some_and(|range| {
                range.start < source_offset + source.offset + cell.source.end && range.end > at
            }) {
                style = style.add_modifier(Modifier::BOLD | Modifier::REVERSED);
            }
            let x = area
                .x
                .saturating_add(inset)
                .saturating_add(cell_count(cell.column));
            if x < area.right() && area.height > 0 {
                if buffer[(x, area.y)].symbol() == cell.symbol {
                    buffer[(x, area.y)].set_style(style);
                } else {
                    buffer.set_stringn(
                        x,
                        area.y,
                        &cell.symbol,
                        usize::from(area.right() - x),
                        style,
                    );
                }
            }
        }
    }
}

fn visible_rows(layout: &Layout) -> impl Iterator<Item = (usize, &crate::layout::Row)> {
    layout.rows.iter().enumerate().filter(|(index, row)| {
        !row.cells.is_empty()
            || index
                .checked_sub(1)
                .and_then(|previous| layout.rows.get(previous))
                .is_none_or(|previous| {
                    previous.end != row.start
                        || previous
                            .cells
                            .last()
                            .is_none_or(|cell| cell.column + cell.width != layout.width)
                })
    })
}

// Cache normal display spans once; search borrows their styles and symbols.
fn paint(source: &Source, layout: &Layout, output: &mut Vec<Line<'static>>) {
    for (_, row) in visible_rows(layout) {
        #[cfg(any(test, feature = "test-support"))]
        WORK.with(|work| {
            let (rows, cells, layouts) = work.get();
            work.set((rows + 1, cells + row.cells.len(), layouts));
        });
        let mut spans = Vec::with_capacity(usize::from(!row.cells.is_empty()));
        let mut group = 0;
        let mut previous = None;
        for (index, cell) in row.cells.iter().enumerate() {
            let style_index = source
                .styles
                .partition_point(|(end, _)| *end <= cell.source.start);
            let style = source
                .styles
                .get(style_index)
                .map_or(Style::default(), |(_, style)| *style);
            if let Some(previous) = previous.filter(|previous| *previous != style) {
                spans.push(span(
                    row.cells.get(group..index).unwrap_or_default(),
                    previous,
                ));
                group = index;
            }
            previous = Some(style);
        }
        if let Some(style) = previous {
            spans.push(span(row.cells.get(group..).unwrap_or_default(), style));
        }
        let mut line = Line::from(spans).style(source.style);
        line.alignment = source.alignment;
        output.push(line);
    }
}

fn span(cells: &[crate::layout::Cell], style: Style) -> Span<'static> {
    let mut text = String::with_capacity(cells.iter().map(|cell| cell.symbol.len()).sum());
    for cell in cells {
        text.push_str(&cell.symbol);
    }
    Span::styled(text, style)
}

#[cfg(test)]
#[path = "wrapped_lines/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "wrapped_lines/viewport_tests.rs"]
mod viewport_tests;
