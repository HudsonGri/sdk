//! Actor records project through the existing Markdown, wrapping and device
//! machinery. This cache owns no follow, transport or authoritative history.

use super::{Line, line, pacing::Pacer};
use crate::{
    agent::{AgentPath, Phase, StatusRecord},
    tui::{self, PromptInput, Screen, SourceRows, TranscriptAnchor, WrappedLines},
};
use std::collections::VecDeque;

const MAXIMUM_TURNS: usize = 16;
const MAXIMUM_BYTES: usize = 4 * 1024 * 1024;

struct TurnRows {
    sequence: u64,
    bytes: usize,
    rows: usize,
}

/// Bounded disposable rows for the selected agent. Complete turns are evicted
/// together; surviving Markdown and wrapping state is never parsed again.
pub struct AgentRenderer {
    agent: AgentPath,
    turns: VecDeque<TurnRows>,
    bytes: usize,
    rows: WrappedLines,
    origins: SourceRows,
    top: Option<TranscriptAnchor>,
    height: usize,
    busy: bool,
    observed_status: bool,
    status: Line<'static>,
    notices: Vec<Line<'static>>,
    overlay: Vec<Line<'static>>,
    overlay_top: usize,
    search: tui::TranscriptSearch,
    search_open: bool,
    search_index: i64,
    pacing: Pacer,
}

impl AgentRenderer {
    /// Bind the disposable projection to one selected agent. Switching agents
    /// constructs another cache rather than mixing their sequence spaces.
    pub fn new(agent: AgentPath) -> Self {
        Self {
            agent,
            turns: VecDeque::new(),
            bytes: 0,
            rows: WrappedLines::default(),
            origins: SourceRows::default(),
            top: None,
            height: 0,
            busy: false,
            observed_status: false,
            status: Line::default(),
            notices: Vec::new(),
            overlay: Vec::new(),
            overlay_top: 0,
            search: tui::TranscriptSearch::default(),
            search_open: false,
            search_index: 0,
            pacing: Pacer::default(),
        }
    }

    /// Drop presentation rows no longer owned by the shared stream window.
    pub fn discard_before(&mut self, sequence: u64) {
        while self.turns.front().is_some_and(|turn| turn.sequence < sequence) {
            self.discard_first();
        }
    }

    fn discard_first(&mut self) {
        if let Some(turn) = self.turns.pop_front() {
            self.bytes -= turn.bytes;
            self.rows.replace_range(0..turn.rows, &[]);
            self.origins.discard(turn.rows);
            self.resolve_top();
        }
    }

    fn resolve_top(&mut self) {
        if let Some(top) = &mut self.top {
            top.position.line = top.origin.and_then(|origin| self.origins.resolve(origin)).unwrap_or(0);
            top.origin = self.origins.origin(top.position.line);
        }
    }

    /// Project one immutable selected turn once. The original output file is
    /// read by LocalAgentClient under its authenticated bounded reader.
    pub fn append(&mut self, sequence: u64, text: &str) -> crate::Result<()> {
        if self.turns.back().is_some_and(|turn| turn.sequence >= sequence) {
            return Err(crate::Error::Invalid("agent presentation sequence must advance".into()));
        }
        if text.len() > MAXIMUM_BYTES {
            return Err(crate::Error::Invalid("agent presentation exceeds byte limit".into()));
        }
        while self.turns.len() >= MAXIMUM_TURNS || self.bytes + text.len() > MAXIMUM_BYTES {
            self.discard_first();
        }
        // Each record is a complete UTF-8 turn, so open Markdown blocks cannot
        // cross turn boundaries. Use the one parser and style conversion;
        // retain no TaskVolume source identity or duplicate text decoder.
        let mut markdown = acyclic_markdown::Markdown::default();
        markdown.push(text);
        let mut rows: Vec<_> = markdown.lines().iter().map(line).collect();
        rows.push(Line::default());
        let start = self.rows.source_len();
        self.rows.replace_range(start..start, &rows);
        let mut identity = blake3::Hasher::new();
        identity.update(self.agent.as_str().as_bytes());
        identity.update(&sequence.to_le_bytes());
        self.origins.extend(*identity.finalize().as_bytes(), start + rows.len());
        self.turns.push_back(TurnRows { sequence, bytes: text.len(), rows: rows.len() });
        self.bytes += text.len();
        self.request_frame();
        Ok(())
    }

    /// Lifecycle is displayed only after an actual shared-session delivery.
    pub fn status(&mut self, status: &StatusRecord) {
        self.observed_status = true;
        self.busy = matches!(&status.phase, Phase::Running);
        self.status = Line::raw(tui::display_text(&format!("{:?}", status.phase)).into_owned());
        self.request_frame();
    }

    /// Keep bounded input feedback separate from authoritative actor records.
    pub fn notice(&mut self, message: &str) {
        let message = if message.len() > 4096 {
            let end = message.char_indices().map(|(index, _)| index)
                .take_while(|index| *index <= 4096).last().unwrap_or(0);
            &message[..end]
        } else { message };
        self.notices.clear();
        self.notices.push(Line::raw(tui::display_text(message).into_owned()));
        self.request_frame();
    }

    /// The composed transcript width determines wrapping. Only visible rows
    /// are materialized, including during resize and scroll.
    pub fn layout(&mut self, input: &PromptInput, size: tui::ratatui::layout::Size) {
        let area = tui::ratatui::layout::Rect::new(0, 0, size.width, size.height);
        let areas = self.screen().areas(area, input);
        self.rows.reflow(areas.transcript.width);
        self.height = usize::from(areas.transcript.height);
        self.overlay_top = self.overlay_top.min(self.overlay.len()
            .saturating_sub(self.height.saturating_sub(2)));
        self.resolve_top();
        if self.search_open {
            self.search.update(
                &self.rows, input.editor.text(), self.search_index,
                input.theme.style(tui::config::ThemeRole::Selection),
            );
            self.top = self.search.selected_anchor(&self.rows, self.search_index).or(self.top);
        }
        self.top = tui::view_transcript(
            [None, Some(&mut self.rows), None], self.top, self.height, usize::from(self.busy),
        );
        if let Some(top) = &mut self.top {
            top.origin = self.origins.origin(top.position.line);
        }
        if self.search_open { self.search.prepare(&self.rows, 0, self.height); }
    }

    /// Reuse the lazy transcript index only while Find is open. The prompt
    /// controller preserves the chat draft separately from the search editor.
    pub fn open_search(&mut self, input: &mut PromptInput) {
        self.overlay.clear();
        self.search_open = true;
        self.search_index = 0;
        input.set_transcript_search(Some(""));
        self.request_frame();
    }

    /// Navigate matches without rescanning or copying the retained transcript.
    pub fn search_step(&mut self, previous: bool) {
        if self.search_open {
            self.search_index = self.search_index.saturating_add(if previous { -1 } else { 1 });
            self.request_frame();
        }
    }

    /// One bounded background slice from the shared search implementation.
    /// There is no task or timer when no search work remains.
    pub async fn search_progress(&mut self) {
        if !self.search_open || !self.search.pending() { std::future::pending::<()>().await; }
        tokio::task::yield_now().await;
        self.search.advance(&self.rows, 4096);
        self.request_frame();
    }

    /// Display the same descriptors and keymap used by input dispatch.
    pub fn help(&mut self, catalog: &crate::client::CommandCatalog, input: &mut PromptInput) {
        self.close_panel(input);
        self.overlay_top = 0;
        self.overlay.push(Line::raw("Commands"));
        for command in catalog.commands() {
            let aliases = command.aliases.iter().map(|alias| format!(" /{alias}")).collect::<String>();
            self.overlay.push(Line::raw(tui::display_text(
                &format!("/{}{} · {}", command.name, aliases, command.description),
            ).into_owned()));
        }
        self.overlay.push(Line::raw("Keys"));
        self.overlay.extend(input.keymap.help().lines().map(|line|
            Line::raw(tui::display_text(&line.replace('\t', " · ")).into_owned())));
        self.request_frame();
    }

    /// Close presentation state before Escape can cancel an actor turn.
    pub fn close_panel(&mut self, input: &mut PromptInput) -> bool {
        let was_open = self.search_open || !self.overlay.is_empty();
        self.overlay.clear();
        self.search_open = false;
        self.search.project(&self.rows, false);
        self.rows.clear_search_index();
        input.set_transcript_search(None);
        if was_open { self.request_frame(); }
        was_open
    }

    /// Seek only within this selected source window. Source identity, rather
    /// than a stale row number, determines the anchor after eviction.
    pub fn seek(&mut self, top: Option<TranscriptAnchor>) {
        self.top = top;
        self.resolve_top();
        self.request_frame();
    }

    /// Resolve a track hit through the maintained byte/row index, then attach
    /// this agent's stable source origin. No transcript scan or bare persisted
    /// row number crosses the input boundary.
    pub fn anchor_at(&mut self, byte: usize) -> Option<TranscriptAnchor> {
        let anchor = Some(TranscriptAnchor {
            part: tui::TranscriptPart::Answer,
            position: self.rows.position_of(byte.min(self.rows.text().len())),
            origin: None,
        });
        tui::view_transcript([None, Some(&mut self.rows), None], anchor, self.height, usize::from(self.busy))
            .map(|mut top| {
                top.origin = self.origins.origin(top.position.line);
                top
            })
    }

    fn source_window(&self) -> (std::ops::Range<usize>, usize) {
        let window = self.rows.window();
        let visible = if window.start < window.end {
            self.rows.ranges().get(window.start)
                .zip(self.rows.ranges().get(window.end - 1))
                .map(|(first, last)| first.start..last.end)
                .unwrap_or(0..0)
        } else { 0..0 };
        (visible, self.rows.text().len())
    }

    /// Move through the retained page using the shared wrapping projection.
    pub fn scroll(&mut self, rows: isize) {
        if !self.overlay.is_empty() {
            let maximum = self.overlay.len().saturating_sub(self.height.saturating_sub(2));
            self.overlay_top = self.overlay_top.saturating_add_signed(rows).min(maximum);
            self.request_frame();
            return;
        }
        self.resolve_top();
        self.top = tui::move_transcript(
            [None, Some(&mut self.rows), None], self.top, rows,
            self.height, usize::from(self.busy),
        );
        if let Some(top) = &mut self.top {
            top.origin = self.origins.origin(top.position.line);
        }
        self.request_frame();
    }

    /// Help opens at its first descriptor and owns its own bounded row offset.
    /// Closing it leaves the transcript's source anchor intact.
    pub fn view_scroll(&self, area: tui::ratatui::layout::Rect, input: &PromptInput) -> usize {
        if self.overlay.is_empty() { self.screen().tail_scroll(area, input) }
        else { self.overlay_top }
    }

    /// Borrow visible rows without joining or copying the retained window.
    pub fn screen(&self) -> Screen<'_> {
        let mut screen = Screen {
            header: Line::raw(if !self.observed_status {
                "GraphCoder · connecting"
            } else if self.busy { "● GraphCoder" } else { "○ GraphCoder" }),
            transcript: self.rows.shown(),
            hidden_tail: self.top.is_some(),
            scrollbar: Some(self.source_window()),
            notifications: &self.notices,
            overlay: &self.overlay,
            overlay_title: "Shortcuts",
            status: self.status.clone(),
            ..Screen::default()
        };
        if self.search_open {
            screen.transcript_search = Some(((&self.rows).into(), &self.search));
            screen.status = Line::raw(format!(
                "Find: {}/{}{} matches · Enter next · Shift+Enter previous · Escape closes",
                self.search.position(self.search_index), self.search.count(),
                if self.search.pending() { "…" } else { "" },
            ));
        }
        screen
    }

    /// Successful device output starts the shared 60 Hz presentation interval.
    pub fn frame_presented(&mut self) {
        self.pacing.frame_presented();
        if self.busy { self.pacing.request_frame(); }
    }

    /// Mark the existing clock dirty without starting another timer or task.
    pub fn request_frame(&mut self) { self.pacing.request_frame(); }

    /// Next dirty frame; an idle client has no wake-up deadline.
    pub fn frame_deadline(&self) -> Option<tokio::time::Instant> { self.pacing.frame_deadline() }
}
