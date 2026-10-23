use super::ClientRenderer;
use crate::{
    client::{CommandFileRoot, CommandSession},
    tui::{self, TranscriptAnchor},
};
use acyclic_stream::StreamProvider;

pub(super) fn reasoning<P: StreamProvider + ?Sized>(
    files: &CommandSession<P>,
) -> crate::Result<bool> {
    let settings = files.state(CommandFileRoot::Config, "settings.json")?;
    let layout = files.state(CommandFileRoot::Session, "layout.json")?;
    if layout
        .as_ref()
        .is_some_and(|layout| layout["open_view"] == "find" && layout["view_input"] == "find")
    {
        return Ok(false);
    }
    settings
        .as_ref()
        .and_then(|value| value.get("reasoning"))
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| crate::Error::Invalid("reasoning must be a boolean".into()))
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

impl ClientRenderer {
    pub(super) fn select_view<P: StreamProvider + ?Sized>(
        &mut self,
        files: &CommandSession<P>,
        input: &tui::PromptInput,
        height: usize,
    ) -> crate::Result<()> {
        self.view_height = height;
        if height == 0 {
            self.search_open = false;
            self.wrapped_reasoning.hide();
            self.wrapped_transcript.hide();
            self.wrapped_tools.hide();
            self.top = crate::client::tui_anchor(files)?;
            return Ok(());
        }
        let layout = files
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or_default();
        self.search_open = layout["open_view"] == "find" && layout["view_input"] == "find";
        let rows = tui::TranscriptRows::new(&self.wrapped_transcript, &self.wrapped_tools);
        self.search.project(rows, self.search_open);
        let mut top = crate::client::tui_anchor(files)?;
        if self.search_open {
            let index = layout["search_index"].as_i64().unwrap_or(0);
            self.search.update(
                rows,
                layout["search_query"].as_str().unwrap_or(""),
                index,
                input.theme.style(tui::config::ThemeRole::Selection),
            );
            top = self.search.selected_anchor(rows, index).or(top);
        }
        self.top = tui::view_transcript(
            [
                self.reasoning_visible
                    .then_some(&mut self.wrapped_reasoning),
                Some(&mut self.wrapped_transcript),
                Some(&mut self.wrapped_tools),
            ],
            top,
            height,
            usize::from(self.busy),
        );
        if self.search_open {
            self.search.prepare(
                tui::TranscriptRows::new(&self.wrapped_transcript, &self.wrapped_tools),
                0,
                height,
            );
        }
        Ok(())
    }

    /// Move a source anchor through the current physical layout. Files remain
    /// authoritative; the caller publishes the returned anchor as a file change.
    pub fn move_view(
        &mut self,
        from: Option<TranscriptAnchor>,
        rows: isize,
    ) -> Option<TranscriptAnchor> {
        tui::move_transcript(
            [
                self.reasoning_visible
                    .then_some(&mut self.wrapped_reasoning),
                Some(&mut self.wrapped_transcript),
                Some(&mut self.wrapped_tools),
            ],
            from,
            rows,
            self.view_height,
            usize::from(self.busy),
        )
    }
}

impl ClientRenderer {
    /// Resolve a source-track position without traversing unrelated archive rows.
    pub fn anchor_at(&mut self, mut byte: usize) -> Option<TranscriptAnchor> {
        let mut anchor = None;
        for (part, source) in [
            (tui::TranscriptPart::Reasoning, &self.wrapped_reasoning),
            (tui::TranscriptPart::Answer, &self.wrapped_transcript),
            (tui::TranscriptPart::Tools, &self.wrapped_tools),
        ] {
            if source.source_len() == 0
                || part == tui::TranscriptPart::Reasoning && !self.reasoning_visible
            {
                continue;
            }
            let length = source.text().len() + 1;
            if byte < length {
                anchor = Some(TranscriptAnchor {
                    part,
                    position: source.position_of(byte.min(length - 1)),
                });
                break;
            }
            byte -= length;
        }
        tui::view_transcript(
            [
                self.reasoning_visible
                    .then_some(&mut self.wrapped_reasoning),
                Some(&mut self.wrapped_transcript),
                Some(&mut self.wrapped_tools),
            ],
            anchor,
            self.view_height,
            usize::from(self.busy),
        )
    }

    pub(super) fn source_window(&self) -> (std::ops::Range<usize>, usize) {
        let mut total = 0;
        let mut visible = None;
        for source in [
            self.reasoning_visible.then_some(&self.wrapped_reasoning),
            Some(&self.wrapped_transcript),
            Some(&self.wrapped_tools),
        ]
        .into_iter()
        .flatten()
        {
            if source.source_len() == 0 {
                continue;
            }
            let window = source.window();
            if window.end > window.start {
                let first = &source.ranges()[window.start];
                let last = &source.ranges()[window.end - 1];
                let range = visible
                    .get_or_insert(total + first.start..total + last.end.max(first.start + 1));
                range.end = total + last.end.max(last.start + 1);
            }
            total += source.text().len() + 1;
        }
        total += usize::from(self.busy);
        let mut visible = visible.unwrap_or(total..total);
        if self.top.is_none() {
            visible.end = total;
        }
        (visible, total)
    }
}
