use super::{
    delimiters::{self, Delimiters, Run},
    forest,
    tokens::Kind,
};
use std::ops::Range;
#[path = "inline_anchors.rs"]
pub(in crate::inline_scan) mod anchors;
#[path = "inline_brackets.rs"]
mod brackets;
#[path = "inline_codes.rs"]
mod codes;
#[path = "inline_eof.rs"]
mod eof;
#[path = "inline_scope.rs"]
mod scope;

#[derive(Default)]
pub(super) struct State {
    delimiter: Delimiters,
    pub(super) forest: forest::Forest,
    pub(super) invalid: bool,
    brackets: Vec<Vec<Bracket>>,
    bracket_count: usize,
    bracket: usize,
    link_floor: usize,
    labels: [usize; 2],
    closed: usize,
    closed_labels: Vec<Vec<brackets::Closed>>,
    closed_count: usize,
    opaque: Option<usize>,
    codes: codes::Codes,
    eof: Option<Box<eof::Projections>>,
    anchors: Vec<Vec<Option<Box<anchors::Anchor>>>>,
    anchor_slots: usize,
    anchor_context: Option<std::sync::Arc<anchors::Context>>,
    #[cfg(test)]
    stale: bool,
    #[cfg(test)]
    rescan: bool,
    #[cfg(test)]
    replay_eof: bool,
}

struct Bracket {
    previous: usize,
    start: usize,
    frontier: usize,
    first: usize,
    image: bool,
    image_parent: usize,
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Snapshot {
    resume: anchors::Anchor,
    frontier: usize,
}
impl std::ops::Deref for Snapshot {
    type Target = anchors::Anchor;
    fn deref(&self) -> &Self::Target {
        &self.resume
    }
}

impl State {
    pub(super) fn reset(&mut self) {
        #[cfg(test)]
        crate::work::scan(self.anchors.len());
        self.anchors.clear();
        self.anchor_slots = 0;
        self.anchor_context = None;
        self.delimiter.reset();
        self.forest.reset();
        self.invalid = false;
        self.brackets.clear();
        self.bracket_count = 0;
        self.bracket = 0;
        self.link_floor = 0;
        self.labels = [0, 0];
        self.closed = 0;
        self.closed_labels.clear();
        self.closed_count = 0;
        self.opaque = None;
        self.codes.reset();
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
    }

    pub(super) fn finish_block(&mut self, end: usize) {
        self.delimiter.restore(Default::default());
        self.invalid |= !self.codes.restore(0);
        self.bracket = 0;
        self.closed = 0;
        self.labels = [0, 0];
        self.opaque = None;
        self.link_floor = end;
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
    }

    fn snapshot(&mut self) -> Snapshot {
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<usize>());
        let frontier = self.earliest(usize::MAX);
        Snapshot {
            resume: self.resume(),
            frontier,
        }
    }

    fn restore(&mut self, before: Snapshot) {
        let (code, closed, links) = (before.context.code, before.closed, before.context.links);
        self.restore_resume(before.resume, code, closed, links);
    }

    fn restore_resume(
        &mut self,
        before: anchors::Anchor,
        code: usize,
        closed: usize,
        links: usize,
    ) {
        if !self.codes.restore(code) {
            self.invalid = true;
            return;
        }
        self.delimiter.restore(delimiters::Snapshot::from_parts(
            before.delimiter,
            before.context.floor,
        ));
        self.invalid |= !self.forest.restore(before.forest);
        self.bracket = before.bracket;
        self.labels = if before.context.hint {
            [closed, 0]
        } else {
            [0, 0]
        };
        #[cfg(test)]
        let restore_closed = !brackets::FORGET_RESTORE.get();
        #[cfg(not(test))]
        let restore_closed = true;
        if restore_closed {
            self.closed = closed;
        }
        self.opaque = before.context.opaque.map(|position| position.get() - 1);
        self.link_floor = links;
    }

    pub(super) fn neutral(&self) -> bool {
        self.delimiter.neutral()
            && self
                .pending_bracket(self.bracket, self.link_floor)
                .is_none()
            && self.labels == [0, 0]
            && self.opaque.is_none()
            && self.codes.head() == 0
    }

    fn defer(&mut self, start: usize) {
        self.opaque = Some(self.opaque.map_or(start, |previous| previous.min(start)));
    }

    pub(super) fn outside_brackets(&self) -> bool {
        self.pending_bracket(self.bracket, self.link_floor)
            .is_none()
    }

    fn bracket(&self) -> Option<&Bracket> {
        self.opening(self.bracket)
    }

    fn opening(&self, id: usize) -> Option<&Bracket> {
        let index = id.checked_sub(1)?;
        self.brackets
            .get(index / forest::PAGE)?
            .get(index % forest::PAGE)
    }

    fn pending_bracket(&self, cursor: usize, floor: usize) -> Option<&Bracket> {
        let bracket = self.opening(cursor)?;
        if bracket.image || bracket.start >= floor {
            Some(bracket)
        } else {
            self.opening(bracket.image_parent)
        }
    }

    pub(super) fn has_opaque(&self) -> bool {
        self.opaque.is_some()
    }

    pub(super) fn label_code_pending(&self, start: usize) -> bool {
        self.codes
            .latest_start()
            .is_some_and(|opening| opening >= start)
    }

    pub(super) fn resume_bracket(&self) -> usize {
        self.bracket
    }
    pub(super) fn resume_opener(&self, cursor: usize) -> Option<(usize, usize)> {
        self.opening(cursor)
            .map(|bracket| (bracket.first, bracket.previous))
    }
    pub(super) fn resume_code_depth(&self) -> usize {
        self.codes.depth_at(self.codes.head())
    }
    pub(super) fn resume_code(&self, width: usize) -> Option<(usize, usize, usize, usize)> {
        let code = self.codes.get(width)?;
        let start = self
            .pending_bracket(code.before.bracket, code.before.context.links)
            .map_or(code.opening.start, |bracket| {
                bracket.first.min(code.opening.start)
            });
        Some((
            code.depth,
            self.codes.depth_at(code.before.context.code),
            code.before.bracket,
            start,
        ))
    }

    pub(super) fn destination_possible(&self, end: usize) -> bool {
        if self.codes.head() != 0 {
            return true;
        }
        let mut cursor = self.closed;
        while let Some(label) = self.label(cursor) {
            #[cfg(test)]
            crate::work::scan(1);
            if label.range.end <= end {
                return label.range.end == end
                    && (label.opening == 0
                        || self.opening(label.opening).is_some_and(|opening| {
                            opening.image || opening.start >= self.link_floor
                        }));
            }
            cursor = label.previous;
        }
        false
    }

    pub(super) fn resume_closures(
        &self,
        count: usize,
        end: usize,
        destination: Option<usize>,
    ) -> usize {
        let mut base = self
            .labels
            .iter()
            .find_map(|&id| self.label(id).map(|label| label.range.start))
            .unwrap_or(end);
        if let Some(opaque) = self.opaque {
            base = base.min(opaque);
        }
        let mut cursor = self.bracket;
        for _ in 0..count {
            let Some(bracket) = self.opening(cursor) else {
                break;
            };
            base = base.min(bracket.first);
            cursor = bracket.previous;
        }
        if let Some(destination) = destination {
            let mut cursor = self.closed;
            while let Some(label) = self.label(cursor) {
                #[cfg(test)]
                crate::work::scan(1);
                if label.range.end <= destination {
                    if label.range.end == destination {
                        base = base.min(label.range.start);
                    }
                    break;
                }
                cursor = label.previous;
            }
        }
        base
    }

    pub(super) fn classification_start(&self, end: usize) -> usize {
        [
            self.pending_bracket(self.bracket, self.link_floor)
                .map(|bracket| bracket.first),
            self.labels
                .iter()
                .find_map(|&id| self.label(id).map(|label| label.range.start)),
            self.opaque,
            self.codes.classification_start(),
        ]
        .into_iter()
        .flatten()
        .fold(end, usize::min)
    }

    pub(super) fn earliest(&self, end: usize) -> usize {
        [
            self.delimiter.first(),
            self.bracket().map(|bracket| bracket.frontier),
            self.labels
                .iter()
                .find_map(|&id| self.label(id).map(|label| label.range.start)),
            self.opaque,
            self.codes.earliest(),
        ]
        .into_iter()
        .flatten()
        .fold(end, usize::min)
    }

    pub(super) fn feed(&mut self, range: Range<usize>, kind: Kind, linked: bool) {
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
        #[cfg(test)]
        if self.rescan {
            self.forest.scopes();
        }
        if matches!(kind, Kind::Run { byte: b'`', .. } | Kind::Escape('`')) {
            self.ticks(range, kind == Kind::Escape('`'));
            return;
        }
        let cursor = match kind {
            Kind::Escape('\n') | Kind::Break | Kind::Entity => {
                self.forest.leaf(range.clone(), kind)
            }
            Kind::Escape(_) => self.forest.escape(range.clone()),
            _ => self.forest.text(range.clone()),
        };
        // An escaped '[' can start reference lookahead at the next text
        // node. Keep that dependency without inventing a raw bracket opener.
        if kind == Kind::Escape('[')
            && let Some(start) = self
                .labels
                .iter()
                .find_map(|&id| self.label(id).map(|label| label.range.start))
        {
            self.defer(start);
        }
        let opening = match kind {
            Kind::Open { image } => Some((image, range.start)),
            _ => None,
        };
        if opening.is_none() && !matches!(kind, Kind::Close | Kind::ParenOpen) {
            self.labels = [0, 0];
        }
        if let Some((image, start)) = opening.filter(|_| !linked) {
            let frontier = self.earliest(range.start);
            if self.bracket_count.is_multiple_of(forest::PAGE) {
                #[cfg(test)]
                if self.brackets.len() == self.brackets.capacity() {
                    crate::work::copy(self.brackets.len() * std::mem::size_of::<Vec<Bracket>>());
                }
                self.brackets.push(Vec::with_capacity(forest::PAGE));
            }
            let image_parent = if image {
                self.bracket_count + 1
            } else {
                self.bracket().map_or(0, |bracket| bracket.image_parent)
            };
            let first = self
                .labels
                .iter()
                .find_map(|&id| self.label(id).map(|label| label.range.start))
                .unwrap_or(start);
            self.brackets
                .last_mut()
                .expect("bracket page")
                .push(Bracket {
                    previous: self.bracket,
                    start,
                    frontier,
                    first,
                    image,
                    image_parent,
                });
            self.bracket_count += 1;
            self.bracket = self.bracket_count;
            self.labels = [0, 0];
            return;
        }
        match kind {
            Kind::Close if !linked => {
                self.labels = [0, 0];
                if let Some(bracket) = self.bracket() {
                    let opening = self.bracket;
                    let (previous, start, active) = (
                        bracket.previous,
                        bracket.start,
                        bracket.image || bracket.start >= self.link_floor,
                    );
                    self.bracket = previous;
                    let label = self.remember_closed(start..range.end, opening);
                    if active {
                        #[cfg(test)]
                        crate::work::copy(2 * std::mem::size_of::<usize>());
                        self.labels = [label, 0];
                    }
                }
            }
            Kind::ParenOpen => {
                if let Some(start) = self
                    .labels
                    .iter()
                    .find_map(|&id| self.label(id).map(|label| label.range.start))
                {
                    self.defer(start);
                }
            }
            Kind::AngleOpen => {
                self.defer(range.start);
            }
            Kind::Run {
                byte,
                before,
                after,
            } if !linked => {
                let mut run = Run::new(range, byte, before, after);
                run.cursor = cursor;
                self.delimiter.feed(run, |matched| {
                    let wrapped = self.forest.wrap(matched);
                    debug_assert!(wrapped.is_some(), "active delimiter cursors");
                    self.invalid |= wrapped.is_none();
                    wrapped.unwrap_or_default()
                });
            }
            _ => {}
        }
    }

    pub(super) fn projection(&mut self, token: Option<&super::tokens::Token>) -> usize {
        if token.is_none() {
            return self.forest.snapshot();
        }
        let mut eof = self.eof.take().unwrap_or_default();
        #[cfg(test)]
        if self.replay_eof {
            eof.invalidate();
        }
        let root = eof.project(self, token);
        self.eof = Some(eof);
        root
    }

    fn ticks(&mut self, range: Range<usize>, escaped: bool) {
        let physical = range.start + usize::from(escaped)..range.end;
        #[cfg(test)]
        crate::work::scan(1);
        if let Some(code) = self.codes.get(physical.len()) {
            #[cfg(test)]
            crate::work::copy(std::mem::size_of::<Snapshot>());
            let (opening, before) = (code.opening.clone(), code.before.clone());
            #[cfg(test)]
            let restore = !self.stale;
            #[cfg(not(test))]
            let restore = true;
            if restore {
                self.restore(before);
            } else {
                self.codes.restore(before.context.code);
            }
            self.forest.code(opening, physical);
            self.labels = [0, 0];
            return;
        }
        if escaped {
            self.forest.escape(range.start..range.start + 2);
        }
        let opening = physical.start + usize::from(escaped)..physical.end;
        if opening.is_empty() {
            self.labels = [0, 0];
            return;
        }
        if self.codes.get(opening.len()).is_some() {
            // An escaped first tick can make a later opener the same length
            // as an earlier one. The earlier opener wins every future match.
            self.forest.text(opening);
            self.labels = [0, 0];
            return;
        }
        let before = self.snapshot();
        self.codes.push(opening.clone(), before);
        self.forest.text(opening);
        self.labels = [0, 0];
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.delimiter.retained()
            + self.anchors.capacity() * std::mem::size_of::<Vec<Option<Box<anchors::Anchor>>>>()
            + self
                .anchors
                .iter()
                .map(|page| {
                    page.capacity() * std::mem::size_of::<Option<Box<anchors::Anchor>>>()
                        + page.iter().filter(|slot| slot.is_some()).count()
                            * (std::mem::size_of::<anchors::Anchor>()
                                + std::mem::size_of::<anchors::Context>())
                })
                .sum::<usize>()
            + self.forest.retained()
            + self.brackets.capacity() * std::mem::size_of::<Vec<Bracket>>()
            + self
                .brackets
                .iter()
                .map(|page| page.capacity() * std::mem::size_of::<Bracket>())
                .sum::<usize>()
            + self.closed_labels.capacity() * std::mem::size_of::<Vec<brackets::Closed>>()
            + self
                .closed_labels
                .iter()
                .map(|page| page.capacity() * std::mem::size_of::<brackets::Closed>())
                .sum::<usize>()
            + self.codes.retained()
            + self
                .eof
                .as_ref()
                .map_or(0, |_| std::mem::size_of::<eof::Projections>())
    }
}

#[cfg(test)]
#[path = "../tests/inline_state.rs"]
mod tests;
