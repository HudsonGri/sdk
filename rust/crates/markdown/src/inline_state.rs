use super::{
    delimiters::{self, Delimiters, Run},
    forest,
    tokens::Kind,
};
use std::{collections::HashMap, ops::Range};
#[path = "inline_anchors.rs"]
mod anchors;
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
    brackets: Vec<Bracket>,
    bracket: usize,
    link_floor: usize,
    labels: [usize; 2],
    closed: usize,
    closed_labels: Vec<brackets::Closed>,
    opaque: Option<usize>,
    codes: codes::Codes,
    eof: Option<Box<eof::Projections>>,
    anchors: HashMap<usize, anchors::Anchor>,
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
    image: bool,
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Snapshot {
    delimiter: delimiters::Snapshot,
    forest: usize,
    bracket: usize,
    labels: [usize; 2],
    closed: usize,
    opaque: Option<usize>,
    frontier: usize,
    code: usize,
    links: usize,
}

impl State {
    pub(super) fn reset(&mut self) {
        #[cfg(test)]
        crate::work::scan(self.anchors.len());
        self.anchors.clear();
        self.delimiter.reset();
        self.forest.reset();
        self.invalid = false;
        self.brackets.clear();
        self.bracket = 0;
        self.link_floor = 0;
        self.labels = [0, 0];
        self.closed = 0;
        self.closed_labels.clear();
        self.opaque = None;
        self.codes.reset();
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
    }

    fn snapshot(&self) -> Snapshot {
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Snapshot>());
        Snapshot {
            delimiter: self.delimiter.snapshot(),
            forest: self.forest.snapshot(),
            bracket: self.bracket,
            labels: self.labels,
            closed: self.closed,
            opaque: self.opaque,
            frontier: self.earliest(usize::MAX),
            code: self.codes.head(),
            links: self.link_floor,
        }
    }

    fn restore(&mut self, before: Snapshot) {
        if !self.codes.restore(before.code) {
            self.invalid = true;
            return;
        }
        self.delimiter.restore(before.delimiter);
        self.invalid |= !self.forest.restore(before.forest);
        self.bracket = before.bracket;
        self.labels = before.labels;
        #[cfg(test)]
        let restore_closed = !brackets::FORGET_RESTORE.get();
        #[cfg(not(test))]
        let restore_closed = true;
        if restore_closed {
            self.closed = before.closed;
        }
        self.opaque = before.opaque;
        self.link_floor = before.links;
    }

    pub(super) fn neutral(&self) -> bool {
        self.delimiter.neutral()
            && self.bracket == 0
            && self.labels == [0, 0]
            && self.opaque.is_none()
            && self.codes.head() == 0
    }

    pub(super) fn outside_brackets(&self) -> bool {
        self.bracket == 0
    }

    fn bracket(&self) -> Option<&Bracket> {
        self.bracket
            .checked_sub(1)
            .and_then(|index| self.brackets.get(index))
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

    pub(super) fn opaque_text(&mut self, range: Range<usize>) {
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
        self.forest.text(range);
        self.labels = [0, 0];
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
            self.opaque.get_or_insert(start);
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
            #[cfg(test)]
            if self.brackets.len() == self.brackets.capacity() {
                crate::work::copy(self.brackets.len() * std::mem::size_of::<Bracket>());
            }
            self.brackets.push(Bracket {
                previous: self.bracket,
                start,
                frontier,
                image,
            });
            self.bracket = self.brackets.len();
            return;
        }
        match kind {
            Kind::Close if !linked => {
                if let Some(bracket) = self.bracket() {
                    let opening = self.bracket;
                    let (previous, start, image, active) = (
                        bracket.previous,
                        bracket.start,
                        bracket.image,
                        bracket.image || bracket.start >= self.link_floor,
                    );
                    self.bracket = previous;
                    let label = self.remember_closed(start..range.end, image, opening);
                    if previous == 0 && active {
                        #[cfg(test)]
                        crate::work::copy(2 * std::mem::size_of::<usize>());
                        self.labels = if self.labels == [0, 0] {
                            [label, 0]
                        } else {
                            [
                                if self.labels[1] == 0 {
                                    self.labels[0]
                                } else {
                                    self.labels[1]
                                },
                                label,
                            ]
                        };
                    }
                }
            }
            Kind::ParenOpen => {
                if let Some(start) = self
                    .labels
                    .iter()
                    .find_map(|&id| self.label(id).map(|label| label.range.start))
                {
                    self.opaque.get_or_insert(start);
                }
            }
            Kind::AngleOpen => {
                self.opaque.get_or_insert(range.start);
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
                self.codes.restore(before.code);
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
            + self.anchors.capacity() * std::mem::size_of::<(usize, anchors::Anchor)>()
            + self.forest.retained()
            + self.brackets.capacity() * std::mem::size_of::<Bracket>()
            + self.closed_labels.capacity() * std::mem::size_of::<brackets::Closed>()
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
