use super::{
    delimiters::{self, Delimiters, Run},
    forest,
    tokens::Kind,
};
use std::{collections::HashMap, ops::Range};
#[path = "inline_anchors.rs"]
mod anchors;
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
    labels: [Option<Range<usize>>; 2],
    opaque: Option<usize>,
    codes: Vec<Code>,
    code_lengths: HashMap<usize, usize>,
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
    labels: [Option<Range<usize>>; 2],
    opaque: Option<usize>,
    frontier: usize,
    code: (usize, Option<usize>),
    links: usize,
}

struct Code {
    opening: Range<usize>,
    before: Snapshot,
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
        self.labels = [None, None];
        self.opaque = None;
        for code in &self.codes {
            #[cfg(test)]
            crate::work::scan(1);
            self.code_lengths.remove(&code.opening.len());
        }
        self.codes.clear();
        debug_assert!(
            self.code_lengths.is_empty(),
            "every code opener has one index entry"
        );
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
            labels: self.labels.clone(),
            opaque: self.opaque,
            frontier: self.earliest(usize::MAX),
            code: (
                self.codes.len(),
                self.codes.last().map(|code| code.opening.start),
            ),
            links: self.link_floor,
        }
    }

    fn restore(&mut self, before: Snapshot) {
        if !self.has_code_prefix(before.code) {
            self.invalid = true;
            return;
        }
        while self.codes.len() > before.code.0 {
            let code = self.codes.pop().expect("later code opener");
            #[cfg(test)]
            crate::work::scan(1);
            self.code_lengths.remove(&code.opening.len());
        }
        self.delimiter.restore(before.delimiter);
        self.invalid |= !self.forest.restore(before.forest);
        self.bracket = before.bracket;
        self.labels = before.labels;
        self.opaque = before.opaque;
        self.link_floor = before.links;
    }

    pub(super) fn neutral(&self) -> bool {
        self.delimiter.neutral()
            && self.bracket == 0
            && self.labels.iter().all(Option::is_none)
            && self.opaque.is_none()
            && self.codes.is_empty()
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
            self.labels.iter().flatten().next().map(|label| label.start),
            self.opaque,
            self.codes
                .first()
                .map(|code| code.opening.start.min(code.before.frontier)),
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
        self.labels = [None, None];
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
        // Reference lookahead starts at the next first-pass text node. An
        // escaped opening bracket therefore still begins a reference suffix.
        let opening = match kind {
            Kind::Open { image } => Some((image, range.start)),
            Kind::Escape('[') if self.labels.iter().any(Option::is_some) => {
                Some((false, range.start + 1))
            }
            _ => None,
        };
        if opening.is_none() && !matches!(kind, Kind::Close | Kind::ParenOpen) {
            self.labels = [None, None];
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
                    let (previous, start, active) = (
                        bracket.previous,
                        bracket.start,
                        bracket.image || bracket.start >= self.link_floor,
                    );
                    self.bracket = previous;
                    if previous == 0 && active {
                        #[cfg(test)]
                        crate::work::copy(2 * std::mem::size_of::<Option<Range<usize>>>());
                        self.labels = if self.labels.iter().all(Option::is_none) {
                            [Some(start..range.end), None]
                        } else {
                            [
                                self.labels[1].take().or_else(|| self.labels[0].take()),
                                Some(start..range.end),
                            ]
                        };
                    }
                }
            }
            Kind::ParenOpen => {
                if let Some(label) = self.labels.iter().flatten().next() {
                    self.opaque.get_or_insert(label.start);
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
        if let Some(index) = self.code_lengths.get(&physical.len()).copied() {
            // Every removed opener was inserted once. A late match restores
            // its prefix directly, discarding tentative inner interpretations.
            let mut opening = None;
            while self.codes.len() > index {
                let Some(code) = self.codes.pop() else { break };
                #[cfg(test)]
                crate::work::scan(1);
                self.code_lengths.remove(&code.opening.len());
                opening = Some(code);
            }
            if let Some(code) = opening {
                #[cfg(test)]
                let restore = !self.stale;
                #[cfg(not(test))]
                let restore = true;
                if restore {
                    self.restore(code.before);
                }
                self.forest.code(code.opening, physical);
                self.labels = [None, None];
            }
            return;
        }
        if escaped {
            self.forest.escape(range.start..range.start + 2);
        }
        let opening = physical.start + usize::from(escaped)..physical.end;
        if opening.is_empty() {
            self.labels = [None, None];
            return;
        }
        if self.code_lengths.contains_key(&opening.len()) {
            // An escaped first tick can make a later opener the same length
            // as an earlier one. The earlier opener wins every future match.
            self.forest.text(opening);
            self.labels = [None, None];
            return;
        }
        let before = self.snapshot();
        #[cfg(test)]
        {
            if self.codes.len() == self.codes.capacity() {
                crate::work::copy(self.codes.len() * std::mem::size_of::<Code>());
            }
            if self.code_lengths.len() == self.code_lengths.capacity() {
                crate::work::copy(self.code_lengths.len() * std::mem::size_of::<(usize, usize)>());
            }
        }
        self.code_lengths.insert(opening.len(), self.codes.len());
        self.codes.push(Code {
            opening: opening.clone(),
            before,
        });
        self.forest.text(opening);
        self.labels = [None, None];
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.delimiter.retained()
            + self.anchors.capacity() * std::mem::size_of::<(usize, anchors::Anchor)>()
            + self.forest.retained()
            + self.brackets.capacity() * std::mem::size_of::<Bracket>()
            + self.codes.capacity() * std::mem::size_of::<Code>()
            + self.code_lengths.capacity() * std::mem::size_of::<(usize, usize)>()
            + self
                .eof
                .as_ref()
                .map_or(0, |_| std::mem::size_of::<eof::Projections>())
    }
}

#[cfg(test)]
#[path = "../tests/inline_state.rs"]
mod tests;
