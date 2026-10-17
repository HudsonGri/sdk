use super::*;
#[path = "inline_bind.rs"]
mod bind;
#[path = "inline_delimiters.rs"]
mod delimiters;
#[path = "inline_forest.rs"]
mod forest;
#[path = "inline_resume.rs"]
mod resume;
#[path = "inline_state.rs"]
mod state;
#[path = "inline_tokens.rs"]
mod tokens;
use state::State;
use tokens::{Kind, Tokens};

#[derive(Clone, Copy)]
pub(super) struct Slice<'a> {
    pub(super) original: &'a str,
    pub(super) parsed: &'a str,
    pub(super) start: usize,
    pub(super) base: usize,
    pub(super) sentinel: usize,
    pub(super) boundary: usize,
    pub(super) classified: bool,
}

#[derive(Default)]
pub(super) struct Scanner {
    tokens: Tokens,
    capsules: Vec<Range<usize>>,
    state: State,
    render: forest::Cache,
    syntax: Syntax,
    processed: usize,
    projected: usize,
    fed: usize,
    discarded: usize,
    frozen: usize,
    frontier: usize,
    pending_break: Option<usize>,
    pending_after_break: bool,
    first_expansion: Option<usize>,
    #[cfg(test)]
    replay: bool,
    #[cfg(test)]
    fed_bytes: usize,
}

struct Scope {
    next: usize,
    events: Range<usize>,
    range: Range<usize>,
    close: usize,
}

struct ScopeFrame {
    end: usize,
    scope: usize,
    labelled: bool,
}

#[derive(Default)]
struct Syntax {
    outer: Vec<Scope>,
    hard_breaks: HashSet<usize>,
    opaque: Vec<Range<usize>>,
    links: Vec<Range<usize>>,
}

impl Syntax {
    fn new(
        events: &[(Event<'_>, Range<usize>)],
        parsed: &str,
        tokens: &Tokens,
        base: usize,
        sentinel: usize,
        discarded: usize,
        frozen: usize,
    ) -> Self {
        let mut syntax = Self::default();
        let mut links = Vec::<ScopeFrame>::new();
        for (event_index, (event, range)) in events.iter().enumerate() {
            if base + range.end.saturating_sub(sentinel) <= frozen {
                continue;
            }
            #[cfg(test)]
            work::scan(1);
            if matches!(event, Event::HardBreak) {
                #[cfg(test)]
                work::copy(
                    std::mem::size_of::<usize>()
                        * (1 + if syntax.hard_breaks.len() == syntax.hard_breaks.capacity() {
                            syntax.hard_breaks.len()
                        } else {
                            0
                        }),
                );
                syntax.hard_breaks.insert(range.start);
            }
            let mut consumed = range.clone();
            match event {
                Event::Start(Tag::Link { link_type, .. } | Tag::Image { link_type, .. }) => {
                    // The reported range excludes the collapsed suffix. Use the
                    // lexer's source coverage, including first-pass escapes.
                    if matches!(
                        link_type,
                        pulldown_cmark::LinkType::Collapsed
                            | pulldown_cmark::LinkType::CollapsedUnknown
                    ) && let Some(end) =
                        tokens.collapsed_end(base + range.end.saturating_sub(sentinel), discarded)
                    {
                        consumed.end = sentinel + end.saturating_sub(base);
                    }
                    if let Some(frame) = links.last_mut() {
                        frame.end = frame.end.max(consumed.end);
                    }
                    if syntax
                        .links
                        .last()
                        .is_none_or(|parent| parent.end < consumed.end)
                    {
                        push_range(&mut syntax.links, consumed.clone());
                    }
                    #[cfg(test)]
                    work::copy(
                        std::mem::size_of::<ScopeFrame>()
                            * (1 + if links.len() == links.capacity() {
                                links.len()
                            } else {
                                0
                            }),
                    );
                    let index = syntax.outer.len();
                    syntax.scope(Scope {
                        next: index + 1,
                        events: event_index..event_index + 1,
                        range: consumed.clone(),
                        close: 0,
                    });
                    links.push(ScopeFrame {
                        end: range.start,
                        scope: index,
                        labelled: !matches!(
                            link_type,
                            pulldown_cmark::LinkType::Autolink | pulldown_cmark::LinkType::Email
                        ),
                    });
                }
                Event::End(TagEnd::Link | TagEnd::Image) => {
                    if let Some(frame) = links.pop() {
                        let next = syntax.outer.len();
                        let scope = &mut syntax.outer[frame.scope];
                        scope.events.end = event_index + 1;
                        scope.next = next;
                        if frame.labelled {
                            scope.close = parsed
                                .get(frame.end..scope.range.end)
                                .and_then(|tail| tail.find(']'))
                                .map_or(0, |offset| {
                                    #[cfg(test)]
                                    work::scan(offset + 1);
                                    frame.end + offset + 1
                                });
                        }
                    }
                }
                _ => {
                    if let Some(frame) = links.last_mut() {
                        frame.end = frame.end.max(range.end);
                    }
                }
            }
            if matches!(event, Event::InlineHtml(_)) {
                push_range(&mut syntax.opaque, range.clone());
                syntax.scope(Scope {
                    next: syntax.outer.len() + 1,
                    events: event_index..event_index + 1,
                    range: range.clone(),
                    close: 0,
                });
            }
        }
        syntax
    }

    fn scope(&mut self, scope: Scope) {
        #[cfg(test)]
        work::copy(
            std::mem::size_of::<Scope>()
                * (1 + if self.outer.len() == self.outer.capacity() {
                    self.outer.len()
                } else {
                    0
                }),
        );
        self.outer.push(scope);
    }

    fn absolute(&mut self, base: usize, sentinel: usize) {
        #[cfg(test)]
        work::copy(self.hard_breaks.len() * std::mem::size_of::<usize>());
        self.hard_breaks = self
            .hard_breaks
            .drain()
            .map(|position| {
                #[cfg(test)]
                work::scan(1);
                base + position.saturating_sub(sentinel)
            })
            .collect();
        for scope in &mut self.outer {
            if scope.close != 0 {
                scope.close = base + scope.close.saturating_sub(sentinel);
            }
        }
        for range in self
            .outer
            .iter_mut()
            .map(|scope| &mut scope.range)
            .chain(&mut self.opaque)
            .chain(&mut self.links)
        {
            #[cfg(test)]
            work::scan(1);
            *range = base + range.start.saturating_sub(sentinel)
                ..base + range.end.saturating_sub(sentinel);
        }
    }

    fn changed_before(
        &self,
        previous: &Self,
        base: usize,
        end: usize,
        capsules: &[Range<usize>],
    ) -> Option<usize> {
        fn first_difference(
            left: &[Range<usize>],
            right: &[Range<usize>],
            base: usize,
            end: usize,
            capsules: &[Range<usize>],
        ) -> Option<usize> {
            let clipped = |range: &Range<usize>| {
                #[cfg(test)]
                work::scan(1);
                let range = range.start.max(base)..range.end;
                (range.start < end && !range.is_empty()).then_some(range)
            };
            let mut left = left.iter().filter_map(clipped).filter(|range| {
                if capsules.last().is_none_or(|scope| scope.end < range.end) {
                    return true;
                }
                let old = right.partition_point(|old| {
                    #[cfg(test)]
                    work::scan(1);
                    old.end < range.end
                });
                if right.get(old) == Some(range) {
                    return false;
                }
                let index = capsules.partition_point(|old| {
                    #[cfg(test)]
                    work::scan(1);
                    old.end < range.end
                });
                !capsules
                    .get(index)
                    .is_some_and(|scope| range.end <= scope.end && scope.start <= range.start)
            });
            left.next().map(|range| range.start)
        }
        first_difference(&self.opaque, &previous.opaque, base, end, capsules)
            .into_iter()
            .chain(first_difference(
                &self.links,
                &previous.links,
                base,
                end,
                capsules,
            ))
            .min()
    }
}

fn push_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    #[cfg(test)]
    work::copy(
        std::mem::size_of::<Range<usize>>()
            * (1 + if ranges.len() == ranges.capacity() {
                ranges.len()
            } else {
                0
            }),
    );
    ranges.push(range);
}

fn inside(ranges: &[Range<usize>], cursor: &mut usize, position: usize) -> bool {
    while ranges
        .get(*cursor)
        .is_some_and(|range| range.end <= position)
    {
        *cursor += 1;
    }
    ranges
        .get(*cursor)
        .is_some_and(|range| range.contains(&position))
}

impl Scanner {
    pub(super) fn new(source: &str, start: usize) -> Self {
        Self {
            tokens: Tokens::at(
                start,
                source
                    .get(..start)
                    .and_then(|text| text.chars().next_back()),
            ),
            processed: start,
            frozen: start,
            frontier: start,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.tokens.retained()
            + self.capsules.capacity() * std::mem::size_of::<Range<usize>>()
            + self.state.retained()
            + self.syntax.outer.capacity() * std::mem::size_of::<Scope>()
            + self.syntax.hard_breaks.capacity() * std::mem::size_of::<usize>()
            + (self.syntax.opaque.capacity() + self.syntax.links.capacity())
                * std::mem::size_of::<Range<usize>>()
    }

    pub(super) fn plain(&self) -> bool {
        self.state.neutral() && self.tokens.neutral()
    }

    pub(super) fn expanded_before(&self, position: usize) -> bool {
        self.first_expansion.is_some_and(|first| first < position)
    }

    pub(super) fn seed_input(&mut self, source: &str, start: usize) {
        self.render.seed_input(source, start);
    }

    pub(super) fn prepare_classification(&mut self, source: &str, start: usize) -> bool {
        self.tokens.push(source);
        let destination = self.render.track_destinations(source, start)
            && self
                .render
                .completed_destinations()
                .any(|start| start >= self.processed || self.state.destination_possible(start));
        destination
    }

    pub(super) fn classification_end(&self, end: usize) -> usize {
        self.tokens
            .preview()
            .filter(|token| matches!(token.kind, Kind::Run { byte: b'`', .. } | Kind::Escape('`')))
            .map_or(end, |token| token.range.start)
    }

    pub(super) fn expands_after(&self, start: usize) -> bool {
        self.tokens.expands_after(start)
    }

    pub(super) fn pending_angle(&self) -> bool {
        self.state.has_opaque()
    }

    pub(super) fn classification_boundary(&self, mut position: usize) -> usize {
        if let Some(range) = self.tokens.covering(position) {
            position = range.start;
        }
        if let Some(range) = self.state.forest.scope_covering(self.projected, position) {
            if range.start < position && position < range.end {
                position = range.start;
            }
        }
        if self
            .capsules
            .last()
            .is_none_or(|scope| scope.end <= position)
        {
            return position;
        }
        let (mut low, mut high) = (0, self.capsules.len());
        while low < high {
            #[cfg(test)]
            work::scan(1);
            let mid = low + (high - low) / 2;
            if self.capsules[mid].end <= position {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        self.capsules
            .get(low)
            .filter(|scope| scope.start < position)
            .map_or(position, |scope| scope.start)
    }

    pub(super) fn classification_start(&self, end: usize, foreign: bool) -> usize {
        if foreign {
            self.classification_boundary(self.state.classification_start(end))
        } else {
            self.classification_boundary(end)
        }
    }

    pub(super) fn discard_before(&mut self, position: usize) {
        self.frozen = self.frozen.max(position);
        let before = self.tokens.ready.len();
        self.tokens.discard_before(position);
        let removed = before - self.tokens.ready.len();
        self.discarded += removed;
        self.fed = self.fed.saturating_sub(removed);
    }

    pub(super) fn render_tree(
        &mut self,
        source: &str,
        start: usize,
        lines: &mut Vec<Line>,
        first: usize,
    ) -> Option<(usize, usize)> {
        if self.state.invalid {
            return None;
        }
        #[cfg(test)]
        let rendering = work::rendering(true);
        let result = self.render.project(
            &mut self.state.forest,
            self.projected,
            forest::glyphs::events::projection::Projection {
                source,
                visible: start..source.len(),
                hard_breaks: &self.syntax.hard_breaks,
            },
            lines,
            first,
        );
        #[cfg(test)]
        work::rendering(rendering);
        result
    }

    #[cfg(test)]
    pub(super) fn glyph_retained(
        &self,
        nodes: &mut HashSet<usize>,
        texts: &mut HashSet<usize>,
        links: &mut HashSet<*const u8>,
    ) -> (usize, usize) {
        self.render.retained(nodes, texts, links)
    }

    pub(super) fn neutral_frontier(&self) -> usize {
        self.pending_break
            .map_or(self.frontier, |start| self.frontier.min(start))
    }

    pub(super) fn checkpoint(&self, position: usize) -> usize {
        if position == self.frontier {
            return position;
        }
        let mut boundary = self.classification_boundary(position);
        if let Some(range) = self.tokens.covering(position) {
            boundary = boundary.min(range.start);
        }
        if let Some(range) = self.state.forest.scope_covering(self.projected, position) {
            if range.start < position && position < range.end {
                boundary = boundary.min(range.start);
            }
        }
        boundary
    }

    pub(super) fn frontier(
        &mut self,
        source: Slice<'_>,
        events: &[(Event<'_>, Range<usize>)],
        references: &References,
    ) -> usize {
        let original = source.original;
        let base = source.base;
        let sentinel = source.sentinel;
        self.tokens.push(original);
        self.render.track_destinations(original, source.start);
        let mut syntax = Syntax::new(
            events,
            source.parsed,
            &self.tokens,
            base,
            sentinel,
            self.discarded,
            self.frozen,
        );
        syntax.absolute(base, sentinel);
        let changed = syntax.changed_before(&self.syntax, base, self.processed, &self.capsules);
        let removed = if source.classified {
            let first = self
                .capsules
                .partition_point(|scope| scope.end <= base.max(self.frozen));
            let end = base + source.parsed.len().saturating_sub(sentinel);
            let mut next = 0;
            self.capsules[first..].iter().find_map(|old| {
                #[cfg(test)]
                work::scan(1);
                if old.start < base.max(self.frozen) || old.end > end {
                    return None;
                }
                while syntax
                    .outer
                    .get(next)
                    .is_some_and(|scope| scope.range.end < old.end)
                {
                    next += 1;
                    #[cfg(test)]
                    work::scan(1);
                }
                (!syntax.outer.get(next).is_some_and(|scope| {
                    scope.range.start <= old.start && old.end <= scope.range.end
                }))
                .then_some(old.start)
            })
        } else {
            None
        };
        let changed = changed.into_iter().chain(removed).min();
        while self
            .capsules
            .last()
            .is_some_and(|scope| scope.end > base.max(self.frozen))
        {
            #[cfg(test)]
            work::scan(1);
            self.capsules.pop();
        }
        for scope in &syntax.outer {
            if self
                .capsules
                .last()
                .is_none_or(|last| last.end < scope.range.end)
            {
                #[cfg(test)]
                work::copy(
                    std::mem::size_of::<Range<usize>>()
                        * (1 + if self.capsules.len() == self.capsules.capacity() {
                            self.capsules.len()
                        } else {
                            0
                        }),
                );
                self.capsules.push(scope.range.clone());
            }
        }
        let fresh_breaks = std::mem::take(&mut syntax.hard_breaks);
        syntax.hard_breaks = std::mem::take(&mut self.syntax.hard_breaks);
        for position in fresh_breaks {
            #[cfg(test)]
            work::copy(
                std::mem::size_of::<usize>()
                    * (1 + if syntax.hard_breaks.len() == syntax.hard_breaks.capacity() {
                        syntax.hard_breaks.len()
                    } else {
                        0
                    }),
            );
            syntax.hard_breaks.insert(position);
        }
        let pending = self.tokens.preview().map(|token| token.range.start);
        #[cfg(test)]
        let replay = self.replay;
        #[cfg(not(test))]
        let replay = false;
        if replay || changed.is_some() {
            self.pending_break = None;
            let restored = if replay {
                None
            } else {
                changed.and_then(|position| {
                    let resume = self.state.classification_start(position);
                    [position, resume, base].into_iter().find_map(|position| {
                        let ordinal = self.tokens.ordinal_at(position)?;
                        if ordinal < self.discarded {
                            return None;
                        }
                        let anchor = self.tokens.anchor_at(position)?;
                        self.state.restore_at(anchor).map(|()| (position, ordinal))
                    })
                })
            };
            if let Some((position, ordinal)) = restored {
                self.processed = position;
                self.fed = ordinal - self.discarded;
                self.frontier = self.frontier.min(position);
            } else {
                self.state.reset();
                if self
                    .render
                    .rendered_origin()
                    .is_some_and(|origin| base > origin)
                {
                    self.state.invalid = true;
                    return sentinel;
                }
                self.processed = base;
                self.fed = 0;
                self.frontier = base;
            }
        }
        let (mut opaque, mut link, mut scope) = (0, 0, 0);
        let mut token_index = self.fed;
        while let Some(token) = self.tokens.ready.get(token_index) {
            let kind = token.kind;
            token_index += 1;
            #[cfg(test)]
            work::scan(1);
            if token.range.end <= self.processed {
                continue;
            }
            let start = token.range.start.max(source.start).max(self.processed);
            let mut end = token.range.end;
            if self.first_expansion.is_none() && kind.expands(token.range.clone()) {
                self.first_expansion = Some(start);
            }
            #[cfg(test)]
            {
                self.fed_bytes += end - start;
            }
            while syntax
                .outer
                .get(scope)
                .is_some_and(|scope| scope.range.end <= start)
            {
                scope += 1;
            }
            if syntax
                .outer
                .get(scope)
                .is_some_and(|scope| scope.range.start == start)
            {
                #[cfg(test)]
                {
                    self.fed_bytes -= end - start;
                }
                let Some(consumed) =
                    self.bind_scope(&source, scope, &syntax.outer, events, references)
                else {
                    self.state.invalid = true;
                    return sentinel;
                };
                end = consumed;
                if let Some(after) = self.tokens.after_absolute(end) {
                    token_index = token_index.max(after.saturating_sub(self.discarded));
                }
            } else if inside(&syntax.opaque, &mut opaque, start) {
                self.state.opaque_text(start..end);
            } else {
                if matches!(kind, Kind::AngleOpen | Kind::Open { .. }) {
                    self.state
                        .remember(self.tokens.anchor_at(start).expect("scope opener"));
                }
                let linked = inside(&syntax.links, &mut link, start);
                self.state.feed(start..end, kind, linked);
            }
            if kind == Kind::Escape('\n') {
                self.pending_break = Some(start);
                self.pending_after_break = false;
            } else if kind == Kind::Break && self.pending_break.is_some() {
                self.pending_after_break = true;
            } else if self.pending_break.is_some()
                && original.get(start..end).is_some_and(|raw| {
                    raw.bytes().any(|byte| {
                        #[cfg(test)]
                        work::scan(1);
                        let expanded = self.first_expansion.is_some_and(|first| {
                            self.pending_break.is_some_and(|pending| first < pending)
                        });
                        !matches!(byte, b' ' | b'\t' | b'\n')
                            && (!expanded
                                || self.pending_after_break
                                || !matches!(byte, 0x0b | 0x0c))
                    })
                })
            {
                if let Some(previous) = self.pending_break.take() {
                    #[cfg(test)]
                    work::copy(
                        std::mem::size_of::<usize>()
                            * (1 + if syntax.hard_breaks.len() == syntax.hard_breaks.capacity() {
                                syntax.hard_breaks.len()
                            } else {
                                0
                            }),
                    );
                    syntax.hard_breaks.insert(previous);
                }
            }
            if self.state.neutral()
                && self.pending_break.is_none()
                && (self.tokens.neutral() || pending.is_some_and(|start| start >= end))
            {
                self.frontier = end;
            }
            self.processed = end;
        }
        // The lexer may extend its final plain-text token on the next push.
        self.fed = self.tokens.ready.len().saturating_sub(usize::from(
            self.tokens
                .ready
                .back()
                .is_some_and(|token| token.kind == Kind::Text),
        ));
        if self.state.invalid {
            return sentinel;
        }
        // An unresolved construct can reopen older ancestors. Advance only
        // through a boundary actually observed with neutral scanner state.
        let candidate = self
            .pending_break
            .map_or(self.frontier, |start| self.frontier.min(start));
        self.projected = self.state.projection(self.tokens.preview().as_ref());
        self.syntax = syntax;
        candidate.saturating_sub(base) + sentinel
    }
}

#[cfg(test)]
#[path = "../tests/inline_scan.rs"]
mod tests;
