use super::*;
#[path = "inline_delimiters.rs"]
mod delimiters;
#[path = "inline_forest.rs"]
mod forest;
#[path = "inline_state.rs"]
mod state;
#[path = "inline_tokens.rs"]
mod tokens;
use state::State;
use tokens::{Kind, Tokens};

pub(super) struct Slice<'a> {
    pub(super) original: &'a str,
    pub(super) parsed: &'a str,
    pub(super) start: usize,
    pub(super) base: usize,
    pub(super) sentinel: usize,
    pub(super) boundary: usize,
}

#[derive(Default)]
pub(super) struct Scanner {
    tokens: Tokens,
    atomic: Vec<Range<usize>>,
    state: State,
    render: forest::Cache,
    syntax: Syntax,
    processed: usize,
    projected: usize,
    fed: usize,
    discarded: usize,
    frontier: usize,
    pending_break: Option<usize>,
    first_expansion: Option<usize>,
    #[cfg(test)]
    replay: bool,
    #[cfg(test)]
    fed_bytes: usize,
}

struct Scope {
    events: Range<usize>,
    range: Range<usize>,
    label: bool,
    disables_links: bool,
    open_code: bool,
}

struct ScopeFrame {
    range: Range<usize>,
    end: usize,
    scope: usize,
    codes: usize,
    scan_codes: bool,
}

#[derive(Default)]
struct Syntax {
    outer: Vec<Scope>,
    indices: HashMap<usize, usize>,
    hard_breaks: HashSet<usize>,
    opaque: Vec<Range<usize>>,
    links: Vec<Range<usize>>,
}

impl Syntax {
    fn new(
        events: &[(Event<'_>, Range<usize>)],
        tokens: &Tokens,
        base: usize,
        sentinel: usize,
        discarded: usize,
    ) -> Self {
        let mut syntax = Self::default();
        let mut links = Vec::<ScopeFrame>::new();
        let mut last_link = None;
        let mut codes = 0;
        let mut code_ends = tokens.code_ends.iter().peekable();
        for (event_index, (event, range)) in events.iter().enumerate() {
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
            if matches!(event, Event::Text(_)) && links.last().is_some_and(|frame| frame.scan_codes)
            {
                let begin = base + range.start.saturating_sub(sentinel);
                let end = base + range.end.saturating_sub(sentinel);
                while code_ends.peek().is_some_and(|position| **position <= begin) {
                    #[cfg(test)]
                    work::scan(1);
                    code_ends.next();
                }
                while code_ends.peek().is_some_and(|position| **position <= end) {
                    #[cfg(test)]
                    work::scan(1);
                    codes += 1;
                    code_ends.next();
                }
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
                        events: event_index..event_index + 1,
                        range: consumed.clone(),
                        label: matches!(
                            link_type,
                            pulldown_cmark::LinkType::Shortcut
                                | pulldown_cmark::LinkType::ShortcutUnknown
                        ),
                        disables_links: false,
                        open_code: false,
                    });
                    if matches!(event, Event::Start(Tag::Link { .. }))
                        && !matches!(
                            link_type,
                            pulldown_cmark::LinkType::Autolink | pulldown_cmark::LinkType::Email
                        )
                    {
                        last_link = Some(range.start);
                    }
                    links.push(ScopeFrame {
                        range: consumed.clone(),
                        end: range.start,
                        scope: index,
                        codes,
                        scan_codes: !matches!(
                            link_type,
                            pulldown_cmark::LinkType::Autolink | pulldown_cmark::LinkType::Email
                        ),
                    });
                }
                Event::End(TagEnd::Link | TagEnd::Image) => {
                    if let Some(frame) = links.pop() {
                        syntax.outer[frame.scope].events.end = event_index + 1;
                        syntax.outer[frame.scope].disables_links =
                            last_link.is_some_and(|start| frame.range.contains(&start));
                        syntax.outer[frame.scope].open_code = codes > frame.codes;
                        let opaque = if frame.scan_codes {
                            frame.end..frame.range.end
                        } else {
                            frame.range
                        };
                        push_range(&mut syntax.opaque, opaque);
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
                    events: event_index..event_index + 1,
                    range: range.clone(),
                    label: false,
                    disables_links: false,
                    open_code: false,
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
        for (index, scope) in self.outer.iter().enumerate() {
            #[cfg(test)]
            {
                let bytes = std::mem::size_of::<(usize, usize)>();
                work::copy(
                    bytes
                        * (1 + if self.indices.len() == self.indices.capacity() {
                            self.indices.len()
                        } else {
                            0
                        }),
                );
            }
            self.indices
                .entry(base + scope.range.start.saturating_sub(sentinel))
                .or_insert(index);
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

    fn changed_before(&self, previous: &Self, base: usize, end: usize) -> Option<usize> {
        fn first_difference(
            left: &[Range<usize>],
            right: &[Range<usize>],
            base: usize,
            end: usize,
        ) -> Option<usize> {
            let clipped = |range: &Range<usize>| {
                #[cfg(test)]
                work::scan(1);
                let range = range.start.max(base)..range.end;
                (range.start < end && !range.is_empty()).then_some(range)
            };
            let mut left = left.iter().filter_map(clipped);
            let mut right = right.iter().filter_map(clipped);
            loop {
                match (left.next(), right.next()) {
                    (None, None) => return None,
                    (Some(left), Some(right)) if left == right => {}
                    (left, right) => {
                        return left.into_iter().chain(right).map(|range| range.start).min();
                    }
                }
            }
        }
        first_difference(&self.opaque, &previous.opaque, base, end)
            .into_iter()
            .chain(first_difference(&self.links, &previous.links, base, end))
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
            frontier: start,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.tokens.retained()
            + self.atomic.capacity() * std::mem::size_of::<Range<usize>>()
            + self.state.retained()
            + self.syntax.outer.capacity() * std::mem::size_of::<Scope>()
            + self.syntax.indices.capacity() * std::mem::size_of::<(usize, usize)>()
            + self.syntax.hard_breaks.capacity() * std::mem::size_of::<usize>()
            + (self.syntax.opaque.capacity() + self.syntax.links.capacity())
                * std::mem::size_of::<Range<usize>>()
    }

    pub(super) fn expanded_before(&self, position: usize) -> bool {
        self.first_expansion.is_some_and(|first| first < position)
    }

    pub(super) fn discard_before(&mut self, position: usize) {
        let before = self.tokens.ready.len();
        self.tokens.discard_before(position);
        let removed = before - self.tokens.ready.len();
        self.discarded += removed;
        self.fed = self.fed.saturating_sub(removed);
        self.atomic.clear();
    }

    pub(super) fn render_events(
        &mut self,
        source: Slice<'_>,
        events: &[(Event<'_>, Range<usize>)],
        references: &References,
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
                source: source.original,
                visible: source.start..source.original.len(),
                hard_breaks: &self.syntax.hard_breaks,
                atoms: Some(forest::glyphs::events::projection::Atoms {
                    source: &source,
                    events,
                    references,
                    scopes: &self.syntax.outer,
                    indices: &self.syntax.indices,
                }),
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

    pub(super) fn checkpoint(&self, position: usize) -> usize {
        let (mut low, mut high) = (0, self.atomic.len());
        while low < high {
            #[cfg(test)]
            work::scan(1);
            let middle = low + (high - low) / 2;
            if self
                .atomic
                .get(middle)
                .is_some_and(|range| range.end <= position)
            {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        self.atomic.get(low).map_or(position, |range| {
            if range.start < position {
                range.start
            } else {
                position
            }
        })
    }

    pub(super) fn frontier(
        &mut self,
        original: &str,
        base: usize,
        events: &[(Event<'_>, Range<usize>)],
        sentinel: usize,
    ) -> usize {
        self.tokens.push(original);
        let mut syntax = Syntax::new(events, &self.tokens, base, sentinel, self.discarded);
        syntax.absolute(base, sentinel);
        let pending = self.tokens.preview().map(|token| token.range.start);
        let changed = syntax.changed_before(&self.syntax, base, self.processed);
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
                    self.state
                        .restore_at(position)
                        .filter(|ordinal| *ordinal >= self.discarded)
                        .map(|ordinal| (position, ordinal))
                })
            };
            if let Some((position, ordinal)) = restored {
                self.processed = position;
                self.fed = ordinal - self.discarded;
                self.frontier = self.frontier.min(position);
            } else {
                self.state.reset();
                if self.render.origin().is_some_and(|origin| base > origin) {
                    self.state.invalid = true;
                    return sentinel;
                }
                self.processed = base;
                self.fed = 0;
                self.frontier = base;
            }
        }
        let (mut opaque, mut link, mut scope) = (0, 0, 0);
        let state = &mut self.state;
        let mut token_index = self.fed;
        while let Some(token) = self.tokens.ready.get(token_index) {
            let ordinal = self.discarded + token_index;
            token_index += 1;
            #[cfg(test)]
            work::scan(1);
            if token.range.end <= self.processed {
                continue;
            }
            let start = token.range.start.max(base).max(self.processed);
            let mut end = token.range.end;
            if self.first_expansion.is_none() && token.kind.expands(token.range.clone()) {
                self.first_expansion = Some(start);
            }
            if matches!(token.kind, Kind::Open { .. } | Kind::AngleOpen) {
                state.remember(start, ordinal);
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
            if let Some(scope) = syntax
                .outer
                .get(scope)
                .filter(|scope| scope.range.start == start)
            {
                state.scope(scope);
                end = scope.range.end;
                if let Some(&after) = self.tokens.ends.get(&end) {
                    token_index = token_index.max(after.saturating_sub(self.discarded));
                }
            } else if inside(&syntax.opaque, &mut opaque, start) {
                state.opaque_text(start..end);
            } else {
                let linked = inside(&syntax.links, &mut link, start);
                state.feed(start..end, token.kind, linked);
            }
            if token.kind == Kind::Escape('\n') {
                self.pending_break = Some(start);
            } else if self.pending_break.is_some()
                && original.get(start..end).is_some_and(|raw| {
                    raw.bytes().any(|byte| {
                        #[cfg(test)]
                        work::scan(1);
                        !matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c)
                    })
                })
            {
                self.pending_break = None;
            }
            if state.neutral()
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
        if state.invalid {
            return sentinel;
        }
        // An unresolved construct can reopen older ancestors. Advance only
        // through a boundary actually observed with neutral scanner state.
        let candidate = self.frontier.max(base);
        self.projected = self.state.projection(self.tokens.preview().as_ref());
        let scopes = self.state.forest.scopes_since(self.projected, base);
        self.forbid_splits(&syntax.outer, scopes);
        self.syntax = syntax;
        self.checkpoint(
            self.pending_break
                .map_or(candidate, |start| candidate.min(start)),
        )
        .saturating_sub(base)
            + sentinel
    }

    fn forbid_splits(&mut self, ranges: &[Scope], scopes: Vec<Range<usize>>) {
        self.atomic.clear();
        let mut syntax = ranges.iter().map(|scope| scope.range.clone()).peekable();
        let mut scopes = scopes.into_iter().peekable();
        let mut tokens = self
            .tokens
            .ready
            .iter()
            .filter(|token| token.kind != Kind::Text)
            .map(|token| token.range.clone())
            .chain(self.tokens.preview().map(|token| token.range))
            .peekable();
        loop {
            let first = syntax
                .peek()
                .into_iter()
                .chain(scopes.peek())
                .chain(tokens.peek())
                .map(|range| range.start)
                .min();
            let Some(first) = first else {
                break;
            };
            let next = if syntax.peek().is_some_and(|range| range.start == first) {
                syntax.next()
            } else if scopes.peek().is_some_and(|range| range.start == first) {
                scopes.next()
            } else {
                tokens.next()
            };
            let Some(next) = next else {
                break;
            };
            #[cfg(test)]
            work::scan(1);
            if let Some(last) = self.atomic.last_mut()
                && next.start < last.end
            {
                last.end = last.end.max(next.end);
            } else {
                #[cfg(test)]
                work::copy(
                    std::mem::size_of::<Range<usize>>()
                        * (1 + if self.atomic.len() == self.atomic.capacity() {
                            self.atomic.len()
                        } else {
                            0
                        }),
                );
                self.atomic.push(next);
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/inline_scan.rs"]
mod tests;
