use super::*;

#[derive(Default)]
pub(super) struct Checkpoint {
    offset: usize,
    cost: usize,
    pub(super) valid: Range<usize>,
}

#[derive(Default)]
pub(super) struct Inline {
    pub(super) checkpoint: Checkpoint,
    start: usize,
    scanner: inline_scan::Scanner,
}

impl Inline {
    #[cfg(test)]
    pub(super) fn retained(
        &self,
        nodes: &mut HashSet<usize>,
        texts: &mut HashSet<usize>,
        links: &mut HashSet<*const u8>,
    ) -> (usize, usize) {
        let (payload, metadata) = self.scanner.glyph_retained(nodes, texts, links);
        (payload, metadata + self.scanner.retained())
    }
    pub(super) fn visible_frontier(&self, stable: usize, fallback: usize) -> (usize, usize) {
        (stable, fallback)
    }
}

impl Markdown {
    pub(super) fn advance_inline(&mut self, previous: usize) -> Option<usize> {
        let mut inline = self.inline.take()?;
        let checkpoint = &inline.checkpoint;
        let cost = checkpoint.cost;
        let base = checkpoint.offset;
        let tail = self.source.get(base..).unwrap_or_default();
        let context = if base == inline.start {
            String::new()
        } else if self
            .source
            .get(previous..)
            .unwrap_or_default()
            .contains('\n')
            || tail.contains('\n')
        {
            row_prefix(self.source.get(..base).unwrap_or_default())
        } else {
            left_context(self.source.get(..base).unwrap_or_default())
        };
        let mut prefix = context;
        if inline.scanner.expanded_before(base) {
            // Keep the row's column shape and final flanking class intact.
            if let Some(at) = prefix.find('x') {
                prefix.insert_str(at, "`x`");
            } else {
                prefix.insert_str(0, "`x`\n");
            }
        }
        let sentinel = prefix.len();
        let source = if prefix.is_empty() {
            std::borrow::Cow::Borrowed(tail)
        } else {
            #[cfg(test)]
            work::copy(prefix.len() + tail.len());
            std::borrow::Cow::Owned(format!("{prefix}{tail}"))
        };
        #[cfg(test)]
        work::copy(prefix.len());
        let (events, calls) = events::scoped(
            &source,
            &self.references,
            options(),
            self.source
                .len()
                .max(100_000)
                .saturating_sub(self.reference_cost)
                .saturating_sub(cost),
        );
        if !paragraph(&source, &events)
            || events.iter().any(|(event, range)| {
                matches!(event, Event::Start(tag) if !matches!(tag, Tag::Paragraph))
                    && range.start < sentinel
                    && range.end > sentinel
            })
        {
            return None;
        }
        let frontier = inline
            .scanner
            .frontier(&self.source, base, &events, sentinel);
        let boundary = boundary(&source, &events, sentinel, frontier);
        let boundary = inline
            .scanner
            .checkpoint(base + boundary.saturating_sub(sentinel))
            .saturating_sub(base)
            + sentinel;
        let (cost, valid) = references::extend_expansion(
            cost,
            &checkpoint.valid,
            calls.iter().filter(|(end, _)| *end <= boundary),
        );
        let (changed, visible) = inline.scanner.render_events(
            inline_scan::Slice {
                original: &self.source,
                parsed: &source,
                start: inline.start,
                base,
                sentinel,
                boundary,
            },
            &events,
            &self.references,
            &mut self.lines,
            self.stable_lines,
        )?;
        self.visible_lines = if visible == 0 {
            self.visibility.end
        } else {
            self.stable_lines + visible
        };
        self.plain = false;
        let offset = base + boundary.saturating_sub(sentinel);
        inline.scanner.discard_before(offset);
        inline.checkpoint.offset = offset;
        inline.checkpoint.cost = cost;
        inline.checkpoint.valid = valid;
        self.inline = Some(inline);
        Some(changed)
    }

    pub(super) fn begin_inline(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        base: usize,
    ) {
        let paragraph_start = events.first().map_or(0, |(_, range)| range.start);
        let body = source.get(paragraph_start..).unwrap_or_default();
        let content = body.trim_start_matches([' ', '\t']);
        if possible_definition(content)
            || !self.context.is_empty()
            || self.offset != base
            || !paragraph(source, events)
        {
            return;
        }
        let start = base + paragraph_start + body.len() - content.len();
        self.inline = Some(Inline {
            checkpoint: Checkpoint {
                offset: start,
                cost: 0,
                valid: 0..usize::MAX,
            },
            start,
            scanner: inline_scan::Scanner::new(&self.source, start),
        });
        // Replay only this initial open tail; subsequent pushes retain its prefix.
        let plain = self.plain;
        let _ = self.advance_inline(self.source.len());
        self.plain = plain;
    }
}

fn left_context(source: &str) -> String {
    match source.chars().next_back() {
        Some('\n') => "x\n".into(),
        Some(c) if c.is_whitespace() => "x ".into(),
        Some(c) if !c.is_ascii() => format!("x{c}"),
        Some(c) if !c.is_ascii_punctuation() => "x".into(),
        _ => "x.".into(),
    }
}

fn table_candidate(source: &str) -> bool {
    source.rsplit_once('\n').is_some_and(|(_, next)| {
        next.bytes()
            .all(|byte| matches!(byte, b'|' | b'-' | b':' | b' ' | b'\t'))
    })
}

// Only column structure is needed to recognize a table interrupt after a soft
// break. Keep its source text in the rendered checkpoint, not in parser input.
fn row_prefix(source: &str) -> String {
    let row = source.rsplit('\n').next().unwrap_or_default();
    #[cfg(test)]
    work::scan(row.len() * 2);
    if !row.contains('|') {
        return left_context(source);
    }
    #[cfg(test)]
    work::scan(row.len());
    let (mut escaped, mut content) = (false, false);
    let mut prefix = String::new();
    for byte in row.bytes() {
        if byte == b'|' && !escaped {
            prefix.push('|');
            content = false;
        } else if !byte.is_ascii_whitespace() && !content {
            prefix.push('x');
            content = true;
        }
        escaped = byte == b'\\' && !escaped;
    }
    match left_context(source).as_str() {
        "x " => prefix.push(' '),
        "x" => {}
        _ if prefix.ends_with('|') => {}
        context => {
            prefix.pop();
            prefix.push(context.chars().next_back().unwrap_or('.'));
        }
    }
    prefix
}

// Until content follows an empty marker it can still interrupt this paragraph.
fn block_pending(row: &str) -> bool {
    let row = row.trim_start_matches([' ', '\t']);
    if row
        .bytes()
        .all(|byte| matches!(byte, b' ' | b'\t' | b'-' | b'_' | b'*'))
    {
        return true;
    }
    let marker = row.trim_start_matches(|c: char| c.is_ascii_digit());
    let digits = row.len() - marker.len();
    (1..=9).contains(&digits)
        && (marker.is_empty()
            || marker
                .strip_prefix(['.', ')'])
                .is_some_and(|rest| rest.bytes().all(|byte| matches!(byte, b' ' | b'\t'))))
        || row
            .strip_prefix(['-', '+', '*'])
            .is_some_and(|rest| rest.bytes().all(|byte| matches!(byte, b' ' | b'\t')))
}

fn paragraph(source: &str, events: &[(Event<'_>, Range<usize>)]) -> bool {
    matches!(events.first(), Some((Event::Start(Tag::Paragraph), _)))
        && events.iter().filter(|(event, _)| matches!(event, Event::Start(Tag::Paragraph))).count() == 1
        && !events.iter().any(|(event, _)| matches!(event, Event::Start(tag) if super::is_leaf(tag) && !matches!(tag, Tag::Paragraph)))
        && events.last().is_some_and(|(event, range)| matches!(event, Event::End(TagEnd::Paragraph))
            && source.get(range.end..).unwrap_or_default().bytes().all(|byte| matches!(byte, b' ' | b'\t')))
}

fn possible_definition(source: &str) -> bool {
    let Some(label) = source.strip_prefix('[') else {
        return false;
    };
    let mut escaped = false;
    for (at, character) in label.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' => escaped = true,
            '[' => return false,
            ']' => {
                return label
                    .get(at + 1..)
                    .is_some_and(|tail| tail.is_empty() || tail.starts_with(':'));
            }
            _ => {}
        }
    }
    true
}

fn boundary(
    source: &str,
    events: &[(Event<'_>, Range<usize>)],
    sentinel: usize,
    frontier: usize,
) -> usize {
    let (prefix, row) = source.rsplit_once('\n').unwrap_or(("", source));
    let mut limit = frontier;
    if block_pending(row) {
        limit = limit.min(prefix.len());
    }
    if table_candidate(source) {
        limit = limit.min(source.rfind('\n').unwrap_or(source.len()));
    }
    limit = limit.min(source.trim_end_matches([' ', '\t']).len());
    let mut depth = 0usize;
    let mut boundary = sentinel;
    for (event, range) in events {
        match event {
            Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph) => continue,
            Event::Start(_) => depth += 1,
            Event::End(_) => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 {
            if range.end <= limit && range.end > sentinel {
                boundary = range.end;
            } else if let Event::Text(text) = event
                && source.get(range.clone()) == Some(text.as_ref())
                && range.start <= limit
                && range.end > limit
                && limit > sentinel
            {
                boundary = limit;
            }
        }
    }
    boundary
}
