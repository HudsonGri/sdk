//! Directive presentation at parser event boundaries, preserving surrounding styles.
use super::{Directive, Kind, Located, Resolution, Stream};
use crate::{Key, References, render::Renderer};
use pulldown_cmark::{Event, Tag};
use std::ops::Range;

pub(crate) fn key(position: usize) -> Key {
    crate::normalize(&format!("\0file:{position}"))
}

pub(crate) struct Projection {
    nodes: Vec<Located>,
    next: usize,
    until: usize,
    offsets: Vec<(usize, usize)>,
}
impl Projection {
    pub(crate) fn new(source: &str, events: &[(Event<'_>, Range<usize>)]) -> Self {
        #[cfg(test)]
        crate::work::scan(source.len());
        if !source.contains(":file[") {
            return Self {
                nodes: Vec::new(),
                next: 0,
                until: 0,
                offsets: Vec::new(),
            };
        }
        let mut scanner = Stream::default();
        scanner.push(source);
        scanner.finish();
        let mut excluded = events
            .iter()
            .filter_map(|(event, range)| {
                matches!(
                    event,
                    Event::Code(_)
                        | Event::Html(_)
                        | Event::InlineHtml(_)
                        | Event::Start(Tag::CodeBlock(_))
                )
                .then_some(range)
            })
            .peekable();
        let nodes = scanner
            .nodes()
            .iter()
            .filter(|node| {
                while excluded
                    .peek()
                    .is_some_and(|range| range.end <= node.range.start)
                {
                    excluded.next();
                }
                node.directive.target().is_some()
                    && !excluded.peek().is_some_and(|range| {
                        range.start < node.range.end && node.range.start < range.end
                    })
            })
            .cloned()
            .collect();
        Self {
            nodes,
            next: 0,
            until: 0,
            offsets: Vec::new(),
        }
    }
    pub(crate) fn offsets(mut self, offsets: &[(usize, usize)]) -> Self {
        if !self.nodes.is_empty() {
            self.offsets.extend_from_slice(offsets);
        }
        self
    }
    fn position(&self, offset: usize) -> usize {
        let index = self.offsets.partition_point(|(start, _)| *start <= offset);
        index
            .checked_sub(1)
            .and_then(|index| self.offsets.get(index))
            .map_or(offset, |(start, original)| original + offset - start)
    }
    pub(crate) fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.nodes
            .iter()
            .map(|node| key(self.position(node.range.start)))
    }
    pub(crate) fn event(
        &mut self,
        out: &mut Renderer,
        event: &Event<'_>,
        range: &Range<usize>,
        references: &References,
    ) {
        // Container events enclose the whole directive; inline events inside a
        // substituted directive are discarded together with its literal text.
        if self.until > range.start && range.end <= self.until {
            return;
        }
        let Event::Text(text) = event else {
            out.event_in(event, Some(references));
            return;
        };
        let mut at = self.until.saturating_sub(range.start).min(text.len());
        while let Some(node) = self.nodes.get(self.next) {
            if node.range.start >= range.end {
                break;
            }
            self.next += 1;
            if node.range.start < range.start {
                continue;
            }
            let resolution = references
                .citations
                .get(&key(self.position(node.range.start)));
            if node.directive.kind() == Kind::Block && resolution.is_none() {
                continue;
            }
            let start = node.range.start - range.start;
            let Some(prefix) = text.get(at..start) else {
                continue;
            };
            out.event_in(&Event::Text(prefix.into()), Some(references));
            out.directive(&node.directive, resolution);
            self.until = node.range.end;
            at = node.range.end.saturating_sub(range.start).min(text.len());
        }
        if let Some(suffix) = text.get(at..) {
            out.event_in(&Event::Text(suffix.into()), Some(references));
        }
    }
}

impl Renderer {
    pub(crate) fn directive(&mut self, directive: &Directive, resolution: Option<&Resolution>) {
        self.directive_lines(directive.kind(), directive.render(resolution));
    }
}
