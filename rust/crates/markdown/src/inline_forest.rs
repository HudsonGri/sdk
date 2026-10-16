use super::delimiters::Matched;
use std::ops::Range;
#[path = "inline_glyphs.rs"]
pub(in crate::inline_scan) mod glyphs;
pub(super) use glyphs::Cache;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Text,
    Escape,
    Code,
    Atom(Option<usize>),
    Break(bool),
    Entity,
    Emphasis,
    Strong,
    StrongEmphasis,
    Strike,
}

#[derive(Clone, Copy, Default)]
pub(in crate::inline_scan) struct Rendered {
    pub(in crate::inline_scan) epoch: usize,
    pub(in crate::inline_scan) full: usize,
    pub(in crate::inline_scan) trimmed: usize,
    pub(in crate::inline_scan) whitespace: usize,
    pub(in crate::inline_scan) escape: Option<(usize, usize)>,
}

struct Node {
    previous: usize,
    range: Range<usize>,
    children: usize,
    stop: usize,
    kind: Kind,
    output: Option<Rendered>,
}

const PAGE: usize = 8;

#[derive(Default)]
pub(super) struct Forest {
    nodes: Vec<Vec<Node>>,
    length: usize,
    root: usize,
    #[cfg(test)]
    scan: bool,
    #[cfg(test)]
    stale: bool,
}

impl Forest {
    pub(super) fn reset(&mut self) {
        self.length = 0;
        self.root = 0;
    }

    pub(super) fn snapshot(&self) -> usize {
        self.root
    }

    pub(super) fn restore(&mut self, cursor: usize) -> bool {
        if cursor > self.length {
            return false;
        }
        self.root = cursor;
        true
    }

    fn node(&self, cursor: usize) -> Option<&Node> {
        if cursor > self.length {
            return None;
        }
        let index = cursor.checked_sub(1)?;
        self.nodes.get(index / PAGE)?.get(index % PAGE)
    }

    pub(super) fn text_prefix(&self, cursor: usize) -> Option<(usize, Range<usize>)> {
        let node = self.node(cursor)?;
        (node.kind == Kind::Text).then(|| (node.previous, node.range.clone()))
    }

    fn append(&mut self, range: Range<usize>, children: usize, stop: usize, kind: Kind) -> usize {
        #[cfg(test)]
        crate::work::scan(1);
        if self.length.is_multiple_of(PAGE) {
            if self.length / PAGE == self.nodes.len() {
                #[cfg(test)]
                if self.nodes.len() == self.nodes.capacity() {
                    crate::work::copy(self.nodes.len() * std::mem::size_of::<Vec<Node>>());
                }
                self.nodes.push(Vec::with_capacity(PAGE));
            } else {
                self.nodes
                    .get_mut(self.length / PAGE)
                    .expect("existing node page")
                    .clear();
            }
        }
        self.nodes
            .get_mut(self.length / PAGE)
            .expect("node page allocated")
            .push(Node {
                previous: self.root,
                range,
                children,
                stop,
                kind,
                output: None,
            });
        self.length += 1;
        self.root = self.length;
        self.snapshot()
    }

    pub(super) fn text(&mut self, range: Range<usize>) -> usize {
        if range.is_empty() {
            self.root
        } else {
            self.append(range, 0, 0, Kind::Text)
        }
    }

    pub(super) fn escape(&mut self, range: Range<usize>) -> usize {
        self.append(range, 0, 0, Kind::Escape)
    }

    #[cfg(test)]
    pub(super) fn atom(&mut self, range: Range<usize>, output: usize) -> usize {
        self.append(range, 0, 0, Kind::Atom(Some(output)))
    }

    pub(super) fn opaque(&mut self, range: Range<usize>) {
        self.append(range, 0, 0, Kind::Atom(None));
    }

    pub(super) fn leaf(&mut self, range: Range<usize>, token: super::tokens::Kind) -> usize {
        let kind = match token {
            super::tokens::Kind::Escape('\n') => Kind::Break(true),
            super::tokens::Kind::Break => Kind::Break(false),
            super::tokens::Kind::Entity => Kind::Entity,
            _ => return self.text(range),
        };
        self.append(range, 0, 0, kind)
    }

    pub(super) fn code(&mut self, opening: Range<usize>, closing: Range<usize>) {
        self.append(
            opening.start..closing.end,
            opening.end,
            closing.start,
            Kind::Code,
        );
    }

    pub(super) fn wrap(&mut self, matched: Matched) -> Option<(usize, usize)> {
        let opening = self.node(matched.opener_cursor)?;
        let closing = self.node(matched.closer_cursor)?;
        if opening.kind != Kind::Text
            || closing.kind != Kind::Text
            || opening.range.start > matched.opener.start
            || opening.range.end < matched.opener.end
            || closing.range.start > matched.closer.start
            || closing.range.end < matched.closer.end
            || matched.opener.is_empty()
            || matched.opener.end > matched.closer.start
            || matched.opener.len() != matched.closer.len()
            || matched.opener_cursor >= matched.closer_cursor
            || matched.closer_cursor != self.root
        {
            return None;
        }
        let (prefix, start, end, children) = (
            opening.previous,
            opening.range.start,
            closing.range.end,
            closing.previous,
        );
        #[cfg(test)]
        if self.scan {
            let mut cursor = children;
            while cursor != matched.opener_cursor {
                crate::work::scan(1);
                cursor = self.node(cursor)?.previous;
            }
        }
        let width = matched.opener.len();
        let kind = match (matched.byte, width >= 2, width % 2 != 0) {
            (b'~', _, _) => Kind::Strike,
            (b'*' | b'_', true, true) => Kind::StrongEmphasis,
            (b'*' | b'_', true, false) => Kind::Strong,
            (b'*' | b'_', false, _) => Kind::Emphasis,
            _ => return None,
        };
        if !self.restore(prefix) {
            return None;
        }
        let left = self.text(start..matched.opener.start);
        self.append(
            matched.opener.start..matched.closer.end,
            children,
            matched.opener_cursor,
            kind,
        );
        let right = self.text(matched.closer.end..end);
        #[cfg(test)]
        let left = if self.stale && start < matched.opener.start {
            matched.opener_cursor
        } else {
            left
        };
        Some((left, right))
    }

    #[cfg(test)]
    pub(super) fn scopes(&self) -> Vec<Range<usize>> {
        self.scopes_at(self.root)
    }

    #[cfg(test)]
    pub(super) fn scopes_at(&self, cursor: usize) -> Vec<Range<usize>> {
        self.scopes_since(cursor, 0)
    }

    pub(super) fn scopes_since(&self, mut cursor: usize, start: usize) -> Vec<Range<usize>> {
        let mut result = Vec::new();
        while let Some(node) = self.node(cursor) {
            #[cfg(test)]
            crate::work::scan(1);
            if node.range.end <= start {
                break;
            }
            if node.kind != Kind::Text {
                debug_assert!(
                    matches!(node.kind, Kind::Escape | Kind::Code)
                        || node.stop <= node.children && node.children < cursor
                );
                #[cfg(test)]
                crate::work::copy(
                    std::mem::size_of::<Range<usize>>()
                        * (1 + if result.len() == result.capacity() {
                            result.len()
                        } else {
                            0
                        }),
                );
                result.push(node.range.clone());
            }
            cursor = node.previous;
        }
        #[cfg(test)]
        crate::work::copy(result.len() * std::mem::size_of::<Range<usize>>());
        result.reverse();
        result
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.nodes.capacity() * std::mem::size_of::<Vec<Node>>()
            + self
                .nodes
                .iter()
                .map(|page| page.capacity() * std::mem::size_of::<Node>())
                .sum::<usize>()
    }

    #[cfg(test)]
    pub(super) fn rendered(&self, source: &str) -> Option<crate::Line> {
        self.rendered_at(self.root, source)
    }

    #[cfg(test)]
    pub(super) fn rendered_at(&self, root: usize, source: &str) -> Option<crate::Line> {
        use crate::Style;
        enum Task {
            List(usize, usize, Style),
            Value(usize, Style),
        }
        let mut tasks = vec![Task::List(root, 0, Style::default())];
        let mut line = crate::Line::default();
        let end = source.trim_end_matches([' ', '\t']).len();
        while let Some(task) = tasks.pop() {
            match task {
                Task::List(cursor, stop, style) => {
                    if cursor == stop {
                        continue;
                    }
                    let node = self.node(cursor)?;
                    tasks.push(Task::Value(cursor, style));
                    tasks.push(Task::List(node.previous, stop, style));
                }
                Task::Value(cursor, mut style) => {
                    let node = self.node(cursor)?;
                    match node.kind {
                        Kind::Text => line.push(
                            source.get(node.range.start.min(end)..node.range.end.min(end))?,
                            style,
                            None,
                        ),
                        Kind::Escape => line.push(
                            source.get(node.range.start + 1..node.range.end)?,
                            style,
                            None,
                        ),
                        Kind::Code => {
                            let code = source.get(node.children..node.stop)?;
                            let code = code.replace(['\r', '\n'], " ");
                            let code = if code.starts_with(' ')
                                && code.ends_with(' ')
                                && code.bytes().any(|byte| byte != b' ')
                            {
                                code.get(1..code.len() - 1)?
                            } else {
                                &code
                            };
                            style.foreground = Some(crate::style::YELLOW);
                            line.push(code, style, None);
                        }
                        kind => {
                            style.bold |= matches!(kind, Kind::Strong | Kind::StrongEmphasis);
                            style.italic |= matches!(kind, Kind::Emphasis | Kind::StrongEmphasis);
                            style.strikethrough |= kind == Kind::Strike;
                            style.dim |= kind == Kind::Strike;
                            tasks.push(Task::List(node.children, node.stop, style));
                        }
                    }
                }
            }
        }
        Some(line)
    }
}

#[cfg(test)]
#[path = "../tests/inline_forest.rs"]
mod tests;
