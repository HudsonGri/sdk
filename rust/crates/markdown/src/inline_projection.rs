use super::{Cache, Key, Output, text_output};
use crate::inline_scan::forest::{Forest, Kind, PAGE, Rendered};
use crate::{Line, Range, Style};

pub(in crate::inline_scan) struct Atoms<'a, 'b> {
    pub(in crate::inline_scan) source: &'b crate::inline_scan::Slice<'a>,
    pub(in crate::inline_scan) events: &'b [(crate::Event<'a>, Range<usize>)],
    pub(in crate::inline_scan) references: &'b crate::References,
    pub(in crate::inline_scan) scopes: &'b [crate::inline_scan::Scope],
    pub(in crate::inline_scan) indices: &'b std::collections::HashMap<usize, usize>,
}

pub(in crate::inline_scan) struct Projection<'a, 'b> {
    pub(in crate::inline_scan) source: &'a str,
    pub(in crate::inline_scan) visible: Range<usize>,
    pub(in crate::inline_scan) atoms: Option<Atoms<'a, 'b>>,
    pub(in crate::inline_scan) hard_breaks: &'b std::collections::HashSet<usize>,
}

impl Cache {
    fn entity(&mut self, source: &str, range: Range<usize>) -> Option<usize> {
        let parsed = source.get(range.clone())?;
        let (events, _) =
            crate::events::scoped(parsed, &crate::References::default(), crate::options(), 0);
        let slice = crate::inline_scan::Slice {
            original: source,
            parsed,
            base: range.start,
            start: self.input.origin?,
            sentinel: 0,
            boundary: 0,
        };
        let mut body = 0;
        for (event, part) in events {
            if let crate::Event::Text(text) = event {
                let leaf = self.value(
                    14,
                    range.start + part.start..range.start + part.end,
                    &text,
                    &slice,
                    Style::default(),
                )?;
                body = self.join(body, leaf);
            }
        }
        Some(body)
    }

    fn break_value(&mut self, source: &str, range: Range<usize>, escaped: bool) -> usize {
        let key = Key::Leaf(15, range.start, range.end);
        if let Some(&id) = self.keys.get(&key) {
            return id;
        }
        let horizontal = |index| {
            source
                .as_bytes()
                .get(index)
                .is_some_and(|byte| matches!(byte, b' ' | b'\t' | 0x0b | 0x0c))
        };
        let hard = escaped
            || range
                .start
                .checked_sub(2)
                .is_some_and(|at| horizontal(at) && horizontal(at + 1));
        let value = if hard {
            Output::newline(false)
        } else {
            text_output(" ".into(), Style::default())
        };
        self.insert(key, value)
    }

    fn projected(&self, forest: &Forest, cursor: usize) -> Option<Rendered> {
        if cursor == 0 {
            return Some(Rendered {
                epoch: self.epoch,
                ..Rendered::default()
            });
        }
        forest
            .node(cursor)?
            .output
            .filter(|output| output.epoch == self.epoch)
    }

    fn suffix(&mut self, body: usize, prefix: usize) -> Option<usize> {
        if prefix == 0 {
            return Some(body);
        }
        let key = Key::Suffix(body, prefix);
        if let Some(&id) = self.keys.get(&key) {
            return Some(id);
        }
        let output = self.output(body)?.suffix_after(self.output(prefix)?)?;
        Some(self.insert(key, output))
    }

    #[cfg(test)]
    pub(in crate::inline_scan) fn forest(
        &mut self,
        forest: &mut Forest,
        root: usize,
        source: &str,
        visible: Range<usize>,
        lines: &mut Vec<Line>,
        first: usize,
    ) -> Option<(usize, usize)> {
        let hard_breaks = pulldown_cmark::Parser::new_ext(source, crate::options())
            .into_offset_iter()
            .filter_map(|(event, range)| {
                matches!(event, crate::Event::HardBreak).then_some(range.start)
            })
            .collect();
        self.project(
            forest,
            root,
            Projection {
                source,
                visible,
                atoms: None,
                hard_breaks: &hard_breaks,
            },
            lines,
            first,
        )
    }

    pub(in crate::inline_scan) fn project(
        &mut self,
        forest: &mut Forest,
        root: usize,
        projection: Projection<'_, '_>,
        lines: &mut Vec<Line>,
        first: usize,
    ) -> Option<(usize, usize)> {
        let Projection {
            source,
            visible,
            atoms,
            hard_breaks,
        } = projection;
        #[cfg(test)]
        if super::REPLAY.get() {
            let epoch = self.epoch.checked_add(1).expect("render epoch");
            *self = Self::default();
            self.epoch = epoch;
        }
        self.input.feed(source, visible.start);
        if self.nodes.is_empty() {
            self.store(Output::default());
        }
        let mut pending = vec![root];
        while let Some(cursor) = pending.pop() {
            if self.projected(forest, cursor).is_some() {
                continue;
            }
            let node = forest.node(cursor)?;
            let (previous, children, stop, kind, range) = (
                node.previous,
                node.children,
                node.stop,
                node.kind,
                node.range.clone(),
            );
            let scoped = matches!(
                kind,
                Kind::Emphasis | Kind::Strong | Kind::StrongEmphasis | Kind::Strike
            );
            let dependencies = [
                previous,
                if scoped { children } else { 0 },
                if scoped { stop } else { 0 },
            ];
            if dependencies
                .iter()
                .any(|&id| self.projected(forest, id).is_none())
            {
                pending.push(cursor);
                pending.extend(
                    dependencies
                        .into_iter()
                        .filter(|&id| self.projected(forest, id).is_none()),
                );
                continue;
            }
            #[cfg(test)]
            crate::work::scan(1);
            let Rendered {
                full: mut before,
                mut trimmed,
                mut whitespace,
                mut escape,
                ..
            } = self.projected(forest, previous)?;
            let content = match kind {
                Kind::Text | Kind::Escape => {
                    let range = if kind == Kind::Text {
                        self.input.literal_range(range)?
                    } else {
                        range
                    };
                    let left = range.start + usize::from(kind == Kind::Escape);
                    let key = Key::Leaf(6, left, range.end);
                    let full = if let Some(&id) = self.keys.get(&key) {
                        id
                    } else {
                        let output = self.raw(source, left..range.end, Style::default())?;
                        self.insert(key, output)
                    };
                    let end = range.end.min(visible.end);
                    let right = if range.start < end {
                        self.input.trim_end(range.start..end)?
                    } else {
                        end
                    };
                    let left = if right < range.end { range.start } else { left };
                    if range.start < range.end && right == range.start {
                        trimmed = whitespace;
                    }
                    if left < right {
                        let key = Key::Leaf(6, left, right);
                        let visible = if let Some(&id) = self.keys.get(&key) {
                            id
                        } else {
                            let output = self.raw(source, left..right, Style::default())?;
                            self.insert(key, output)
                        };
                        trimmed = self.join(before, visible);
                        whitespace = trimmed;
                        escape = None;
                    }
                    full
                }
                Kind::Code => {
                    let key = Key::Leaf(1, range.start, range.end);
                    if let Some(&id) = self.keys.get(&key) {
                        id
                    } else {
                        let output = text_output(
                            self.input.code(children..stop, source)?,
                            Style {
                                foreground: Some(crate::style::YELLOW),
                                ..Style::default()
                            },
                        );
                        self.insert(key, output)
                    }
                }
                Kind::Atom(Some(id)) => id,
                Kind::Atom(None) => {
                    let atoms = atoms.as_ref()?;
                    let index = *atoms.indices.get(&range.start)?;
                    let scope = atoms
                        .scopes
                        .get(index)
                        .filter(|scope| scope.range == range)?;
                    let isolated = crate::inline_scan::Slice {
                        original: atoms.source.original,
                        parsed: atoms.source.parsed,
                        start: atoms.source.start,
                        base: atoms.source.base,
                        sentinel: atoms.source.sentinel,
                        // No prefix checkpoint belongs inside a completed scope.
                        boundary: atoms.source.sentinel,
                    };
                    let body = self
                        .reduce(
                            &isolated,
                            atoms.events.get(scope.events.clone())?,
                            atoms.references,
                            0,
                        )?
                        .0;
                    #[cfg(test)]
                    {
                        let entry = std::mem::size_of::<((usize, usize), (usize, usize))>();
                        crate::work::copy(
                            entry
                                * (1 + if self.atoms.len() == self.atoms.capacity() {
                                    self.atoms.len()
                                } else {
                                    0
                                }),
                        );
                    }
                    let reported = &atoms.events.get(scope.events.start)?.1;
                    self.atoms.insert(
                        (
                            isolated.base + reported.start.saturating_sub(isolated.sentinel),
                            isolated.base + reported.end.saturating_sub(isolated.sentinel),
                        ),
                        (body, scope.events.len()),
                    );
                    body
                }
                Kind::Entity => self.entity(source, range)?,
                Kind::Break(escaped) => {
                    let body = self.break_value(source, range.clone(), escaped);
                    if escaped {
                        let key = Key::Leaf(6, range.start, range.start + 1);
                        let slash = if let Some(&id) = self.keys.get(&key) {
                            id
                        } else {
                            let output =
                                self.raw(source, range.start..range.start + 1, Style::default())?;
                            self.insert(key, output)
                        };
                        trimmed = self.join(before, slash);
                        escape = Some((trimmed, range.start));
                    } else {
                        before = trimmed;
                    }
                    body
                }
                _ => {
                    let children = self.projected(forest, children)?.full;
                    let stop = self.projected(forest, stop)?.full;
                    let body = self.suffix(children, stop)?;
                    self.mask(
                        body,
                        Style {
                            italic: matches!(kind, Kind::Emphasis | Kind::StrongEmphasis),
                            bold: matches!(kind, Kind::Strong | Kind::StrongEmphasis),
                            dim: kind == Kind::Strike,
                            strikethrough: kind == Kind::Strike,
                            ..Style::default()
                        },
                        None,
                    )
                }
            };
            let full = self.join(before, content);
            if !matches!(kind, Kind::Text | Kind::Escape | Kind::Break(_)) {
                trimmed = full;
                whitespace = full;
                escape = None;
            }
            if matches!(kind, Kind::Break(_)) {
                whitespace = full;
            }
            let index = cursor.checked_sub(1)?;
            forest
                .nodes
                .get_mut(index / PAGE)?
                .get_mut(index % PAGE)?
                .output = Some(Rendered {
                epoch: self.epoch,
                full,
                trimmed,
                whitespace,
                escape,
            });
        }
        let Rendered {
            trimmed: mut current,
            escape,
            ..
        } = self.projected(forest, root)?;
        if let Some((literal, start)) = escape
            && !hard_breaks.contains(&start)
        {
            current = literal;
        }
        let output = self.output(current)?;
        let changed = output.update(self.output(self.last)?, lines, first);
        let visible = output.visible();
        self.last = current;
        Some((changed, visible))
    }
}

#[cfg(test)]
#[path = "../tests/inline_projection.rs"]
mod tests;
