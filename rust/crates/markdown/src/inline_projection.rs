use super::{Cache, Key, Output, text_output};
use crate::inline_scan::forest::{Forest, Kind, PAGE, Rendered};
use crate::{Line, Range, Style};

pub(in crate::inline_scan) struct Projection<'a, 'b> {
    pub(in crate::inline_scan) source: &'a str,
    pub(in crate::inline_scan) visible: Range<usize>,
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
            classified: true,
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
        if let Some(&(id, _)) = self.keys.get(&key) {
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
        let output = forest.node(cursor)?.output;
        (output.epoch == self.epoch).then_some(output)
    }

    pub(in crate::inline_scan) fn freeze_prefix(
        &mut self,
        forest: &mut Forest,
        root: usize,
        projection: Projection<'_, '_>,
    ) -> Option<Rendered> {
        if !self.rendered {
            return None;
        }
        #[cfg(test)]
        let rendering = crate::work::rendering(true);
        let result = (|| {
            let old = self.resolve(forest, root, &projection)?;
            let ids = [old.full, old.trimmed, old.whitespace, self.last];
            let mut saved: [Option<Output>; 4] = std::array::from_fn(|_| None);
            for i in 0..4 {
                if ids[i] != 0 && !ids[..i].contains(&ids[i]) {
                    let (page, offset) = Self::slot(ids[i]);
                    let output = self.nodes.get_mut(page)?.get_mut(offset)?;
                    saved[i] = Some(std::mem::take(output));
                    #[cfg(test)]
                    crate::work::scan(1);
                }
            }
            let input = std::mem::take(&mut self.input);
            let destinations = std::mem::take(&mut self.destinations);
            #[cfg(test)]
            let committed = self.committed;
            *self = Self::default();
            self.input = input;
            self.destinations = destinations;
            #[cfg(test)]
            {
                self.committed = committed;
            }
            self.store(Output::default());
            let mut compiled = [0; 4];
            for i in 0..4 {
                compiled[i] = if ids[i] == 0 {
                    0
                } else if let Some(previous) = ids[..i].iter().position(|id| *id == ids[i]) {
                    compiled[previous]
                } else {
                    self.store(saved[i].take()?)
                };
            }
            self.last = compiled[3];
            self.rendered = true;
            Some(Rendered {
                epoch: self.epoch,
                full: compiled[0],
                trimmed: compiled[1],
                whitespace: compiled[2],
                escape: None,
            })
        })();
        #[cfg(test)]
        crate::work::rendering(rendering);
        result
    }

    fn suffix(&mut self, body: usize, prefix: usize) -> Option<usize> {
        if prefix == 0 {
            return Some(body);
        }
        let key = Key::Suffix(body, prefix);
        if let Some(&(id, _)) = self.keys.get(&key) {
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
                hard_breaks: &hard_breaks,
            },
            lines,
            first,
            None,
        )
    }

    pub(in crate::inline_scan) fn project(
        &mut self,
        forest: &mut Forest,
        root: usize,
        projection: Projection<'_, '_>,
        lines: &mut Vec<Line>,
        first: usize,
        spare: Option<&mut crate::inline_views::Spare>,
    ) -> Option<(usize, usize)> {
        let Rendered {
            trimmed: mut current,
            escape,
            ..
        } = self.resolve(forest, root, &projection)?;
        if let Some((literal, start)) = escape
            && !projection.hard_breaks.contains(&start)
        {
            current = literal;
        }
        #[cfg(test)]
        let spare = spare.filter(|_| !crate::inline_views::REPLAY.get());
        let mut exchanged = false;
        if let Some(spare) = spare {
            if self.alternate.is_some_and(|(_, start)| start != first) {
                self.alternate = None;
            }
            if self.rendered && !self.output(current)?.same_mask(self.output(self.last)?) {
                if let Some((alternate, _)) = self.alternate
                    && self.output(current)?.same_mask(self.output(alternate)?)
                {
                    spare.exchange(lines, first);
                    self.alternate = Some((self.last, first));
                    self.last = alternate;
                    exchanged = true;
                } else {
                    spare.capture(lines);
                    self.alternate = Some((self.last, first));
                }
            }
        }
        let output = self.output(current)?;
        let changed = output.update(self.output(self.last)?, lines, first);
        let changed = if exchanged { first } else { changed };
        let visible = output.visible();
        self.last = current;
        self.rendered = true;
        Some((changed, visible))
    }

    pub(super) fn resolve(
        &mut self,
        forest: &mut Forest,
        root: usize,
        projection: &Projection<'_, '_>,
    ) -> Option<Rendered> {
        let source = projection.source;
        let visible = &projection.visible;
        #[cfg(test)]
        if super::REPLAY.get() {
            let epoch = self.epoch.checked_add(1).expect("render epoch");
            self.input = super::Input::default();
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
                    let full = if let Some(&(id, _)) = self.keys.get(&key) {
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
                        let visible = if let Some(&(id, _)) = self.keys.get(&key) {
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
                    if let Some(&(id, _)) = self.keys.get(&key) {
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
                Kind::Atom | Kind::Prefix => stop,
                Kind::Entity => self.entity(source, range)?,
                Kind::Break(escaped) => {
                    let body = self.break_value(source, range.clone(), escaped);
                    if escaped {
                        let key = Key::Leaf(6, range.start, range.start + 1);
                        let slash = if let Some(&(id, _)) = self.keys.get(&key) {
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
                .output = Rendered {
                epoch: self.epoch,
                full,
                trimmed,
                whitespace,
                escape,
            };
        }
        self.projected(forest, root)
    }
}

#[cfg(test)]
#[path = "../tests/inline_projection.rs"]
mod tests;
