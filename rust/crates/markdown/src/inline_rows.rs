use crate::{Line, Spans, Style};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(super) struct Rows {
    root: Option<Arc<Node>>,
}

struct Node {
    count: usize,
    displayed: usize,
    visible: Option<usize>,
    first: (Spans, bool),
    last: (Spans, bool),
    kind: Kind,
}

enum Kind {
    Line(Spans, bool),
    Concat(Rows, Rows),
    Mask(Rows, Style, Option<Arc<str>>),
    Suffix(Rows, usize, usize),
}

#[derive(Clone, Copy, Default)]
pub(super) struct Mask<'a> {
    style: Style,
    link: Option<&'a Arc<str>>,
}

impl<'a> Mask<'a> {
    fn inside(self, style: Style, link: Option<&'a Arc<str>>) -> Self {
        Self {
            style: Style {
                foreground: style.foreground.or(self.style.foreground),
                bold: style.bold || self.style.bold,
                italic: style.italic || self.style.italic,
                dim: style.dim || self.style.dim,
                underline: style.underline || self.style.underline,
                strikethrough: style.strikethrough || self.style.strikethrough,
            },
            link: link.or(self.link),
        }
    }

    fn same(self, other: Self) -> bool {
        self.style == other.style
            && match (self.link, other.link) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
    }
}

pub(super) struct Row<'a> {
    pub(super) spans: &'a Spans,
    pub(super) mask: Mask<'a>,
    required: bool,
}

impl Row<'_> {
    fn displayed(&self) -> bool {
        self.required || !self.spans.is_empty()
    }
    pub(super) fn owned(&self) -> Line {
        Line {
            spans: self
                .spans
                .clone()
                .masked(self.mask.style, self.mask.link.cloned()),
        }
    }
}

impl Rows {
    pub(super) fn len(&self) -> usize {
        self.root.as_ref().map_or(0, |node| node.count)
    }

    fn displayed(&self) -> usize {
        self.root.as_ref().map_or(0, |node| node.displayed)
    }

    pub(super) fn line(spans: Spans, required: bool) -> Self {
        #[cfg(test)]
        crate::work::scan(1);
        Self {
            root: Some(Arc::new(Node {
                count: 1,
                displayed: usize::from(required || !spans.is_empty()),
                visible: (!spans.is_empty()).then_some(0),
                first: (spans.clone(), required),
                last: (spans.clone(), required),
                kind: Kind::Line(spans, required),
            })),
        }
    }

    pub(super) fn concat(self, other: Self) -> Self {
        if self.len() == 0 {
            return other;
        }
        if other.len() == 0 {
            return self;
        }
        #[cfg(test)]
        crate::work::scan(1);
        Self {
            root: Some(Arc::new(Node {
                count: self.len() + other.len(),
                displayed: self.displayed() + other.displayed(),
                first: self.root.as_ref().expect("nonempty prefix").first.clone(),
                last: other.root.as_ref().expect("nonempty suffix").last.clone(),
                visible: other
                    .last_visible()
                    .map(|index| self.displayed() + index)
                    .or_else(|| self.last_visible()),
                kind: Kind::Concat(self, other),
            })),
        }
    }

    pub(super) fn masked(self, style: Style, link: Option<Arc<str>>) -> Self {
        if self.len() == 0 || style == Style::default() && link.is_none() {
            return self;
        }
        #[cfg(test)]
        crate::work::scan(1);
        Self {
            root: Some(Arc::new(Node {
                count: self.len(),
                displayed: self.displayed(),
                visible: self.last_visible(),
                first: {
                    let (spans, required) = &self.root.as_ref().expect("nonempty rows").first;
                    (spans.clone().masked(style, link.clone()), *required)
                },
                last: {
                    let (spans, required) = &self.root.as_ref().expect("nonempty rows").last;
                    (spans.clone().masked(style, link.clone()), *required)
                },
                kind: Kind::Mask(self, style, link),
            })),
        }
    }

    pub(super) fn iter(&self) -> Iter<'_> {
        Iter {
            pending: self
                .root
                .as_deref()
                .map(|node| (node, Mask::default()))
                .into_iter()
                .collect(),
        }
    }

    fn last_visible(&self) -> Option<usize> {
        self.root.as_ref().and_then(|node| node.visible)
    }

    fn row(&self, mut index: usize) -> Option<(Spans, bool)> {
        let mut rows = self;
        let mut masks = Vec::new();
        let (mut spans, required) = loop {
            let node = rows.root.as_ref()?;
            #[cfg(test)]
            crate::work::scan(1);
            if index == 0 {
                break node.first.clone();
            }
            if index == node.count.checked_sub(1)? {
                break node.last.clone();
            }
            match &node.kind {
                Kind::Line(..) => return None,
                Kind::Concat(left, right) => {
                    if index < left.len() {
                        rows = left;
                    } else {
                        index -= left.len();
                        rows = right;
                    }
                }
                Kind::Mask(child, style, link) => {
                    masks.push((*style, link.clone()));
                    rows = child;
                }
                Kind::Suffix(child, skip, _) => {
                    index += skip;
                    rows = child;
                }
            }
        };
        for (style, link) in masks.into_iter().rev() {
            spans = spans.masked(style, link);
        }
        Some((spans, required))
    }

    fn suffix(&self, skip: usize) -> Option<Self> {
        if skip == 0 {
            return Some(self.clone());
        }
        if skip == self.len() {
            return Some(Self::default());
        }
        let first = self.row(skip)?;
        let node = self.root.as_ref()?;
        let displayed = self.prefix_displayed(skip)?;
        #[cfg(test)]
        crate::work::scan(1);
        Some(Self {
            root: Some(Arc::new(Node {
                count: self.len().checked_sub(skip)?,
                displayed: self.displayed().checked_sub(displayed)?,
                visible: node.visible.and_then(|index| index.checked_sub(displayed)),
                first,
                last: node.last.clone(),
                kind: Kind::Suffix(self.clone(), skip, displayed),
            })),
        })
    }

    fn prefix_displayed(&self, mut count: usize) -> Option<usize> {
        let (mut result, mut subtract) = (0usize, 0usize);
        let mut rows = self;
        loop {
            if count == 0 {
                return result.checked_sub(subtract);
            }
            let node = rows.root.as_ref()?;
            #[cfg(test)]
            crate::work::scan(1);
            if count == node.count {
                return (result + node.displayed).checked_sub(subtract);
            }
            match &node.kind {
                Kind::Line(..) => return None,
                Kind::Concat(left, right) => {
                    if count <= left.len() {
                        rows = left;
                    } else {
                        count -= left.len();
                        result += left.displayed();
                        rows = right;
                    }
                }
                Kind::Mask(child, ..) => rows = child,
                Kind::Suffix(child, skip, displayed) => {
                    count += skip;
                    subtract += displayed;
                    rows = child;
                }
            }
        }
    }

    // Compare shared chunks before descending to individual rows. Appending a
    // row, or changing one scope, never scans an unchanged row prefix.
    pub(super) fn changed<'a>(&'a self, previous: &'a Self) -> Vec<(usize, Row<'a>)> {
        let mut before = previous.iter();
        let mut after = self.iter();
        let (mut index, mut before_index) = (0, 0);
        let mut changed = Vec::new();
        while let (Some(&(old, old_mask)), Some(&(new, new_mask))) =
            (before.pending.last(), after.pending.last())
        {
            #[cfg(test)]
            crate::work::scan(1);
            let shared = std::ptr::eq(old, new) && old_mask.same(new_mask);
            #[cfg(test)]
            let shared = shared && !SCAN_SHARED.get();
            if shared && (index == before_index || new.displayed == 0) {
                index += new.displayed;
                before_index += old.displayed;
                before.pending.pop();
                after.pending.pop();
            } else if matches!(old.kind, Kind::Mask(..) | Kind::Suffix(..)) {
                before.expand();
            } else if matches!(new.kind, Kind::Mask(..) | Kind::Suffix(..)) {
                after.expand();
            } else if old.count > new.count {
                before.expand();
            } else if new.count > old.count {
                after.expand();
            } else if matches!(old.kind, Kind::Concat(..)) || matches!(new.kind, Kind::Concat(..)) {
                before.expand();
                after.expand();
            } else {
                let Kind::Line(spans, required) = &new.kind else {
                    unreachable!("one row")
                };
                if new.displayed != 0 {
                    #[cfg(test)]
                    if changed.len() == changed.capacity() {
                        crate::work::copy(changed.len() * std::mem::size_of::<(usize, Row<'_>)>());
                    }
                    changed.push((
                        index,
                        Row {
                            spans,
                            mask: new_mask,
                            required: *required,
                        },
                    ));
                }
                index += new.displayed;
                before_index += old.displayed;
                before.pending.pop();
                after.pending.pop();
            }
        }
        for row in after {
            if !row.displayed() {
                continue;
            }
            #[cfg(test)]
            if changed.len() == changed.capacity() {
                crate::work::copy(changed.len() * std::mem::size_of::<(usize, Row<'_>)>());
            }
            changed.push((index, row));
            index += 1;
        }
        changed
    }

    #[cfg(test)]
    pub(super) fn retained(
        &self,
        nodes: &mut std::collections::HashSet<usize>,
        texts: &mut std::collections::HashSet<usize>,
        links: &mut std::collections::HashSet<*const u8>,
    ) -> (usize, usize) {
        let mut pending: Vec<_> = self.root.iter().collect();
        let (mut payload, mut metadata) = (0, 0);
        while let Some(node) = pending.pop() {
            if !nodes.insert(Arc::as_ptr(node) as usize) {
                continue;
            }
            metadata += std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>();
            match &node.kind {
                Kind::Line(spans, _) => {
                    let (bytes, size) = spans.retained(nodes, texts, links);
                    payload += bytes;
                    metadata += size;
                }
                Kind::Concat(left, right) => {
                    pending.extend(left.root.iter());
                    pending.extend(right.root.iter());
                }
                Kind::Mask(child, _, link) => {
                    pending.extend(child.root.iter());
                    if let Some(link) = link
                        && links.insert(Arc::as_ptr(link).cast::<u8>())
                    {
                        payload += link.len();
                    }
                }
                Kind::Suffix(child, ..) => pending.extend(child.root.iter()),
            }
            for (spans, _) in [&node.first, &node.last] {
                let (bytes, size) = spans.retained(nodes, texts, links);
                payload += bytes;
                metadata += size;
            }
        }
        (payload, metadata)
    }
}

#[cfg(test)]
thread_local! { static SCAN_SHARED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_rows.rs"]
mod tests;

pub(super) struct Iter<'a> {
    pending: Vec<(&'a Node, Mask<'a>)>,
}

impl Iter<'_> {
    fn expand(&mut self) {
        let Some((node, mask)) = self.pending.pop() else {
            return;
        };
        #[cfg(test)]
        crate::work::scan(1);
        match &node.kind {
            Kind::Concat(left, right) => {
                self.pending
                    .extend(right.root.as_deref().map(|node| (node, mask)));
                self.pending
                    .extend(left.root.as_deref().map(|node| (node, mask)));
            }
            Kind::Mask(child, style, link) => {
                let mask = mask.inside(*style, link.as_ref());
                self.pending
                    .extend(child.root.as_deref().map(|node| (node, mask)));
            }
            Kind::Line(..) => self.pending.push((node, mask)),
            Kind::Suffix(child, skip, _) => {
                self.pending
                    .extend(child.root.as_deref().map(|node| (node, mask)));
                self.skip(*skip);
            }
        }
    }

    fn skip(&mut self, mut count: usize) {
        while count != 0 {
            let &(node, _) = self.pending.last().expect("row range");
            #[cfg(test)]
            crate::work::scan(1);
            if node.count <= count {
                count -= node.count;
                self.pending.pop();
            } else {
                self.expand();
            }
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Row<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let &(node, mask) = self.pending.last()?;
            if let Kind::Line(spans, required) = &node.kind {
                self.pending.pop();
                return Some(Row {
                    spans,
                    mask,
                    required: *required,
                });
            }
            self.expand();
        }
    }
}

impl Drop for Rows {
    fn drop(&mut self) {
        let mut current = self.root.take();
        let mut pending = Vec::new();
        while let Some(node) = current.take().or_else(|| pending.pop()) {
            let Some(mut node) = Arc::into_inner(node) else {
                continue;
            };
            match &mut node.kind {
                Kind::Line(..) => {}
                Kind::Concat(left, right) => {
                    current = left.root.take();
                    pending.extend(right.root.take());
                }
                Kind::Mask(child, _, _) => current = child.root.take(),
                Kind::Suffix(child, ..) => current = child.root.take(),
            }
        }
    }
}

// The first and last row remain separate, so concatenation and enclosing
// styles touch a constant number of rows. Only completed middle rows enter
// the immutable row tree.
#[derive(Clone, Default)]
pub(super) struct Output {
    pub(super) head: Spans,
    pub(super) middle: Rows,
    pub(super) tail: Option<Spans>,
    pub(super) required: bool,
}

impl Output {
    pub(super) fn suffix_after(&self, prefix: &Self) -> Option<Self> {
        #[cfg(test)]
        if SCAN_SUFFIX.get() {
            for row in self.middle.iter() {
                let _ = row.owned();
            }
        }
        let Some(start) = &prefix.tail else {
            return Some(Self {
                head: self.head.suffix_after(&prefix.head),
                middle: self.middle.clone(),
                tail: self.tail.clone(),
                required: self.required,
            });
        };
        let at = 1 + prefix.middle.len();
        let last = 1 + self.middle.len();
        if at == last {
            return Some(Self::text(self.tail.as_ref()?.suffix_after(start)));
        }
        let index = at - 1;
        let (row, required) = self.middle.row(index)?;
        Some(Self {
            head: row.suffix_after(start),
            middle: self.middle.suffix(index + 1)?,
            tail: self.tail.clone(),
            #[cfg(test)]
            required: required || FORGET_BREAK_KIND.get(),
            #[cfg(not(test))]
            required,
        })
    }

    pub(super) fn visible(&self) -> usize {
        #[cfg(test)]
        if SCAN_VISIBLE.get() {
            return self
                .lines()
                .iter()
                .rposition(|line| {
                    crate::work::visibility(1);
                    !line.spans.is_empty()
                })
                .map_or(0, |index| index + 1);
        }
        if self.tail.as_ref().is_some_and(|tail| !tail.is_empty()) {
            return self.len();
        }
        if let Some(index) = self.middle.last_visible() {
            return self.head_count() + index + 1;
        }
        if self.head.is_empty() { 0 } else { 1 }
    }
    fn head_count(&self) -> usize {
        usize::from(self.tail.is_none() || self.required || !self.head.is_empty())
    }

    fn len(&self) -> usize {
        self.head_count() + self.middle.displayed() + usize::from(self.tail.is_some())
    }

    pub(super) fn text(head: Spans) -> Self {
        Self {
            head,
            ..Self::default()
        }
    }

    pub(super) fn newline(required: bool) -> Self {
        Self {
            tail: Some(Spans::default()),
            required,
            ..Self::default()
        }
    }

    pub(super) fn append(&mut self, other: Self) {
        let Self {
            head,
            middle,
            tail,
            required,
        } = other;
        if let Some(last) = &mut self.tail {
            last.concat(head);
            last.freeze();
            if let Some(tail) = tail {
                let joined = self.tail.take().expect("existing final row");
                let row = Rows::line(joined, required);
                self.middle = std::mem::take(&mut self.middle).concat(row).concat(middle);
                self.tail = Some(tail);
            }
        } else {
            self.head.concat(head);
            self.head.freeze();
            if tail.is_some() {
                self.middle = middle;
                self.tail = tail;
                self.required = required;
            }
        }
    }

    pub(super) fn masked(self, style: Style, link: Option<Arc<str>>) -> Self {
        Self {
            head: self.head.masked(style, link.clone()),
            middle: self.middle.masked(style, link.clone()),
            tail: self.tail.map(|tail| tail.masked(style, link)),
            required: self.required,
        }
    }

    pub(super) fn lines(&self) -> Vec<Line> {
        let mut lines = Vec::new();
        if self.tail.is_none() || self.required || !self.head.is_empty() {
            lines.push(Line {
                spans: self.head.clone(),
            });
        }
        lines.extend(
            self.middle
                .iter()
                .filter(|row| row.displayed())
                .map(|row| row.owned()),
        );
        lines.extend(self.tail.iter().cloned().map(|spans| Line { spans }));
        lines
    }

    pub(super) fn update(&self, previous: &Self, lines: &mut Vec<Line>, first: usize) -> usize {
        if self.head_count() != previous.head_count()
            || self.tail.is_some() != previous.tail.is_some()
            || lines.len() != first + previous.len()
        {
            let next = self.lines();
            #[cfg(test)]
            {
                crate::work::copy(next.len() * std::mem::size_of::<Line>());
                if lines.capacity() < first + next.len() {
                    crate::work::copy(first * std::mem::size_of::<Line>());
                }
            }
            lines.truncate(first);
            lines.extend(next);
            return first;
        }
        let mut changed = first + self.len();
        let end = first + self.len();
        if end > lines.len() {
            #[cfg(test)]
            {
                if lines.capacity() < end {
                    crate::work::copy(lines.len() * std::mem::size_of::<Line>());
                }
            }
            // New rows are appended from the changed iterator below; do not
            // initialize empty rows only to overwrite them with rendered ones.
            lines.reserve(end - lines.len());
        } else {
            lines.truncate(end);
        }
        if self.head_count() != 0 && !self.head.same_snapshot(&previous.head) {
            lines[first].spans.clone_from(&self.head);
            changed = first;
        }
        for (index, row) in self.middle.changed(&previous.middle) {
            let at = first + self.head_count() + index;
            #[cfg(test)]
            crate::work::copy(std::mem::size_of::<Line>());
            if at == lines.len() {
                lines.push(row.owned());
            } else {
                lines[at] = row.owned();
            }
            changed = changed.min(at);
        }
        if let (Some(next), Some(old)) = (&self.tail, &previous.tail)
            && (self.len() != previous.len() || !next.same_snapshot(old))
        {
            let at = first + self.len() - 1;
            if at == lines.len() {
                #[cfg(test)]
                crate::work::copy(std::mem::size_of::<Line>());
                lines.push(Line {
                    spans: next.clone(),
                });
            } else {
                lines[at].spans.clone_from(next);
            }
            changed = changed.min(at);
        }
        debug_assert_eq!(lines.len(), end);
        changed
    }
}

#[cfg(test)]
pub(in crate::inline_scan) use SCAN_VISIBLE as RESCAN_VISIBLE;

#[cfg(test)]
thread_local! { pub(in crate::inline_scan) static SCAN_VISIBLE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
thread_local! { static FORGET_BREAK_KIND: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
thread_local! { static SCAN_SUFFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
