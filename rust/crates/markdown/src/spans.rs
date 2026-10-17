use crate::{Span, Style};
#[cfg(any(test, feature = "fuzz-support"))]
use std::collections::HashSet;
use std::{fmt, sync::Arc};

#[path = "spans_view.rs"]
mod view;
pub use view::{SpanRef, TextRef};

/// A snapshot of ordered styled runs with shared immutable prefixes.
#[derive(Default)]
pub struct Spans {
    prefix: Option<Arc<Node>>,
    stop: Option<Arc<Node>>,
    tail: Option<Box<Span>>,
    forced: bool,
}

struct Node {
    previous: Option<Arc<Node>>,
    count: Option<usize>,
    runs: usize,
    part: Part,
}

enum Part {
    Span(Box<Span>, bool),
    Group(Spans, Style, Option<Arc<str>>, bool),
}

impl Spans {
    /// Visit canonical styled runs, retaining explicit link boundaries.
    pub fn iter(&self) -> view::Iter<'_> {
        view::Iter::new(self)
    }

    /// Number of canonical styled runs.
    pub fn len(&self) -> usize {
        self.known_len().unwrap_or_else(|| self.iter().count())
    }

    fn known_len(&self) -> Option<usize> {
        if self.stop.is_some() {
            return None;
        }
        Some(
            self.prefix.as_ref().map_or(Some(0), |node| node.count)?
                + usize::from(self.tail.is_some()),
        )
    }

    /// Whether the sequence has no runs.
    pub fn is_empty(&self) -> bool {
        self.raw_count() == 0
    }

    fn raw_count(&self) -> usize {
        self.prefix.as_ref().map_or(0, |node| node.runs)
            - self.stop.as_ref().map_or(0, |node| node.runs)
            + usize::from(self.tail.is_some())
    }

    /// The first canonical run.
    pub fn first(&self) -> Option<SpanRef<'_>> {
        if self.prefix.is_none() {
            return self.tail.as_ref().map(|tail| {
                let mut span = SpanRef::borrowed(tail);
                span.boundary = self.forced;
                span
            });
        }
        self.iter().next()
    }

    /// The last canonical run.
    pub fn last(&self) -> Option<SpanRef<'_>> {
        if let Some(tail) = &self.tail {
            return Some(SpanRef::borrowed(tail));
        }
        self.iter().last()
    }

    /// Mutate the final run; shared prefixes remain unchanged.
    pub fn last_mut(&mut self) -> Option<&mut Span> {
        if self.is_empty() {
            return None;
        }
        if self.tail.is_none()
            && let Some(node) = &self.prefix
        {
            match &node.part {
                Part::Span(span, forced) => {
                    #[cfg(test)]
                    crate::work::copy(std::mem::size_of::<Span>());
                    self.tail = Some(span.clone());
                    self.forced = *forced;
                    self.prefix = node.previous.clone();
                }
                Part::Group(..) => {
                    *self = self.materialized(usize::MAX);
                }
            }
        }
        self.tail.as_deref_mut()
    }

    /// Append a distinct run, preserving its boundary.
    pub fn push(&mut self, span: Span) {
        self.push_content(span, true);
    }

    pub(crate) fn push_content(&mut self, span: Span, forced: bool) {
        self.flush_tail();
        self.tail = Some(Box::new(span));
        self.forced = forced;
    }

    fn flush_tail(&mut self) {
        if let Some(span) = self.tail.take() {
            let count = self
                .prefix
                .as_ref()
                .map_or(Some(0), |node| node.count)
                .map(|count| count + 1);
            #[cfg(test)]
            crate::work::scan(1);
            self.prefix = Some(Arc::new(Node {
                runs: self.prefix.as_ref().map_or(0, |node| node.runs) + 1,
                previous: self.prefix.take(),
                count,
                part: Part::Span(span, self.forced),
            }));
        }
    }

    pub(crate) fn freeze(&mut self) {
        self.flush_tail();
    }

    // Both values are frozen cursors in the same immutable sibling list.
    // The inline forest owns and validates their ancestry before constructing
    // the view; no traversal or copying of the enclosed children is needed.
    pub(crate) fn suffix_after(&self, start: &Self) -> Self {
        debug_assert!(self.tail.is_none() && start.tail.is_none());
        let result = Self {
            prefix: self.prefix.clone(),
            stop: start.prefix.clone().or_else(|| self.stop.clone()),
            tail: None,
            forced: false,
        };
        if result.is_empty() {
            Self::default()
        } else {
            result
        }
    }

    /// Remove every run from this snapshot.
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// Retain the first `length` canonical runs.
    pub fn truncate(&mut self, length: usize) {
        let current = self.len();
        if length >= current {
            return;
        }
        if length == 0 {
            self.clear();
            return;
        }
        if self.known_len().is_none() {
            *self = self.materialized(length);
            return;
        }
        let mut remaining = current;
        remaining -= usize::from(self.tail.take().is_some());
        while remaining > length {
            let Some(node) = &self.prefix else { break };
            if matches!(node.part, Part::Span(..)) {
                self.prefix = node.previous.clone();
                remaining -= 1;
            } else {
                *self = self.materialized(length);
                return;
            }
        }
    }

    fn materialized(&self, length: usize) -> Self {
        let mut spans = Self::default();
        for span in self.iter().take(length) {
            let forced = span.boundary;
            spans.push_content(span.owned(), forced);
        }
        spans
    }

    /// Materialize owned runs when a caller needs to edit the whole sequence.
    pub fn to_vec(&self) -> Vec<Span> {
        let mut result = Vec::new();
        for span in self.iter() {
            #[cfg(test)]
            if result.len() == result.capacity() {
                crate::work::copy(result.len() * std::mem::size_of::<Span>());
            }
            result.push(span.owned());
        }
        result
    }

    pub(crate) fn append_shared(&mut self, other: Spans) {
        self.group(other, Style::default(), None, true);
    }

    /// Apply an enclosing style without copying its child runs.
    pub fn styled(self, style: Style) -> Self {
        let mut spans = Self::default();
        spans.group(self, style, None, false);
        spans
    }

    pub(crate) fn masked(self, style: Style, link: Option<Arc<str>>) -> Self {
        if style == Style::default() && link.is_none() {
            return self;
        }
        let mut spans = Self::default();
        let boundary = link.is_some();
        spans.group(self, style, link, boundary);
        spans
    }

    pub(crate) fn same_snapshot(&self, other: &Self) -> bool {
        fn same(left: &Option<Arc<Node>>, right: &Option<Arc<Node>>) -> bool {
            match (left, right) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
        }
        self.tail.is_none()
            && other.tail.is_none()
            && same(&self.prefix, &other.prefix)
            && same(&self.stop, &other.stop)
    }

    /// Concatenate parsed content, coalescing equal styles at the boundary.
    pub fn concat(&mut self, other: Spans) {
        self.group(other, Style::default(), None, false);
    }

    fn group(&mut self, other: Spans, style: Style, link: Option<Arc<str>>, forced: bool) {
        if other.prefix.is_none() && other.tail.is_none() && link.is_none() {
            return;
        }
        self.flush_tail();
        let count = if style == Style::default() && forced {
            self.known_len()
                .zip(other.known_len())
                .map(|(left, right)| left + right)
        } else {
            None
        };
        #[cfg(test)]
        crate::work::scan(1);
        self.prefix = Some(Arc::new(Node {
            runs: self.prefix.as_ref().map_or(0, |node| node.runs) + other.raw_count(),
            previous: self.prefix.take(),
            count,
            part: Part::Group(other, style, link, forced),
        }));
    }

    /// Count shared run nodes, text backing and link targets once per address.
    #[cfg(any(test, feature = "fuzz-support"))]
    pub fn retained(
        &self,
        nodes: &mut HashSet<usize>,
        texts: &mut HashSet<usize>,
        links: &mut HashSet<*const u8>,
    ) -> (usize, usize) {
        let mut pending = vec![self];
        let (mut payload, mut metadata) = (0, 0);
        while let Some(spans) = pending.pop() {
            if let Some(tail) = &spans.tail {
                let (bytes, size) = retained_span(tail, texts, links);
                payload += bytes;
                metadata += size;
            }
            let mut current = spans.prefix.as_ref();
            while let Some(node) = current {
                if !nodes.insert(Arc::as_ptr(node) as usize) {
                    break;
                }
                metadata += std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>();
                match &node.part {
                    Part::Span(span, _) => {
                        let (bytes, size) = retained_span(span, texts, links);
                        payload += bytes;
                        metadata += size;
                    }
                    Part::Group(child, _, link, _) => {
                        if let Some(link) = link
                            && links.insert(Arc::as_ptr(link).cast::<u8>())
                        {
                            payload += link.len();
                        }
                        pending.push(child);
                    }
                }
                current = node.previous.as_ref();
            }
        }
        (payload, metadata)
    }
}

#[cfg(any(test, feature = "fuzz-support"))]
fn retained_span(
    span: &Span,
    texts: &mut HashSet<usize>,
    links: &mut HashSet<*const u8>,
) -> (usize, usize) {
    let (mut payload, metadata) = span.text.retained(texts);
    if let Some(link) = &span.link
        && links.insert(Arc::as_ptr(link).cast::<u8>())
    {
        payload += link.len();
    }
    (payload, metadata + std::mem::size_of::<Span>())
}

impl Clone for Spans {
    fn clone(&self) -> Self {
        #[cfg(test)]
        if COPY_PREFIX.get() {
            return self.iter().map(SpanRef::owned).collect();
        }
        #[cfg(test)]
        if self.tail.is_some() {
            crate::work::copy(std::mem::size_of::<Span>());
        }
        Self {
            prefix: self.prefix.clone(),
            stop: self.stop.clone(),
            tail: self.tail.clone(),
            forced: self.forced,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        #[cfg(test)]
        if COPY_PREFIX.get() {
            *self = source.clone();
            return;
        }
        if self.tail.is_none() && source.tail.is_none() {
            #[cfg(test)]
            crate::work::copy(2 * std::mem::size_of::<Option<Arc<Node>>>());
            let old = Self {
                prefix: self.prefix.take(),
                stop: self.stop.take(),
                tail: None,
                forced: false,
            };
            self.prefix = source.prefix.clone();
            self.stop = source.stop.clone();
            drop(old);
        } else {
            #[cfg(test)]
            crate::work::copy(std::mem::size_of::<Self>());
            *self = source.clone();
        }
    }
}

impl Drop for Spans {
    fn drop(&mut self) {
        let mut current = self.prefix.take();
        let mut pending: Vec<_> = self.stop.take().into_iter().collect();
        while let Some(node) = current.take().or_else(|| pending.pop()) {
            let Some(mut node) = Arc::into_inner(node) else {
                continue;
            };
            #[cfg(test)]
            crate::work::scan(1);
            current = node.previous.take();
            if let Part::Group(child, _, _, _) = &mut node.part {
                pending.extend(child.prefix.take());
                pending.extend(child.stop.take());
            }
        }
    }
}

impl From<Vec<Span>> for Spans {
    fn from(spans: Vec<Span>) -> Self {
        spans.into_iter().collect()
    }
}
impl FromIterator<Span> for Spans {
    fn from_iter<T: IntoIterator<Item = Span>>(spans: T) -> Self {
        let mut result = Self::default();
        result.extend(spans);
        result
    }
}
impl Extend<Span> for Spans {
    fn extend<T: IntoIterator<Item = Span>>(&mut self, spans: T) {
        for span in spans {
            self.push(span);
        }
    }
}
impl<'a> IntoIterator for &'a Spans {
    type Item = SpanRef<'a>;
    type IntoIter = view::Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl IntoIterator for Spans {
    type Item = Span;
    type IntoIter = std::vec::IntoIter<Span>;
    fn into_iter(self) -> Self::IntoIter {
        self.to_vec().into_iter()
    }
}
impl PartialEq for Spans {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for Spans {}
impl fmt::Debug for Spans {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

fn overlay(inner: Style, outer: Style) -> Style {
    Style {
        foreground: inner.foreground.or(outer.foreground),
        bold: inner.bold || outer.bold,
        italic: inner.italic || outer.italic,
        dim: inner.dim || outer.dim,
        underline: inner.underline || outer.underline,
        strikethrough: inner.strikethrough || outer.strikethrough,
    }
}

#[cfg(test)]
thread_local! { static COPY_PREFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/spans.rs"]
mod tests;
