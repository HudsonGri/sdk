use super::State;

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Context {
    pub(super) code: usize,
    pub(super) links: usize,
    pub(super) floor: usize,
    pub(super) hint: bool,
    pub(super) paren: Option<std::num::NonZeroUsize>,
    pub(super) reference: Option<std::num::NonZeroUsize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(in crate::inline_scan) struct Anchor {
    pub(super) context: std::sync::Arc<Context>,
    // Entry ordinals within one paragraph. An anchor is kept per opener, so
    // they are stored narrow; each entry spans at least one source byte.
    delimiter: u32,
    forest: u32,
    bracket: u32,
    closed: u32,
    wide: Option<std::sync::Arc<[usize; 4]>>,
}

impl Anchor {
    fn new(context: std::sync::Arc<Context>, ordinals: [usize; 4]) -> Self {
        let wide = ordinals
            .iter()
            .any(|&value| u32::try_from(value).is_err())
            .then(|| std::sync::Arc::new(ordinals));
        #[cfg(test)]
        if wide.is_some() {
            crate::work::copy(std::mem::size_of_val(&ordinals));
        }
        let [delimiter, forest, bracket, closed] =
            ordinals.map(|value| u32::try_from(value).unwrap_or_default());
        Self {
            context,
            delimiter,
            forest,
            bracket,
            closed,
            wide,
        }
    }
    pub(super) fn delimiter(&self) -> usize {
        self.wide.as_deref().map_or(self.delimiter as usize, |v| {
            let [value, ..] = v;
            *value
        })
    }
    pub(in crate::inline_scan) fn forest(&self) -> usize {
        self.wide.as_deref().map_or(self.forest as usize, |v| {
            let [_, value, ..] = v;
            *value
        })
    }
    pub(super) fn bracket(&self) -> usize {
        self.wide.as_deref().map_or(self.bracket as usize, |v| {
            let [_, _, value, _] = v;
            *value
        })
    }
    pub(super) fn closed(&self) -> usize {
        self.wide.as_deref().map_or(self.closed as usize, |v| {
            let [.., value] = v;
            *value
        })
    }
}

impl State {
    pub(in crate::inline_scan) fn forget(&mut self, ordinal: usize) {
        let Some(ordinal) = ordinal.checked_sub(self.anchor_origin) else {
            return;
        };
        #[cfg(test)]
        crate::work::scan(1);
        let (page, offset) = super::forest::slot(ordinal);
        if let Some(slot) = self
            .anchors
            .get_mut(page)
            .and_then(|page| page.get_mut(offset))
        {
            *slot = None;
        }
    }

    pub(in crate::inline_scan) fn resume(&mut self) -> Anchor {
        debug_assert!(self.labels == [0, 0] || self.labels == [self.closed, 0]);
        let (delimiter, floor) = self.delimiter.snapshot().parts();
        let context = Context {
            reference: self
                .reference
                .and_then(|position| std::num::NonZeroUsize::new(position + 1)),
            code: self.codes.head(),
            links: self.link_floor,
            floor,
            hint: self.labels != [0, 0],
            paren: self
                .paren
                .and_then(|position| std::num::NonZeroUsize::new(position + 1)),
        };
        #[cfg(test)]
        crate::work::scan(6);
        if self.anchor_context.as_deref() != Some(&context) {
            #[cfg(test)]
            crate::work::scan(6);
            if self.anchor_context_other.as_deref() == Some(&context) {
                std::mem::swap(&mut self.anchor_context, &mut self.anchor_context_other);
            } else {
                #[cfg(test)]
                crate::work::copy(std::mem::size_of::<Context>());
                self.anchor_context_other =
                    self.anchor_context.replace(std::sync::Arc::new(context));
            }
        }
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Anchor>());
        let Some(context) = self.anchor_context.clone() else {
            unreachable!("resume initializes the context above");
        };
        Anchor::new(
            context,
            [delimiter, self.forest.snapshot(), self.bracket, self.closed],
        )
    }

    pub(in crate::inline_scan) fn remember(&mut self, ordinal: usize) {
        let anchor = self.resume();
        self.remember_resume(ordinal, anchor);
    }

    pub(in crate::inline_scan) fn remember_resume(&mut self, ordinal: usize, anchor: Anchor) {
        let ordinal = ordinal
            .checked_sub(self.anchor_origin)
            .expect("mutable opener");
        #[cfg(test)]
        crate::work::scan(1);
        let mut anchor = Some(anchor);
        while self.anchor_slots <= ordinal {
            if super::forest::slot(self.anchor_slots).1 == 0 {
                #[cfg(test)]
                if self.anchors.len() == self.anchors.capacity() {
                    crate::work::copy(
                        self.anchors.len() * std::mem::size_of::<Vec<Option<Anchor>>>(),
                    );
                }
                self.anchors
                    .push(super::forest::new_page(self.anchors.len()));
            }
            let value = if self.anchor_slots == ordinal {
                anchor.take()
            } else {
                #[cfg(test)]
                crate::work::copy(std::mem::size_of::<usize>());
                None
            };
            super::forest::push_page(self.anchors.last_mut().expect("resume page"), value);
            self.anchor_slots += 1;
        }
        if let Some(anchor) = anchor {
            let (page, offset) = super::forest::slot(ordinal);
            self.anchors[page][offset] = Some(anchor);
        }
    }

    pub(in crate::inline_scan) fn bind_resume(&mut self, anchor: Anchor) -> Option<()> {
        let (code, closed, links) = (self.codes.head(), self.closed, self.link_floor);
        self.restore_resume(anchor, code, closed, links);
        (!self.invalid).then_some(())
    }

    pub(in crate::inline_scan) fn restore_at(&mut self, ordinal: usize) -> Option<()> {
        let ordinal = ordinal.checked_sub(self.anchor_origin)?;
        let (page, offset) = super::forest::slot(ordinal);
        let anchor = self.anchors.get(page)?.get(offset)?.as_ref()?.clone();
        #[cfg(test)]
        if SKIP_RESTORE.get() {
            return Some(());
        }
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Anchor>());
        if !self.codes.valid(anchor.context.code) {
            return None;
        }
        let (code, closed, links) = (anchor.context.code, anchor.closed(), anchor.context.links);
        self.restore_resume(anchor, code, closed, links);
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
        (!self.invalid).then_some(())
    }
}

#[cfg(test)]
thread_local! { static SKIP_RESTORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_anchors.rs"]
mod tests;
