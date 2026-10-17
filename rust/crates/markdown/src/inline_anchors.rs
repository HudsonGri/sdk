use super::State;

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Context {
    pub(super) code: usize,
    pub(super) links: usize,
    pub(super) floor: usize,
    pub(super) hint: bool,
    pub(super) opaque: Option<std::num::NonZeroUsize>,
    pub(super) reference: Option<std::num::NonZeroUsize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(in crate::inline_scan) struct Anchor {
    pub(super) context: std::sync::Arc<Context>,
    pub(super) delimiter: usize,
    pub(super) forest: usize,
    pub(super) bracket: usize,
    pub(super) closed: usize,
}

impl State {
    pub(in crate::inline_scan) fn forget(&mut self, ordinal: usize) {
        let Some(ordinal) = ordinal.checked_sub(self.anchor_origin) else {
            return;
        };
        #[cfg(test)]
        crate::work::scan(1);
        if let Some(slot) = self
            .anchors
            .get_mut(ordinal / super::forest::PAGE)
            .and_then(|page| page.get_mut(ordinal % super::forest::PAGE))
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
            opaque: self
                .opaque
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
        Anchor {
            context: self
                .anchor_context
                .as_ref()
                .expect("resume context")
                .clone(),
            delimiter,
            forest: self.forest.snapshot(),
            bracket: self.bracket,
            closed: self.closed,
        }
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
        while self.anchor_slots <= ordinal {
            if self.anchor_slots.is_multiple_of(super::forest::PAGE) {
                #[cfg(test)]
                if self.anchors.len() == self.anchors.capacity() {
                    crate::work::copy(
                        self.anchors.len() * std::mem::size_of::<Vec<Option<Box<Anchor>>>>(),
                    );
                }
                self.anchors.push(Vec::with_capacity(super::forest::PAGE));
            }
            #[cfg(test)]
            crate::work::copy(std::mem::size_of::<Option<Box<Anchor>>>());
            self.anchors.last_mut().expect("resume page").push(None);
            self.anchor_slots += 1;
        }
        self.anchors[ordinal / super::forest::PAGE][ordinal % super::forest::PAGE] =
            Some(Box::new(anchor));
    }

    pub(in crate::inline_scan) fn bind_resume(&mut self, anchor: Anchor) -> Option<()> {
        let (code, closed, links) = (self.codes.head(), self.closed, self.link_floor);
        self.restore_resume(anchor, code, closed, links);
        (!self.invalid).then_some(())
    }

    pub(in crate::inline_scan) fn restore_at(&mut self, ordinal: usize) -> Option<()> {
        let ordinal = ordinal.checked_sub(self.anchor_origin)?;
        let anchor = self
            .anchors
            .get(ordinal / super::forest::PAGE)?
            .get(ordinal % super::forest::PAGE)?
            .as_deref()?
            .clone();
        #[cfg(test)]
        if SKIP_RESTORE.get() {
            return Some(());
        }
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Anchor>());
        if !self.codes.valid(anchor.context.code) {
            return None;
        }
        let (code, closed, links) = (anchor.context.code, anchor.closed, anchor.context.links);
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
