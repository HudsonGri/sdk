use super::{Snapshot, State};

#[derive(Clone)]
pub(super) struct Anchor {
    before: Snapshot,
    ordinal: usize,
}

impl State {
    pub(in crate::inline_scan) fn remember(&mut self, position: usize, ordinal: usize) {
        let before = self.snapshot();
        #[cfg(test)]
        {
            crate::work::scan(1);
            if self.anchors.len() == self.anchors.capacity() {
                crate::work::copy(self.anchors.len() * std::mem::size_of::<(usize, Anchor)>());
            }
        }
        self.anchors.insert(position, Anchor { before, ordinal });
    }

    pub(in crate::inline_scan) fn restore_at(&mut self, position: usize) -> Option<usize> {
        let anchor = self.anchors.get(&position)?.clone();
        #[cfg(test)]
        if SKIP_RESTORE.get() {
            return Some(anchor.ordinal);
        }
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Anchor>());
        if !self.codes.valid(anchor.before.code) {
            return None;
        }
        self.restore(anchor.before);
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
        (!self.invalid).then_some(anchor.ordinal)
    }
}

#[cfg(test)]
thread_local! { static SKIP_RESTORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_anchors.rs"]
mod tests;
