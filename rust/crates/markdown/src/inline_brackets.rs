use super::State;
use std::ops::Range;

pub(super) struct Closed {
    pub(super) previous: usize,
    pub(super) range: Range<usize>,
    pub(super) image: bool,
    pub(super) opening: usize,
}

impl State {
    pub(super) fn preview_code(
        &self,
        token: &crate::inline_scan::tokens::Token,
    ) -> Option<&super::codes::Code> {
        use crate::inline_scan::tokens::Kind;
        let escaped = match token.kind {
            Kind::Run { byte: b'`', .. } => false,
            Kind::Escape('`') => true,
            _ => return None,
        };
        self.codes
            .get(token.range.len().checked_sub(usize::from(escaped))?)
    }

    #[cfg(test)]
    fn closed_at(&self, pending: Option<&crate::inline_scan::tokens::Token>) -> usize {
        pending
            .and_then(|token| self.preview_code(token))
            .map_or(self.closed, |code| code.before.closed)
    }

    pub(super) fn label(&self, cursor: usize) -> Option<&Closed> {
        let label = self.closed_labels.get(cursor.checked_sub(1)?)?;
        debug_assert!(label.previous < cursor);
        if label.opening != 0 {
            let opening = self.brackets.get(label.opening - 1)?;
            debug_assert_eq!(opening.start, label.range.start);
            debug_assert_eq!(opening.image, label.image);
        }
        Some(label)
    }

    pub(super) fn remember_closed(
        &mut self,
        range: Range<usize>,
        image: bool,
        opening: usize,
    ) -> usize {
        #[cfg(test)]
        crate::work::copy(
            std::mem::size_of::<Closed>()
                * (1 + if self.closed_labels.len() == self.closed_labels.capacity() {
                    self.closed_labels.len()
                } else {
                    0
                }),
        );
        self.closed_labels.push(Closed {
            previous: self.closed,
            range,
            image,
            opening,
        });
        self.closed = self.closed_labels.len();
        self.closed
    }
}

#[cfg(test)]
thread_local! { pub(super) static FORGET_RESTORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_brackets.rs"]
mod tests;
