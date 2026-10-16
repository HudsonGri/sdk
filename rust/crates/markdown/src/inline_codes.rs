use super::Snapshot;
use crate::inline_scan::forest::PAGE;
use std::{collections::HashMap, ops::Range};

pub(super) struct Code {
    pub(super) opening: Range<usize>,
    pub(super) before: Snapshot,
    previous: usize,
    depth: usize,
    earliest: usize,
}

#[derive(Default)]
pub(super) struct Codes {
    nodes: Vec<Vec<Code>>,
    length: usize,
    head: usize,
    index: HashMap<usize, usize>,
    #[cfg(test)]
    stale: bool,
}

impl Codes {
    pub(super) fn head(&self) -> usize {
        self.head
    }

    pub(super) fn valid(&self, cursor: usize) -> bool {
        cursor <= self.length
    }

    fn node(&self, cursor: usize) -> Option<&Code> {
        if !self.valid(cursor) {
            return None;
        }
        let index = cursor.checked_sub(1)?;
        self.nodes.get(index / PAGE)?.get(index % PAGE)
    }

    pub(super) fn get(&self, width: usize) -> Option<&Code> {
        self.node(*self.index.get(&width)?)
    }

    pub(super) fn earliest(&self) -> Option<usize> {
        self.node(self.head).map(|code| code.earliest)
    }

    pub(super) fn reset(&mut self) {
        #[cfg(test)]
        if !self.index.is_empty() {
            crate::work::copy(self.index.capacity());
        }
        self.index.clear();
        self.head = 0;
        self.length = 0;
    }

    pub(super) fn push(&mut self, opening: Range<usize>, before: Snapshot) {
        debug_assert_eq!(before.code, self.head);
        debug_assert!(!self.index.contains_key(&opening.len()));
        let depth = self.node(self.head).map_or(0, |node| node.depth) + 1;
        let earliest = self
            .earliest()
            .unwrap_or(opening.start)
            .min(before.frontier);
        if self.length.is_multiple_of(PAGE) {
            let page = self.length / PAGE;
            if page == self.nodes.len() {
                #[cfg(test)]
                if self.nodes.len() == self.nodes.capacity() {
                    crate::work::copy(self.nodes.len() * std::mem::size_of::<Vec<Code>>());
                }
                self.nodes.push(Vec::with_capacity(PAGE));
            } else {
                self.nodes[page].clear();
            }
        }
        let width = opening.len();
        self.nodes[self.length / PAGE].push(Code {
            opening,
            before,
            previous: self.head,
            depth,
            earliest,
        });
        self.length += 1;
        self.head = self.length;
        self.insert(width, self.head);
    }

    fn insert(&mut self, width: usize, cursor: usize) {
        #[cfg(test)]
        crate::work::copy(
            std::mem::size_of::<(usize, usize)>()
                * (1 + if self.index.len() == self.index.capacity() {
                    self.index.len()
                } else {
                    0
                }),
        );
        self.index.insert(width, cursor);
    }

    pub(super) fn restore(&mut self, mut cursor: usize) -> bool {
        if !self.valid(cursor) {
            return false;
        }
        let mut pending = Vec::new();
        while self.head != cursor {
            #[cfg(test)]
            crate::work::scan(1);
            let depth = self.node(self.head).map_or(0, |node| node.depth);
            let target_depth = self.node(cursor).map_or(0, |node| node.depth);
            if depth >= target_depth {
                let node = self.node(self.head).expect("active code context");
                let (width, previous) = (node.opening.len(), node.previous);
                self.index.remove(&width);
                self.head = previous;
            } else {
                #[cfg(test)]
                if pending.len() == pending.capacity() {
                    crate::work::copy(pending.len() * std::mem::size_of::<usize>());
                }
                pending.push(cursor);
                cursor = self.node(cursor).expect("retained code context").previous;
            }
        }
        for cursor in pending.into_iter().rev() {
            let width = self
                .node(cursor)
                .expect("retained code opener")
                .opening
                .len();
            #[cfg(test)]
            let insert = !self.stale;
            #[cfg(not(test))]
            let insert = true;
            if insert {
                self.insert(width, cursor);
            }
            self.head = cursor;
        }
        true
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.nodes.capacity() * std::mem::size_of::<Vec<Code>>()
            + self
                .nodes
                .iter()
                .map(|page| page.capacity() * std::mem::size_of::<Code>())
                .sum::<usize>()
            + self.index.capacity() * std::mem::size_of::<(usize, usize)>()
    }
}

#[cfg(test)]
#[path = "../tests/inline_codes.rs"]
mod tests;
