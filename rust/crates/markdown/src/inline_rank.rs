use super::super::PAGE;

#[derive(Clone, Copy, Default)]
struct Word {
    bits: u64,
    prefix: usize,
    previous: Option<usize>,
}

#[derive(Default)]
pub(super) struct Rank {
    pages: Vec<Box<[Word; PAGE]>>,
    length: usize,
    ones: usize,
}

impl Rank {
    fn word(&self, index: usize) -> Option<&Word> {
        if index >= self.length.div_ceil(64) {
            return None;
        }
        self.pages.get(index / PAGE)?.get(index % PAGE)
    }

    pub(super) fn push(&mut self, value: bool) {
        let bit = self.length % 64;
        let index = self.length / 64;
        #[cfg(test)]
        crate::work::scan(1);
        if bit == 0 {
            if index.is_multiple_of(PAGE) {
                #[cfg(test)]
                {
                    if self.pages.len() == self.pages.capacity() {
                        crate::work::copy(
                            self.pages.len() * std::mem::size_of::<Box<[Word; PAGE]>>(),
                        );
                    }
                    crate::work::copy(std::mem::size_of::<[Word; PAGE]>());
                }
                self.pages.push(Box::new([Word::default(); PAGE]));
            }
            let previous = index.checked_sub(1).and_then(|before| {
                let word = self.word(before).expect("previous rank word");
                if word.bits != 0 {
                    Some(before)
                } else {
                    word.previous
                }
            });
            #[cfg(test)]
            crate::work::copy(std::mem::size_of::<Word>());
            self.pages[index / PAGE][index % PAGE] = Word {
                bits: 0,
                prefix: self.ones,
                previous,
            };
        }
        if value {
            self.pages[index / PAGE][index % PAGE].bits |= 1u64 << bit;
            self.ones += 1;
        }
        self.length += 1;
    }

    pub(super) fn before(&self, position: usize) -> Option<usize> {
        if position > self.length {
            return None;
        }
        if position == self.length {
            return Some(self.ones);
        }
        let mask = (1u64 << (position % 64)) - 1;
        let word = self.word(position / 64)?;
        let bits = word.bits & mask;
        #[cfg(test)]
        if OMIT_PREFIX.get() {
            return Some(bits.count_ones() as usize);
        }
        Some(word.prefix + bits.count_ones() as usize)
    }

    pub(super) fn last_before(&self, position: usize) -> Option<usize> {
        #[cfg(test)]
        crate::work::scan(1);
        if position > self.length {
            return None;
        }
        let index = position.checked_sub(1)? / 64;
        let mask = u64::MAX >> (63 - (position - 1) % 64);
        let word = self.word(index)?.bits & mask;
        let (index, word) = if word == 0 {
            let previous = self.word(index)?.previous?;
            (previous, self.word(previous)?.bits)
        } else {
            (index, word)
        };
        Some(index * 64 + 63 - word.leading_zeros() as usize)
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.pages.capacity() * std::mem::size_of::<Box<[Word; PAGE]>>()
            + self.pages.len() * std::mem::size_of::<[Word; PAGE]>()
    }
}

#[cfg(test)]
thread_local! { static OMIT_PREFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_rank.rs"]
mod tests;
