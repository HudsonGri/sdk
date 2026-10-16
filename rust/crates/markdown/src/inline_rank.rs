#[derive(Default)]
pub(super) struct Rank {
    words: Vec<u64>,
    prefixes: Vec<usize>,
    previous: Vec<Option<usize>>,
    length: usize,
    ones: usize,
}

impl Rank {
    pub(super) fn push(&mut self, value: bool) {
        let bit = self.length % 64;
        #[cfg(test)]
        crate::work::scan(1);
        if bit == 0 {
            #[cfg(test)]
            {
                crate::work::copy(std::mem::size_of::<u64>() + std::mem::size_of::<usize>());
                crate::work::copy(std::mem::size_of::<Option<usize>>());
                if self.previous.len() == self.previous.capacity() {
                    crate::work::copy(self.previous.len() * std::mem::size_of::<Option<usize>>());
                }
                if self.words.len() == self.words.capacity() {
                    crate::work::copy(self.words.len() * std::mem::size_of::<u64>());
                }
                if self.prefixes.len() == self.prefixes.capacity() {
                    crate::work::copy(self.prefixes.len() * std::mem::size_of::<usize>());
                }
            }
            self.previous
                .push(if self.words.last().is_some_and(|word| *word != 0) {
                    self.words.len().checked_sub(1)
                } else {
                    self.previous.last().copied().flatten()
                });
            self.words.push(0);
            self.prefixes.push(self.ones);
        }
        if value {
            *self.words.last_mut().expect("current rank word") |= 1u64 << bit;
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
        let word = self.words.get(position / 64)? & mask;
        #[cfg(test)]
        if OMIT_PREFIX.get() {
            return Some(word.count_ones() as usize);
        }
        Some(self.prefixes.get(position / 64)? + word.count_ones() as usize)
    }

    pub(super) fn last_before(&self, position: usize) -> Option<usize> {
        #[cfg(test)]
        crate::work::scan(1);
        if position > self.length {
            return None;
        }
        let index = position.checked_sub(1)? / 64;
        let mask = u64::MAX >> (63 - (position - 1) % 64);
        let word = self.words.get(index)? & mask;
        let (index, word) = if word == 0 {
            let previous = self.previous.get(index).copied().flatten()?;
            (previous, *self.words.get(previous)?)
        } else {
            (index, word)
        };
        Some(index * 64 + 63 - word.leading_zeros() as usize)
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.words.capacity() * std::mem::size_of::<u64>()
            + self.prefixes.capacity() * std::mem::size_of::<usize>()
            + self.previous.capacity() * std::mem::size_of::<Option<usize>>()
    }
}

#[cfg(test)]
thread_local! { static OMIT_PREFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_rank.rs"]
mod tests;
