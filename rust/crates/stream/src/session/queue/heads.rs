use super::*;

/// A byte-minimum tree over reusable channel slots. Only channel heads enter
/// this index. Round-robin eligible selection cannot starve a larger head that
/// fits the aggregate grant, and never scans blocked channels on each delivery.
#[derive(Default)]
pub(in crate::session) struct Heads {
    ids: Vec<Option<u64>>,
    free: Vec<usize>,
    minimum: Vec<u64>,
    leaves: usize,
    cursor: usize,
}
impl Heads {
    pub(in crate::session) fn insert(&mut self, id: u64) -> Result<usize, StreamError> {
        if let Some(slot) = self.free.pop() {
            self.ids[slot] = Some(id);
            return Ok(slot);
        }
        let slot = self.ids.len();
        if slot == self.leaves {
            let leaves = self
                .leaves
                .max(1)
                .checked_mul(if self.leaves == 0 { 1 } else { 2 })
                .ok_or(StreamError::Capacity)?;
            let mut minimum = vec![u64::MAX; leaves.checked_mul(2).ok_or(StreamError::Capacity)?];
            for i in 0..self.ids.len() {
                examined();
                minimum[leaves + i] = self.minimum[self.leaves + i];
            }
            for i in (1..leaves).rev() {
                examined();
                minimum[i] = minimum[2 * i].min(minimum[2 * i + 1]);
            }
            self.minimum = minimum;
            self.leaves = leaves;
        }
        self.ids.push(Some(id));
        Ok(slot)
    }
    pub(in crate::session) fn remove(&mut self, slot: usize) -> Result<(), StreamError> {
        self.set(slot, None)?;
        self.ids[slot] = None;
        self.free.push(slot);
        Ok(())
    }
    pub(in crate::session) fn set(
        &mut self,
        slot: usize,
        bytes: Option<usize>,
    ) -> Result<(), StreamError> {
        if self.ids.get(slot).is_none_or(Option::is_none) {
            return Err(StreamError::Unavailable);
        }
        let mut node = self.leaves + slot;
        let bytes = bytes.map_or(u64::MAX, |bytes| bytes as u64);
        if self.minimum[node] == bytes {
            return Ok(());
        }
        self.minimum[node] = bytes;
        while node > 1 {
            examined();
            node /= 2;
            self.minimum[node] = self.minimum[2 * node].min(self.minimum[2 * node + 1]);
        }
        Ok(())
    }
    pub(in crate::session) fn next(&mut self, bytes: u64) -> Option<u64> {
        if self.ids.is_empty() {
            return None;
        }
        let slot = self
            .find(1, 0, self.leaves, self.cursor, self.ids.len(), bytes)
            .or_else(|| self.find(1, 0, self.leaves, 0, self.cursor, bytes))?;
        self.cursor = (slot + 1) % self.ids.len();
        self.ids[slot]
    }
    #[cfg(any(test, acyclic_stream_loom))]
    pub(super) fn owners(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        self.ids
            .iter()
            .enumerate()
            .filter_map(|(slot, id)| id.map(|id| (slot, id)))
    }
    #[cfg(all(test, not(acyclic_stream_loom)))]
    pub(super) fn height(&self) -> usize {
        self.leaves.max(1).ilog2() as usize + 1
    }
    fn find(
        &self,
        node: usize,
        left: usize,
        right: usize,
        from: usize,
        through: usize,
        bytes: u64,
    ) -> Option<usize> {
        examined();
        if right <= from
            || left >= through
            || self.minimum[node] == u64::MAX
            || self.minimum[node] > bytes
        {
            return None;
        }
        if right - left == 1 {
            return Some(left);
        }
        let middle = left + (right - left) / 2;
        self.find(2 * node, left, middle, from, through, bytes)
            .or_else(|| self.find(2 * node + 1, middle, right, from, through, bytes))
    }
}

#[cfg(all(test, not(acyclic_stream_loom)))]
mod tests;
