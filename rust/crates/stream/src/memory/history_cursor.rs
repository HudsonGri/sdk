/// Incremental cursor over immutable history.
pub(crate) struct HistoryCursor {
    root: Option<Arc<History>>,
    windows: VecDeque<HistoryWindow>,
    next: u64,
}
impl HistoryCursor {
    fn check_anchor(&self, path: &PathState, anchor: CommitId) -> Result<(), StreamError> {
        if let Some(root) = &self.root
            && HistoryWalk::new(path.history.as_ref(), path.tail, 0)
                .any(|(node, _)| std::ptr::eq(node, root.as_ref()))
        {
            return Ok(());
        }
        let from = self
            .next
            .checked_sub(1)
            .ok_or(StreamError::InvalidArgument)?;
        let mut previous = Self::new(path, self.next, from);
        if previous
            .next_record()
            .is_none_or(|record| record.sequence != from || record.commit_id != anchor)
        {
            return Err(StreamError::PrefixNotRetained);
        }
        Ok(())
    }
    pub(crate) fn at(next: u64) -> Self {
        Self {
            root: None,
            windows: VecDeque::new(),
            next,
        }
    }
    fn new(path: &PathState, tail: u64, next: u64) -> Self {
        if let Some(window) = path.prefix_window(next, tail) {
            return Self {
                root: path.history.clone(),
                windows: VecDeque::from([window]),
                next,
            };
        }
        let mut cursor = Self::at(next);
        cursor.sync(path.history.as_ref(), tail);
        cursor
    }
    fn sync(&mut self, root: Option<&Arc<History>>, tail: u64) {
        if match (&self.root, root) {
            (Some(old), Some(root)) => Arc::ptr_eq(old, root),
            (None, None) => true,
            _ => false,
        } {
            return;
        }
        // The common follow update is one append above the retained root.
        // Reuse the cursor queue directly instead of allocating a scratch Vec.
        if let (Some(old), Some(current)) = (&self.root, root) {
            #[cfg(test)]
            HISTORY_VISITS.with(|count| count.set(count.get() + 1));
            if let History::Batch {
                parent: Some(parent),
                records,
                ..
            } = current.as_ref()
                && Arc::ptr_eq(old, parent)
            {
                if let Some(window) = history_window(Arc::clone(records), tail, self.next) {
                    self.windows.push_back(window);
                }
                self.root = root.cloned();
                return;
            }
        }
        let mut added = Vec::new();
        for (node, ceiling) in HistoryWalk::new(root, tail, self.next) {
            if self
                .root
                .as_ref()
                .is_some_and(|old| std::ptr::eq(old.as_ref(), node))
            {
                added.reverse();
                self.windows.extend(added);
                self.root = root.cloned();
                return;
            }
            if let History::Batch { records, .. } = node
                && let Some(window) = history_window(Arc::clone(records), ceiling, self.next)
            {
                added.push(window);
            }
        }
        added.reverse();
        self.windows = added.into();
        self.root = root.cloned();
    }
    fn next_record(&mut self) -> Option<Record> {
        let window = self.windows.front_mut()?;
        let record = window.records.records.get(window.start)?.clone();
        window.start += 1;
        self.next = record.sequence.checked_add(1)?;
        if window.start == window.end {
            self.windows.pop_front();
        }
        Some(record)
    }
}
