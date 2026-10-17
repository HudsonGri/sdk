use crate::Line;

// An alternate derived presentation. The finalized prefix is synchronized only
// when needed, so successive open blocks never copy the document's history.
#[derive(Default)]
pub(crate) struct Spare {
    pub(crate) lines: Vec<Line>,
    valid: usize,
    active: bool,
}

impl Spare {
    pub(crate) fn finish(&mut self, lines: &[Line], stable: usize, changed: usize) {
        self.valid = self.valid.min(changed);
        #[cfg(test)]
        let rendering = crate::work::rendering(true);
        if self.active {
            self.prefix(lines, stable);
        }
        #[cfg(test)]
        crate::work::rendering(rendering);
    }

    fn prefix(&mut self, lines: &[Line], end: usize) {
        let start = self.valid.min(end);
        let existing = end.min(self.lines.len());
        if start < end {
            #[cfg(test)]
            {
                crate::work::copy((end - start) * std::mem::size_of::<Line>());
                if self.lines.capacity() < end {
                    crate::work::copy(self.lines.len() * std::mem::size_of::<Line>());
                }
            }
            self.lines[start..existing].clone_from_slice(&lines[start..existing]);
            self.lines.extend_from_slice(&lines[existing..end]);
            self.valid = end;
        }
    }

    fn synchronize(&mut self, lines: &[Line], end: usize) {
        let start = self.valid.min(end);
        #[cfg(test)]
        {
            crate::work::copy((end - start) * std::mem::size_of::<Line>());
            if self.lines.capacity() < end {
                crate::work::copy(start * std::mem::size_of::<Line>());
            }
        }
        self.lines.truncate(start);
        self.lines.extend_from_slice(&lines[start..end]);
        self.valid = end;
    }

    pub(crate) fn capture(&mut self, lines: &[Line]) {
        self.active = true;
        self.synchronize(lines, lines.len());
    }

    pub(crate) fn exchange(&mut self, lines: &mut Vec<Line>, first: usize) {
        // Synchronize only the finalized prefix; the alternate open block is
        // already rendered and must retain its previous tail for diffing.
        debug_assert!(first <= self.lines.len());
        self.prefix(lines, first);
        std::mem::swap(lines, &mut self.lines);
        self.valid = first;
    }
}

#[cfg(test)]
thread_local! { pub(crate) static REPLAY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
