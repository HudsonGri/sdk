//! Immutable captures share admitted resources and carry only the reader ledger.
use super::*;

pub(in crate::local::cold) struct Captured {
    pub(in crate::local::cold) state: Root,
    cut: Cut,
    work: Option<Arc<crate::memory::Work>>,
}

impl Cut {
    pub(in crate::local::cold) fn reader(self, work: Option<Arc<crate::memory::Work>>) -> Captured {
        Captured {
            state: self.state,
            cut: self,
            work,
        }
    }
}

impl Store {
    fn cut(&self, state: Root) -> Cut {
        Cut {
            state,
            file: self.active.file.clone(),
            number: self.active.number,
            end: self.active.end,
            root: Arc::clone(&self.root),
        }
    }

    pub(in crate::local::cold) fn capture(
        &self,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<Captured, LocalStreamError> {
        Ok(self.cut(self.state()?).reader(work))
    }
}

impl Transaction<'_> {
    pub(in crate::local::cold) fn cut(&self) -> Cut {
        self.store.cut(self.state)
    }
}

impl PageRead for Captured {
    fn work(&self) -> Option<&Arc<crate::memory::Work>> {
        self.work.as_ref()
    }
    fn read(&mut self, reference: PageRef) -> Result<Page, LocalStreamError> {
        self.cut.read(reference, self.work.as_ref())
    }
}
