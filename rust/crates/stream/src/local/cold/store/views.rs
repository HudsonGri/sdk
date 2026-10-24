//! Accepted cuts are inline watch values. Readers clone only already-funded
//! handles under the short watch borrow and do all I/O outside writer locks.
use super::*;
use crate::query_sync::watch;

#[derive(Clone)]
struct Published {
    accepted: Cut,
    durable: Root,
}

pub(super) struct Views(watch::Sender<Option<Published>>);

impl Views {
    pub(super) fn new() -> Self {
        Self(watch::channel(None).0)
    }

    pub(super) fn publish(&self, accepted: Cut, durable: Root) {
        self.0.send_replace(Some(Published { accepted, durable }));
    }

    pub(super) fn poison(&self) {
        self.0.send_replace(None);
    }

    pub(super) fn capture(
        &self,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<captured::Captured, LocalStreamError> {
        let cut = self
            .0
            .borrow()
            .as_ref()
            .ok_or(LocalStreamError::IncompleteFrame)?
            .accepted
            .clone();
        Ok(cut.reader(work))
    }

    pub(super) fn observe(&self) -> Observer {
        Observer(self.0.subscribe())
    }
}

pub(in crate::local::cold) struct Observer(watch::Receiver<Option<Published>>);

impl Observer {
    /// The returned cut owns its files and directory independently of both the
    /// watch borrow and the writer. Reading consumes exactly this watch cut;
    /// a later publication remains pending for `changed`.
    pub(in crate::local::cold) fn snapshot(&mut self) -> Result<(Cut, Root), LocalStreamError> {
        let published = self.0.borrow_and_update();
        let published = published
            .as_ref()
            .ok_or(LocalStreamError::IncompleteFrame)?;
        Ok((published.accepted.clone(), published.durable))
    }

    pub(in crate::local::cold) async fn changed(&mut self) -> Result<(), LocalStreamError> {
        self.0
            .changed()
            .await
            .map_err(|_| LocalStreamError::IncompleteFrame)?;
        self.snapshot()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
