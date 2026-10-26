//! Shared, coalescing wake registration. Only running work owns a future.
use acyclic_stream::query_sync::SyncMutex;
use std::{collections::BTreeMap, sync::{Arc, Weak}, task::{Context, Poll, Wake, Waker}};

struct Entry<K> { key: K, phase: Phase }
enum Phase { Queued { previous: Option<u64>, next: Option<u64> }, Running { notified: bool }, Idle }
struct Index<K> {
    next: u64,
    entries: BTreeMap<u64, Entry<K>>,
    first: Option<u64>,
    last: Option<u64>,
    ready_count: usize,
    waiter: Option<Waker>,
}
impl<K> Default for Index<K> {
    fn default() -> Self { Self { next: 0, entries: BTreeMap::new(), first: None, last: None, ready_count: 0, waiter: None } }
}
impl<K> Index<K> {
    fn enqueue(&mut self, id: u64) -> Option<Waker> {
        let previous = self.last;
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.phase = Phase::Queued { previous, next: None };
        }
        if let Some(last) = previous {
            if let Some(Entry { phase: Phase::Queued { next, .. }, .. }) = self.entries.get_mut(&last) { *next = Some(id); }
        } else { self.first = Some(id); }
        self.last = Some(id);
        self.ready_count += 1;
        self.waiter.take()
    }
    fn unlink(&mut self, previous: Option<u64>, next: Option<u64>) {
        if let Some(id) = previous {
            if let Some(Entry { phase: Phase::Queued { next: link, .. }, .. }) = self.entries.get_mut(&id) { *link = next; }
        } else { self.first = next; }
        if let Some(id) = next {
            if let Some(Entry { phase: Phase::Queued { previous: link, .. }, .. }) = self.entries.get_mut(&id) { *link = previous; }
        } else { self.last = previous; }
        self.ready_count -= 1;
    }
}
impl<K: Clone> Index<K> {
    fn take(&mut self) -> Option<(u64, K)> {
        let id = self.first?;
        let next = match self.entries.get(&id)?.phase { Phase::Queued { next, .. } => next, Phase::Running { .. } | Phase::Idle => return None };
        self.unlink(None, next);
        let entry = self.entries.get_mut(&id)?;
        entry.phase = Phase::Running { notified: false };
        Some((id, entry.key.clone()))
    }
}
struct Core<K> { index: SyncMutex<Index<K>> }

/// One process-wide ready index. Idle entries own no task, future or timer.
pub struct ReadySet<K> { core: Arc<Core<K>> }
impl<K> Default for ReadySet<K> {
    fn default() -> Self { Self { core: Arc::new(Core { index: SyncMutex::new(Index::default()) }) } }
}

/// Exclusive lifetime of one subscription registration. Old wakers cannot revive it.
pub struct Registration<K> { core: Arc<Core<K>>, id: u64 }

/// Running work retains notifications until its final durable-tail recheck completes.
pub struct Work<K> { core: Arc<Core<K>>, id: u64, key: K }

impl<K: Clone> ReadySet<K> {
    /// Register before reading the durable cursor/tail. Initial replay is always ready.
    pub fn register(&self, key: K) -> Result<Registration<K>, super::Error> {
        let mut index = self.core.index.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = index.next.checked_add(1).ok_or(super::Error::Invalid("wake registration exhausted"))?;
        index.next = id;
        index.entries.insert(id, Entry { key, phase: Phase::Idle });
        let waiter = index.enqueue(id);
        drop(index);
        if let Some(waiter) = waiter { waiter.wake(); }
        Ok(Registration { core: Arc::clone(&self.core), id })
    }

    /// Take one coalesced actor; no dormant-population scan.
    pub fn take(&self) -> Option<Work<K>> {
        let mut index = self.core.index.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (id, key) = index.take()?;
        Some(Work { core: Arc::clone(&self.core), id, key })
    }

    /// One shared dispatcher waiter, installed under the same lock as the empty check.
    /// The runtime owns exactly one pending call, independent of actor population.
    pub fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Work<K>> {
        let mut index = self.core.index.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((id, key)) = index.take() {
            Poll::Ready(Work { core: Arc::clone(&self.core), id, key })
        } else {
            index.waiter = Some(context.waker().clone());
            Poll::Pending
        }
    }

    /// Exact retained registration and ready counts, independent of actor heaps.
    pub fn counts(&self) -> (usize, usize) {
        let index = self.core.index.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        (index.entries.len(), index.ready_count)
    }
}

impl<K> Core<K> {
    fn notify(&self, id: u64) {
        let mut index = self.index.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let waiter = match index.entries.get_mut(&id).map(|entry| &mut entry.phase) {
            Some(Phase::Running { notified }) => { *notified = true; None }
            Some(Phase::Idle) => {
                index.enqueue(id)
            }
            Some(Phase::Queued { .. }) | None => None
        };
        drop(index);
        if let Some(waiter) = waiter { waiter.wake(); }
    }

    fn finish(&self, id: u64) {
        let mut index = self.index.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let waiter = if let Some(entry) = index.entries.get_mut(&id) {
            let notified = matches!(entry.phase, Phase::Running { notified: true });
            entry.phase = Phase::Idle;
            if notified {
                index.enqueue(id)
            } else { None }
        } else { None };
        drop(index);
        if let Some(waiter) = waiter { waiter.wake(); }
    }
}

struct Notifier<K> { core: Weak<Core<K>>, id: u64 }
impl<K: Send + Sync + 'static> Wake for Notifier<K> {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) { if let Some(core) = self.core.upgrade() { core.notify(self.id); } }
}
impl<K: Send + Sync + 'static> Registration<K> {
    /// Lightweight callback for a shared Streams session; it owns no pending future.
    pub fn waker(&self) -> Waker { Waker::from(Arc::new(Notifier { core: Arc::downgrade(&self.core), id: self.id })) }
}
impl<K> Drop for Registration<K> {
    fn drop(&mut self) {
        let mut index = self.core.index.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(Entry { phase: Phase::Queued { previous, next }, .. }) = index.entries.remove(&self.id) { index.unlink(previous, next); }
    }
}
impl<K> Work<K> {
    /// Identity to replay using its durable cursor before parking this work.
    pub const fn key(&self) -> &K { &self.key }
}
impl<K> Drop for Work<K> {
    fn drop(&mut self) { self.core.finish(self.id); }
}

#[cfg(test)]
mod tests;
