use crate::sync::{Condvar, Mutex};
use std::collections::VecDeque;

/// One terminal lock. Never held across serialization, I/O, or another lock.
/// Both predicates and wakeups belong to this lock; models exercise these paths.
pub(super) struct Queue<T> {
    state: Mutex<State<T>>,
    changed: Condvar,
    capacity: usize,
}
struct State<T> {
    pending: VecDeque<T>,
    closed: bool,
    failed: bool,
    submitted: u64,
    committed: u64,
}
impl<T> Queue<T> {
    pub(super) fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            state: Mutex::new(State {
                pending: VecDeque::with_capacity(capacity),
                closed: false,
                failed: false,
                submitted: 0,
                committed: 0,
            }),
            changed: Condvar::new(),
            capacity,
        }
    }
    pub(super) fn push(&self, item: T) -> Result<(), super::AuditError> {
        self.push_inner(item, true)
    }
    #[cfg(all(test, acyclic_fs_loom))]
    pub(super) fn push_without_wakeup(&self, item: T) -> Result<(), super::AuditError> {
        self.push_inner(item, false)
    }
    fn push_inner(&self, item: T, notify: bool) -> Result<(), super::AuditError> {
        let mut state = self.state.lock().map_err(|_| super::AuditError)?;
        while state.submitted - state.committed == self.capacity as u64
            && !state.failed
            && !state.closed
        {
            state = self.changed.wait(state).map_err(|_| super::AuditError)?;
        }
        if state.failed || state.closed {
            return Err(super::AuditError);
        }
        state.submitted = state.submitted.checked_add(1).ok_or(super::AuditError)?;
        state.pending.push_back(item);
        if notify {
            self.changed.notify_all();
        }
        Ok(())
    }
    pub(super) fn take(
        &self,
        batch: &mut Vec<T>,
        maximum: usize,
    ) -> Result<bool, super::AuditError> {
        let mut state = self.state.lock().map_err(|_| super::AuditError)?;
        while state.pending.is_empty() && !state.closed && !state.failed {
            state = self.changed.wait(state).map_err(|_| super::AuditError)?;
        }
        if state.failed {
            return Err(super::AuditError);
        }
        let count = maximum.min(state.pending.len());
        batch.extend(state.pending.drain(..count));
        self.changed.notify_all();
        Ok(count != 0)
    }
    pub(super) fn committed(&self, count: usize) -> Result<(), super::AuditError> {
        let mut state = self.state.lock().map_err(|_| super::AuditError)?;
        let committed = state
            .committed
            .checked_add(count as u64)
            .ok_or(super::AuditError)?;
        assert!(committed <= state.submitted);
        state.committed = committed;
        self.changed.notify_all();
        Ok(())
    }
    pub(super) fn flush(&self) -> Result<(), super::AuditError> {
        let mut state = self.state.lock().map_err(|_| super::AuditError)?;
        let target = state.submitted;
        while state.committed < target && !state.failed {
            state = self.changed.wait(state).map_err(|_| super::AuditError)?;
        }
        if state.failed {
            Err(super::AuditError)
        } else {
            Ok(())
        }
    }
    pub(super) fn close(&self, failed: bool) {
        let (mut state, poisoned) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        state.closed = true;
        state.failed |= failed || poisoned;
        self.changed.notify_all();
    }
}
