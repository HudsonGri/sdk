use super::*;
use crate::sync::Mutex;
use crate::{WorkBudget, WorkCounters, WorkError};
use std::sync::Arc;

/// Cumulative work one invocation charged to a shared ledger.
///
/// A long-lived ledger keeps custody, retention and live allocations for a whole
/// session, but each invocation (one run, one resume, one effect) is checked against
/// the full original budget from zero. Live allocation stays a property of the shared
/// ledger: a window never forgets bytes that are still held.
#[derive(Debug)]
pub(crate) struct NativeWorkWindow {
    // Locked only under the original leaf, which serializes every window of the ledger.
    state: Mutex<WindowState>,
    // The handle's live bytes, claimed on the opening context outside every window.
    metadata: NativeAllocation,
}

/// Everything a window owns on its shared ledger.
#[derive(Debug, Default)]
struct WindowState {
    ledger: WindowLedger,
    /// The first runtime failure inside this window, not yet reported. Each window has
    /// its own, so a failure is reported only to the invocation that caused it.
    failure: Option<crate::DemandError>,
}

/// The only cumulative state a window's budget is derived from.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct WindowLedger {
    /// Work this window charged.
    consumed: WorkCounters,
    /// Reservations this window holds and has not yet started or refunded.
    held: WorkCounters,
}

#[cfg(test)]
thread_local! {
    static WINDOW_LEASES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static WINDOW_ADDITIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// An unbounded allowance is never consumed by a cumulative addition. All
// finite additions are exact, including a finite stored budget under its limit.
fn add_bounded(left: u64, right: u64, limit: u64) -> Result<u64, WorkError> {
    #[cfg(test)]
    WINDOW_ADDITIONS.with(|count| count.set(count.get() + 1));
    if limit == u64::MAX {
        Ok(left)
    } else {
        left.checked_add(right).ok_or(WorkError::Overflow)
    }
}

impl WindowLedger {
    /// The ledger budget that leaves this window exactly `limit - held - consumed`
    /// against the shared `work`, which already includes everything this window charged.
    /// An unbounded counter stays unbounded less exactly what this window holds, so a
    /// held reservation always has room to be refunded or completed.
    fn view(&self, limit: &WorkBudget, work: &WorkCounters) -> Result<WorkBudget, WorkError> {
        #[cfg(test)]
        crate::performance::CUMULATIVE_PASSES.with(|passes| passes.set(passes.get() + 1));
        let mut view = *limit;
        macro_rules! project {
            ($($field:ident),* $(,)?) => {$({
                // A counter with no external work and no held reservation
                // sees its original limit exactly, including the unbounded sentinel.
                if work.$field != self.consumed.$field || self.held.$field != 0 {
                    let external = work.$field.checked_sub(self.consumed.$field)
                        .ok_or(WorkError::Overflow)?;
                    let available = add_bounded(limit.$field, external, limit.$field)?;
                    view.$field = available.checked_sub(self.held.$field)
                        .ok_or(WorkError::Overflow)?;
                }
            })*};
        }
        crate::performance::additive_counters!(project);
        Ok(view)
    }
    /// This window after an operation moved the shared work and pending reservations,
    /// with the work it charged.
    fn settle(
        &self,
        work: (&WorkCounters, &WorkCounters),
        pending: (&WorkCounters, &WorkCounters),
        stored: &WorkBudget,
        limit: &WorkBudget,
    ) -> Result<(Self, WorkBudget), WorkError> {
        #[cfg(test)]
        crate::performance::CUMULATIVE_PASSES.with(|passes| passes.set(passes.get() + 1));
        let mut next = *self;
        let mut budget = *stored;
        // Settle each counter once. No intermediate whole-ledger sums or
        // differences escape this transition; a failure publishes none of it.
        macro_rules! settle {
            ($($field:ident),* $(,)?) => {$({
                // With no work or reservation change, this counter is the
                // identity transition even at the unbounded sentinel.
                if work.1.$field != work.0.$field
                    || pending.0.$field != 0 || pending.1.$field != 0
                {
                    let charged = work.1.$field.checked_sub(work.0.$field)
                        .ok_or(WorkError::Overflow)?;
                    next.consumed.$field =
                        add_bounded(self.consumed.$field, charged, self.consumed.$field)?;
                    let held = add_bounded(self.held.$field, pending.1.$field, self.held.$field)?;
                    next.held.$field = held.checked_sub(pending.0.$field)
                        .ok_or(WorkError::Overflow)?;
                    budget.$field = add_bounded(stored.$field, charged, limit.$field)?;
                }
            })*};
        }
        crate::performance::additive_counters!(settle);
        Ok((next, budget))
    }
}

impl NativeWorkWindow {
    /// The context this window was opened on, outside every window.
    fn base(&self) -> &NativeMaterializationWork {
        self.metadata.work()
    }
    /// The shared leaf seen from this window, under the views of the context it was
    /// opened on, so native I/O is charged exactly like a direct charge in the window.
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.base()
            .state(|state| state.within_window(Some(self), operation))
    }
    /// Whether `view` is the meter this window's opening context used: native I/O
    /// metered through it belongs to this window. A view opened inside the window
    /// already charges through it.
    pub(super) fn binds(&self, view: &Arc<dyn acyclic_native_runtime::NativeReadMeter>) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(view), Arc::as_ptr(&self.base().view_meter()))
    }
    pub(super) fn read_credit(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
    ) -> std::io::Result<NativeResourceCredit> {
        match (kind, self.base().metering_invocation()) {
            (acyclic_native_runtime::NativeOperationKind::Read, Some(invocation)) => {
                Ok(invocation.read_credit)
            }
            _ => crate::demand::DemandWork::byte_credit(kind),
        }
    }
    /// Allocations stay with the owner the opening view delegates them to.
    pub(super) fn allocation_owner(
        &self,
    ) -> Option<Arc<dyn acyclic_native_runtime::NativeReadMeter>> {
        self.base().view_meter().allocation_owner()
    }
    fn state(&self) -> crate::sync::MutexGuard<'_, WindowState> {
        #[cfg(test)]
        WINDOW_LEASES.with(|count| count.set(count.get() + 1));
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl NativeMaterializationWork {
    /// Opens a fresh accounting window on the same ledger.
    ///
    /// Cumulative counters charged through the returned context are checked against the
    /// original budget from zero, and neither the enclosing window nor the ledger's
    /// unwindowed work is charged for them. Custody, retention and live allocation stay on
    /// the shared ledger, so peak allocation is still checked across every window.
    pub fn invocation_window(&self) -> Result<Self, NativeTreeMaterializationError> {
        let bytes = const {
            match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<NativeWorkWindow>())
            {
                Ok(bytes) => bytes as u64,
                Err(_) => panic!("work window Arc layout overflow"),
            }
        };
        // The handle's live bytes stay on the shared ledger, outside every window; the
        // allocation that creates it is this invocation's own work, so no window ever
        // leaves a permanent charge on the session.
        let mut base = self.allocation_work();
        base.window = None;
        let metadata = base.claim(bytes, 0, WorkCounters::UNCHARGED)?;
        let mut work = self.allocation_work();
        work.window = Some(Arc::new(NativeWorkWindow {
            state: Mutex::new(WindowState::default()),
            metadata,
        }));
        drop(work.claim(0, 1, WorkCounters::UNCHARGED)?);
        Ok(work)
    }
}

impl NativeMaterializationWork {
    /// The same context outside any invocation window, for a ledger retained across
    /// invocations: work charged through it later is never attributed to this one.
    #[must_use]
    pub fn outside_window(&self) -> Self {
        let mut work = self.clone();
        work.window = None;
        work
    }
}

impl crate::demand::DemandWork {
    /// Runs `operation` with the budget seen from `window`.
    ///
    /// Inside a window every cumulative counter has exactly `limit - held - consumed`
    /// left, from that window's own state alone: work and reservations of other windows
    /// and of the unwindowed ledger are never part of its view. The shared ledger keeps
    /// only live allocation and peak. The unwindowed budget is raised by exactly what
    /// the window charged, so no other view counts it. Runtime failures recorded inside
    /// the window go to the window's own slot and are reported only through it.
    ///
    /// Arithmetic is exact. A view or settlement that cannot be represented is an
    /// accounting invariant failure, which fails the ledger closed; nothing is clamped.
    pub(crate) fn within_window<R>(
        &mut self,
        window: Option<&NativeWorkWindow>,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let Some(window) = window else {
            return operation(self);
        };
        // A window is entered only while the original leaf is held. Keep
        // its ledger leased through settlement too: there is no second lookup
        // or copied state that can outlive that one transition.
        struct Transition<'a, 'w> {
            state: &'a mut crate::demand::DemandWork,
            ledger: crate::sync::MutexGuard<'w, WindowState>,
            outside: Option<crate::DemandError>,
            stored: WorkBudget,
            work: WorkCounters,
            pending: WorkCounters,
        }
        impl Drop for Transition<'_, '_> {
            fn drop(&mut self) {
                let peak = self.state.budget.peak_allocation_bytes;
                let settled = self.ledger.ledger.settle(
                    (&self.work, &self.state.work),
                    (&self.pending, &self.state.pending_reservations()),
                    &self.stored,
                    &self.state.original_budget(),
                );
                self.state.budget = match settled {
                    Ok((settled, budget)) => {
                        self.ledger.ledger = settled;
                        budget
                    }
                    Err(error) => {
                        self.state.fail_window_invariant(error);
                        self.stored
                    }
                };
                self.state.budget.peak_allocation_bytes = peak;
                self.ledger.failure = self.state.replace_native_io_failure(self.outside.take());
            }
        }
        let stored = self.budget;
        let work = self.work;
        let pending = self.pending_reservations();
        let mut ledger = window.state();
        let outside = self.replace_native_io_failure(ledger.failure.take());
        self.budget = match ledger.ledger.view(&self.original_budget(), &work) {
            Ok(view) => view,
            Err(error) => {
                self.fail_window_invariant(error);
                work
            }
        };
        self.budget.peak_allocation_bytes = stored.peak_allocation_bytes;
        let transition = Transition {
            state: self,
            ledger,
            stored,
            work,
            pending,
            outside,
        };
        operation(transition.state)
    }
    fn fail_window_invariant(&mut self, error: WorkError) {
        self.record_native_io_failure(error.into());
        self.poison_native_original_work();
    }
}

#[cfg(test)]
mod tests;
