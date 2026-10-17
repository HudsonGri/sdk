//! Observe actual by-value accounting snapshots, including restored discarded reads.
use super::allocation::KernelAccounting;
use crate::WorkCounters;
use std::cell::Cell;

thread_local! {
    static STATE: Cell<Option<(u8, u64)>> = const { Cell::new(None) };
}

pub(crate) fn copied(value: &WorkCounters) {
    if let Some((restore, bytes)) = STATE.get() {
        STATE.set(Some((restore, bytes + std::mem::size_of_val(value) as u64)));
        std::hint::black_box(value);
    }
}

pub(crate) fn restore(accounting: &KernelAccounting<'_>, boundary: u8) {
    if STATE
        .get()
        .is_some_and(|(restore, _)| restore & boundary != 0)
    {
        std::hint::black_box(accounting.spent());
    }
}

fn measure<T>(restore: u8, run: impl FnOnce() -> T) -> (T, u64) {
    struct Reset(Option<(u8, u64)>);
    impl Drop for Reset {
        fn drop(&mut self) {
            STATE.set(self.0);
        }
    }
    let _reset = Reset(STATE.replace(Some((restore, 0))));
    let output = run();
    (output, STATE.get().expect("active snapshot measurement").1)
}

#[cfg(feature = "memory")]
#[path = "counter_snapshots/tests.rs"]
mod tests;
