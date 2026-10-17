//! Test hooks that record the work one held directory listing performs.
//!
//! Events are `(operation, bytes or count, capacity)` triples recorded in
//! the active differential trace. `restoring_old_work` brings back the
//! previous behaviour, zero-filling the record buffer and examining every
//! macOS entry with `fstatat`, as the negative control.

use std::cell::Cell;

type Event = (&'static str, u64, u64);

thread_local! {
    static RESTORE: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn restoring_old_work<T>(run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            RESTORE.set(false);
        }
    }
    RESTORE.set(true);
    let _reset = Reset;
    run()
}

pub(crate) fn restoring_old_work_active() -> bool {
    RESTORE.get()
}

/// Records how much of a new record buffer is initialized before the
/// first refill.
pub(crate) fn storage(mut buffer: Vec<u64>) -> Vec<u64> {
    if RESTORE.get() {
        buffer.resize(buffer.capacity(), 0);
    }
    acyclic_test_support::differential::record::<Event>((
        "storage",
        (buffer.len() * 8) as u64,
        (buffer.capacity() * 8) as u64,
    ));
    buffer
}

/// Records one listed entry's facts coming from the listing (`bulk`) or a
/// separate `fstatat`.
#[cfg(target_os = "macos")]
pub(crate) fn examined(bulk: bool) {
    acyclic_test_support::differential::record::<Event>((
        if bulk { "bulk" } else { "fstatat" },
        1,
        0,
    ));
}
