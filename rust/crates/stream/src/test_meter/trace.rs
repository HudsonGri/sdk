use std::cell::RefCell;

pub(crate) type Event = (&'static str, u64, u64, u64);

// An observer for a single-thread block_on test, never shared with the meter's
// other callers. No lock or production synchronization path is introduced.
std::thread_local! {
    static EVENTS: RefCell<Option<Vec<Event>>> = const { RefCell::new(None) };
}

pub(crate) fn begin() {
    EVENTS.with(|events| assert!(events.replace(Some(Vec::new())).is_none()));
}

pub(crate) fn finish() -> Vec<Event> {
    EVENTS.with(|events| events.take().expect("trace was started"))
}

pub(super) fn record(name: &'static str, a: u64, b: u64, c: u64) {
    EVENTS.with(|events| {
        if let Some(events) = events.borrow_mut().as_mut() {
            events.push((name, a, b, c));
        }
    });
}
