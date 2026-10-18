//! Bounded native change notifications for materialized checkouts.
#![allow(
    clippy::result_large_err,
    reason = "Watch admission failures must not allocate another error owner."
)]

include!("watch/types.rs");
include!("watch/linux_delivery.rs");
include!("watch/windows_delivery.rs");
include!("watch/startup.rs");
include!("watch/directories.rs");
include!("watch/rescan.rs");
include!("watch/fence.rs");
include!("watch/poll.rs");
include!("watch/events.rs");

#[cfg(all(test, any(windows, target_os = "macos")))]
#[path = "tests/watch/demand_admission.rs"]
mod demand_admission_tests;

#[cfg(test)]
#[path = "tests/watch/mod.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/watch/deadline.rs"]
mod deadline_tests;
