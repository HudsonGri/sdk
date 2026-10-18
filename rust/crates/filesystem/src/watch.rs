//! Bounded native change notifications for materialized checkouts.
#![allow(
    clippy::result_large_err,
    reason = "Watch admission failures must not allocate another error owner."
)]

use crate::NativeRootIdentity;
use crate::cancellation::{CancellationError, CancellationToken};
#[cfg(any(test, not(windows)))]
use crate::kernel::NamespacePathError;
use crate::kernel::{LogicalName, NamespacePath};
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
#[cfg(not(windows))]
use notify::RecursiveMode;
#[cfg(not(windows))]
use notify::Watcher;
#[cfg(any(test, not(windows)))]
use notify::event::{ModifyKind, RenameMode};
#[cfg(any(test, not(windows)))]
use notify::{Event, EventKind};
use std::mem::size_of;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;

include!("watch/types.rs");
include!("watch/hints.rs");
include!("watch/operations_1.rs");
include!("watch/operations_2.rs");
include!("watch/operations_3.rs");
include!("watch/events.rs");
include!("watch/errors.rs");

#[cfg(test)]
#[path = "tests/watch/mod.rs"]
mod tests;
