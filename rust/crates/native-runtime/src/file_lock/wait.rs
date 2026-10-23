//! Cancellable kernel waits with no periodic lock probes.
#![allow(
    unsafe_code,
    reason = "kernel lock readiness uses owned OS notification handles"
)]
use super::*;
use crate::{NativeOperationKind, NativeOperationReservation, NativeReadMeter, ReadAllocation};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::Wake;

#[cfg(target_vendor = "apple")]
mod apple;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;
#[cfg(target_vendor = "apple")]
use apple as platform;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(windows)]
use windows as platform;

type Credit = NativeOperationReservation<Arc<dyn NativeReadMeter>>;

/// A sticky, single-use cancellation notification for a bounded file-lock wait.
/// Poll the original cancellation future with this object's waker before waiting.
pub struct FileLockCancellation {
    signal: platform::Signal,
    cancelled: AtomicBool,
    notification: Mutex<Option<Credit>>,
    _allocation: Option<ReadAllocation>,
}

impl FileLockCancellation {
    /// Admits its complete shared control and notification before registration.
    pub fn new(meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<Arc<Self>> {
        let layout = std::alloc::Layout::new::<[usize; 2]>()
            .extend(std::alloc::Layout::new::<Self>())
            .map_err(io::Error::other)?
            .0
            .pad_to_align();
        let allocation = meter
            .map(|meter| ReadAllocation::new(meter, layout.size()))
            .transpose()?;
        let notification = credit(meter, NativeOperationKind::Write, 1)?;
        let signal = platform::Signal::new(meter)?;
        Ok(Arc::new(Self {
            signal,
            cancelled: AtomicBool::new(false),
            notification: Mutex::new(notification),
            _allocation: allocation,
        }))
    }

    fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut notification = self
            .notification
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // The accepted cancellation must wake the kernel even if recording
        // discovers a late error on the original request.
        match notification.as_mut() {
            Some(credit) => {
                let _ = credit.attempt(|| self.signal.notify());
            }
            None => {
                let _ = self.signal.notify();
            }
        }
    }
}

impl Wake for FileLockCancellation {
    fn wake(self: Arc<Self>) {
        self.cancel();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.cancel();
    }
}

pub(super) fn check(
    deadline: Instant,
    cancellation: Option<&FileLockCancellation>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    if cancellation.is_some_and(|state| state.cancelled.load(Ordering::Acquire)) {
        return Err(io::ErrorKind::Interrupted.into());
    }
    if let Some(meter) = meter {
        meter.check()?;
    }
    if Instant::now() >= deadline {
        return Err(io::ErrorKind::TimedOut.into());
    }
    Ok(())
}

fn credit(
    meter: Option<&Arc<dyn NativeReadMeter>>,
    kind: NativeOperationKind,
    count: u64,
) -> io::Result<Option<Credit>> {
    meter
        .map(|meter| NativeOperationReservation::new(Arc::clone(meter), kind, count))
        .transpose()
}

pub(super) fn rollback_credit(
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<Option<Credit>> {
    credit(meter, NativeOperationKind::Write, 1)
}

fn attempt<T>(
    meter: Option<&Arc<dyn NativeReadMeter>>,
    kind: NativeOperationKind,
    action: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    match credit(meter, kind, 1)? {
        Some(mut credit) => {
            let (result, recorded) = credit.attempt(action);
            result.and_then(|value| recorded.map(|()| value))
        }
        None => action(),
    }
}

pub(super) fn acquire(
    file: &File,
    mode: FileLockMode,
    deadline: Instant,
    cancellation: Option<&FileLockCancellation>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    match try_file(file, mode, meter) {
        Ok(()) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(error),
    }
    platform::acquire(file, mode, deadline, cancellation, meter)
}

fn try_file(
    file: &File,
    mode: FileLockMode,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    let mut rollback = rollback_credit(meter)?;
    match credit(meter, NativeOperationKind::Read, 1)? {
        Some(mut credit) => {
            let (result, recorded) = credit.attempt(|| FileLock::try_lock_file(file, mode));
            if let Err(error) = recorded {
                if result.is_ok() {
                    match rollback.as_mut() {
                        Some(credit) => {
                            let _ = credit.attempt(|| file.unlock());
                        }
                        None => {
                            let _ = file.unlock();
                        }
                    }
                }
                return Err(error);
            }
            result
        }
        None => FileLock::try_lock_file(file, mode),
    }
}

pub(super) fn release_cancelled(original: &File, rollback: &mut Option<Credit>) -> io::Result<()> {
    match rollback.as_mut() {
        Some(credit) => {
            let (result, recorded) = credit.attempt(|| original.unlock());
            result.and(recorded)
        }
        None => original.unlock(),
    }
}

#[cfg(test)]
thread_local! {
    static WAITING: std::cell::RefCell<Option<std::sync::mpsc::Sender<()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn observe_wait() {
    WAITING.with(|waiting| {
        if let Some(waiting) = waiting.borrow_mut().take() {
            let _ = waiting.send(());
        }
    });
}

#[cfg(test)]
mod tests;
