//! Retain accepted overlapped lock I/O until terminal completion, even on cancel.
use super::*;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::{
    Foundation::*,
    Storage::FileSystem::*,
    System::{IO::*, Threading::*},
};

struct Handle {
    value: Option<OwnedHandle>,
    cleanup: Option<Credit>,
}

impl Handle {
    fn vacant(meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<Self> {
        Ok(Self {
            value: None,
            cleanup: credit(meter, NativeOperationKind::Write, 1)?,
        })
    }
    fn event(meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<Self> {
        let mut event = Self::vacant(meter)?;
        attempt(meter, NativeOperationKind::Read, || {
            // SAFETY: a new unnamed manual-reset event with no inherited handle.
            let raw = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
            if raw.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: CreateEventW transferred this fresh owned handle.
            event.value = Some(unsafe { OwnedHandle::from_raw_handle(raw) });
            Ok(())
        })?;
        Ok(event)
    }
    fn raw(&self) -> HANDLE {
        self.value
            .as_ref()
            .map_or(std::ptr::null_mut(), AsRawHandle::as_raw_handle)
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if self.value.is_some() {
            match self.cleanup.as_mut() {
                Some(credit) => {
                    let _ = credit.attempt(|| drop(self.value.take()));
                }
                None => drop(self.value.take()),
            }
        }
    }
}

pub(super) struct Signal(Handle);
impl Signal {
    pub(super) fn new(meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<Self> {
        Handle::event(meter).map(Self)
    }
    fn raw(&self) -> HANDLE {
        self.0.raw()
    }
    pub(super) fn notify(&self) -> io::Result<()> {
        // SAFETY: this owned manual-reset event remains live through the wake.
        if unsafe { SetEvent(self.raw()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

struct Held {
    handle: Handle,
    unlock: Option<Credit>,
    locked: bool,
}

impl Held {
    fn release(&mut self) -> io::Result<()> {
        if !std::mem::take(&mut self.locked) {
            return Ok(());
        }
        let release = || {
            // SAFETY: the retained file handle is borrowed only for this call.
            let file =
                std::mem::ManuallyDrop::new(unsafe { File::from_raw_handle(self.handle.raw()) });
            file.unlock()
        };
        match self.unlock.as_mut() {
            Some(credit) => {
                let (result, recorded) = credit.attempt(release);
                result.and(recorded)
            }
            None => release(),
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

struct Pending {
    held: Held,
    event: Handle,
    overlapped: std::pin::Pin<Box<OVERLAPPED>>,
    pending: bool,
    cleanup: Option<Credit>,
    _allocation: Option<ReadAllocation>,
}

impl Pending {
    fn terminal(&mut self, cancel: bool) -> io::Result<()> {
        if !self.pending {
            return Ok(());
        }
        if cancel {
            let request = || {
                // SAFETY: this exact accepted request and its file remain owned.
                unsafe { CancelIoEx(self.held.handle.raw(), &raw const *self.overlapped) };
            };
            match self.cleanup.as_mut() {
                Some(credit) => {
                    let _ = credit.attempt(request);
                }
                None => request(),
            }
        }
        let complete = || {
            let mut blocking = true;
            loop {
                let mut transferred = 0;
                // SAFETY: file, event and pinned OVERLAPPED remain live until
                // terminal retrieval; cancellation alone is never terminal.
                if unsafe {
                    GetOverlappedResult(
                        self.held.handle.raw(),
                        &raw const *self.overlapped,
                        &raw mut transferred,
                        i32::from(blocking),
                    )
                } != 0
                {
                    return Ok(());
                }
                let error = io::Error::last_os_error();
                if !matches!(error.raw_os_error(), Some(code)
                    if code == ERROR_IO_INCOMPLETE.cast_signed() || code == ERROR_IO_PENDING.cast_signed())
                {
                    return Err(error);
                }
                if blocking {
                    // Close a spurious-event race without clearing a completed
                    // request's only notification: recheck after resetting.
                    // SAFETY: this private owned event outlives the request.
                    unsafe { ResetEvent(self.event.raw()) };
                }
                blocking = !blocking;
            }
        };
        let result = match self.cleanup.as_mut() {
            Some(credit) => {
                let (result, _recorded) = credit.attempt(complete);
                result
            }
            None => complete(),
        };
        self.pending = false;
        self.held.locked = result.is_ok();
        result
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        let _ = self.terminal(true);
    }
}

pub(super) fn acquire(
    file: &File,
    mode: FileLockMode,
    deadline: Instant,
    cancellation: Option<&FileLockCancellation>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    loop {
        check(deadline, cancellation, meter)?;
        let mut ready = acquire_notification(file, mode, deadline, cancellation, meter)?;
        // The overlapped description supplies readiness. Release it before
        // acquiring through the original description used for caller I/O.
        ready.release()?;
        drop(ready);
        check(deadline, cancellation, meter)?;
        match try_file(file, mode, meter) {
            Ok(()) => return Ok(()),
            // Another real contender won the handoff. Wait for its release
            // in the kernel under the same deadline and original ledger.
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
    }
}

fn acquire_notification(
    file: &File,
    mode: FileLockMode,
    deadline: Instant,
    cancellation: Option<&FileLockCancellation>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<Held> {
    let mut handle = Handle::vacant(meter)?;
    attempt(meter, NativeOperationKind::Read, || {
        // SAFETY: reopen the pinned file object, never a pathname; the returned
        // file description alone owns this overlapped acquisition and release.
        let raw = unsafe {
            ReOpenFile(
                file.as_raw_handle(),
                GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_FLAG_OVERLAPPED,
            )
        };
        if raw == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: ReOpenFile transferred ownership of this fresh handle.
        handle.value = Some(unsafe { OwnedHandle::from_raw_handle(raw) });
        Ok(())
    })?;
    let event = Handle::event(meter)?;
    let allocation = meter
        .map(|meter| ReadAllocation::new(meter, size_of::<OVERLAPPED>()))
        .transpose()?;
    // SAFETY: zero initializes every OVERLAPPED integer, union and null pointer.
    let overlapped = Box::pin(unsafe { std::mem::zeroed() });
    let mut pending = Pending {
        held: Held {
            handle,
            unlock: credit(meter, NativeOperationKind::Write, 1)?,
            locked: false,
        },
        event,
        overlapped,
        pending: false,
        cleanup: credit(meter, NativeOperationKind::Write, 2)?,
        _allocation: allocation,
    };
    pending.overlapped.hEvent = pending.event.raw();
    let mut start = credit(meter, NativeOperationKind::Read, 1)?;
    let mut submit = || {
        // SAFETY: the file is overlapped; pinned request and event stay live
        // through every completion or cancellation and its terminal retrieval.
        let result = unsafe {
            LockFileEx(
                pending.held.handle.raw(),
                if mode == FileLockMode::Exclusive {
                    LOCKFILE_EXCLUSIVE_LOCK
                } else {
                    0
                },
                0,
                u32::MAX,
                u32::MAX,
                &raw mut *pending.overlapped,
            )
        };
        if result != 0 {
            pending.held.locked = true;
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_IO_PENDING.cast_signed()) {
            return Err(error);
        }
        pending.pending = true;
        Ok(())
    };
    match start.as_mut() {
        Some(credit) => {
            let (result, recorded) = credit.attempt(submit);
            result?;
            recorded?;
        }
        None => submit()?,
    }
    if pending.pending {
        let waited = wait(&pending, deadline, cancellation, meter);
        if let Err(error) = waited {
            let _ = pending.terminal(true);
            return Err(error);
        }
        pending.terminal(false)?;
    }
    check(deadline, cancellation, meter)?;
    // Moving only the now-terminal lock leaves no pointer registered in I/O.
    let empty = Held {
        handle: Handle {
            value: None,
            cleanup: None,
        },
        unlock: None,
        locked: false,
    };
    Ok(std::mem::replace(&mut pending.held, empty))
}

fn wait(
    pending: &Pending,
    deadline: Instant,
    cancellation: Option<&FileLockCancellation>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    check(deadline, cancellation, meter)?;
    let handles = [
        pending.event.raw(),
        cancellation.map_or(std::ptr::null_mut(), |state| state.signal.raw()),
    ];
    let count = if cancellation.is_some() { 2 } else { 1 };
    let milliseconds = u32::try_from(
        deadline
            .saturating_duration_since(Instant::now())
            .as_nanos()
            .div_ceil(1_000_000),
    )
    .unwrap_or(INFINITE - 1);
    #[cfg(test)]
    observe_wait();
    attempt(meter, NativeOperationKind::Read, || {
        // SAFETY: the fixed array's live handles are retained through the bounded wait.
        let result = unsafe { WaitForMultipleObjects(count, handles.as_ptr(), 0, milliseconds) };
        match result {
            WAIT_OBJECT_0 => Ok(()),
            WAIT_TIMEOUT => Err(io::ErrorKind::TimedOut.into()),
            value if value == WAIT_OBJECT_0 + 1 => Err(io::ErrorKind::Interrupted.into()),
            _ => Err(io::Error::last_os_error()),
        }
    })
}
