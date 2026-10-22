//! Test support for observing which processes hold this process's output.

use std::io;
use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Console::{GetStdHandle, STD_OUTPUT_HANDLE};

/// Closes this process's standard output handle, so a reader of it sees
/// the end once no other process holds it. Writes to standard output fail
/// afterwards; the caller must hold no borrow of the handle.
pub fn close_standard_output() -> io::Result<()> {
    // SAFETY: reads this process's standard output slot.
    let handle = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no standard output to close",
        ));
    }
    // SAFETY: the slot's open handle; the contract above leaves no borrow.
    if unsafe { CloseHandle(handle) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
