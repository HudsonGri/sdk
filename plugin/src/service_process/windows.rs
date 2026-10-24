//! Same-user Windows named-pipe connections.

use super::*;

/// Connects to the service's pipe, trying `attempts` times while none
/// exists. A pipe whose every instance is connected is busy only until the
/// service creates the next, so that wait counts no attempt.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "waiting for a busy pipe needs the WaitNamedPipeW Win32 call"
)]
pub(crate) async fn connect_windows_control_pipe(
    data: &Path,
    attempts: usize,
    deadline: tokio::time::Instant,
) -> Result<tokio::net::windows::named_pipe::NamedPipeClient, ControlRequestError> {
    use windows_sys::Win32::Foundation::ERROR_PIPE_BUSY;
    use windows_sys::Win32::System::Pipes::WaitNamedPipeW;

    let pipe = windows_control_pipe_path(data);
    let mut recovery = acyclic_stream::runtime::Backoff::connection_attempts(
        u32::try_from(attempts.saturating_sub(1)).unwrap_or(u32::MAX),
        20,
    );
    loop {
        let remaining = remaining_control_wait(deadline)?;
        let error = match tokio::net::windows::named_pipe::ClientOptions::new().open(&pipe) {
            // Only this user's own service may answer.
            Ok(client) => {
                return match windows_pipe_server_is_this_user(&client) {
                    Ok(true) => Ok(client),
                    Ok(false) => Err(ControlRequestError::Unavailable(
                        "Acyclic service pipe is served by another user".to_owned(),
                    )),
                    Err(error) => Err(ControlRequestError::Unavailable(format!(
                        "cannot identify the Acyclic service pipe's server: {error}"
                    ))),
                };
            }
            Err(error) => error,
        };
        if error.raw_os_error() == i32::try_from(ERROR_PIPE_BUSY).ok() {
            let name = pipe.encode_utf16().chain([0]).collect::<Vec<_>>();
            let wait = u32::try_from(remaining.as_millis())
                .unwrap_or(u32::MAX)
                .max(1);
            // A failed wait (the pipe vanished) shows in the next open.
            // SAFETY: `name` is a live NUL-terminated UTF-16 string that the
            // task owns for the whole call.
            let _ =
                tokio::task::spawn_blocking(move || unsafe { WaitNamedPipeW(name.as_ptr(), wait) })
                    .await;
            continue;
        }
        // Access failures cannot be repaired by reconnecting. Only a missing
        // endpoint is safely replayable before any request bytes are sent.
        let Some(delay) = recovery.next(error.kind() == io::ErrorKind::NotFound, None, false)
        else {
            return Err(ControlRequestError::Unavailable(format!(
                "Acyclic service is not running: {error}"
            )));
        };
        tokio::time::sleep(delay.min(remaining)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn missing_pipe_uses_the_shared_bounded_jitter_schedule() {
        let root = tempfile::tempdir().unwrap();
        for attempts in [1_usize, 3] {
            let started = tokio::time::Instant::now();
            let result = connect_windows_control_pipe(
                root.path(),
                attempts,
                started + std::time::Duration::from_secs(1),
            )
            .await;
            assert!(matches!(result, Err(ControlRequestError::Unavailable(_))));
            let elapsed = tokio::time::Instant::now() - started;
            if attempts == 1 {
                assert_eq!(elapsed, std::time::Duration::ZERO);
            } else {
                // Three real missing-pipe opens use the shared 10/20 ms
                // ceilings. Compare against the actual old two fixed sleeps
                // on this paused clock, including the timer's own granularity.
                let old_started = tokio::time::Instant::now();
                for _ in 0..2 {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                let old_elapsed = tokio::time::Instant::now() - old_started;
                assert!(elapsed >= std::time::Duration::from_millis(15));
                assert!(
                    elapsed < old_elapsed,
                    "fixed-delay adapter returned: {elapsed:?}"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ready_pipe_preserves_same_user_admission_without_backoff() {
        let root = tempfile::tempdir().unwrap();
        let server = tokio::net::windows::named_pipe::ServerOptions::new()
            .first_pipe_instance(true)
            .create(windows_control_pipe_path(root.path()))
            .unwrap();
        let started = tokio::time::Instant::now();
        let client = connect_windows_control_pipe(
            root.path(),
            3,
            started + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
        server.connect().await.unwrap();
        assert!(windows_pipe_server_is_this_user(&client).unwrap());
        assert_eq!(tokio::time::Instant::now(), started);
    }
}

/// Whether the process serving `client`'s pipe runs as this process's user.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "queries the pipe's server process and both processes' token users"
)]
pub(crate) fn windows_pipe_server_is_this_user(
    client: &tokio::net::windows::named_pipe::NamedPipeClient,
) -> io::Result<bool> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    let mut server = 0_u32;
    // SAFETY: the pipe handle is live for the call and `server` is writable.
    if unsafe { GetNamedPipeServerProcessId(client.as_raw_handle() as HANDLE, &mut server) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: opening a process by id has no preconditions; a null handle is
    // an error.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, server) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    let server_user = acyclic_native_runtime::private_file::process_user_id(process);
    // SAFETY: `process` was opened above and is closed once.
    unsafe { CloseHandle(process) };
    // SAFETY: the pseudo-handle of this process needs no closing.
    let own_user =
        acyclic_native_runtime::private_file::process_user_id(unsafe { GetCurrentProcess() })?;
    Ok(server_user? == own_user)
}

#[cfg(windows)]
pub(crate) fn windows_control_pipe_path(data: &Path) -> String {
    format!(
        r"\\.\pipe\acyclic-{}",
        short_hash(data.as_os_str().to_string_lossy().as_bytes())
    )
}
