//! Owned readiness descriptors and a sticky cancellation EOF.
use super::*;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

pub(super) struct Descriptor {
    pub(super) fd: Option<OwnedFd>,
    cleanup: Option<Credit>,
}

impl Descriptor {
    pub(super) fn vacant(meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<Self> {
        Ok(Self {
            fd: None,
            cleanup: credit(meter, NativeOperationKind::Write, 1)?,
        })
    }
    pub(super) fn open(
        meter: Option<&Arc<dyn NativeReadMeter>>,
        open: impl FnOnce() -> io::Result<OwnedFd>,
    ) -> io::Result<Self> {
        let mut descriptor = Self::vacant(meter)?;
        attempt(meter, NativeOperationKind::Read, || {
            descriptor.fd = Some(open()?);
            Ok(())
        })?;
        Ok(descriptor)
    }
}

impl AsRawFd for Descriptor {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_ref().map_or(-1, AsRawFd::as_raw_fd)
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        if self.fd.is_some() {
            match self.cleanup.as_mut() {
                Some(credit) => {
                    let _ = credit.attempt(|| drop(self.fd.take()));
                }
                None => drop(self.fd.take()),
            }
        }
    }
}

pub(super) struct Signal {
    reader: Descriptor,
    writer: Descriptor,
}

impl Signal {
    pub(super) fn new(meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<Self> {
        let mut signal = Self {
            reader: Descriptor::vacant(meter)?,
            writer: Descriptor::vacant(meter)?,
        };
        attempt(meter, NativeOperationKind::Read, || {
            let (reader, writer) = std::os::unix::net::UnixStream::pair()?;
            signal.reader.fd = Some(reader.into());
            signal.writer.fd = Some(writer.into());
            Ok(())
        })?;
        Ok(signal)
    }
    pub(super) fn raw(&self) -> RawFd {
        self.reader.as_raw_fd()
    }
    pub(super) fn notify(&self) -> io::Result<()> {
        // SAFETY: the owned socket remains live for this single sticky wake.
        if unsafe { libc::shutdown(self.writer.as_raw_fd(), libc::SHUT_WR) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

pub(super) fn wait(
    descriptor: RawFd,
    deadline: Instant,
    cancellation: Option<&FileLockCancellation>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    loop {
        check(deadline, cancellation, meter)?;
        let mut descriptors = [
            libc::pollfd {
                fd: descriptor,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: cancellation.map_or(-1, |state| state.signal.raw()),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let remaining = deadline.saturating_duration_since(Instant::now());
        let milliseconds =
            i32::try_from(remaining.as_nanos().div_ceil(1_000_000)).unwrap_or(i32::MAX);
        #[cfg(test)]
        observe_wait();
        let result = attempt(meter, NativeOperationKind::Read, || {
            // SAFETY: both retained descriptors and the writable array outlive poll.
            let result = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, milliseconds) };
            if result < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(result)
            }
        });
        match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
            Ok(0) => {}
            Ok(_) => {
                check(deadline, cancellation, meter)?;
                if descriptors
                    .iter()
                    .any(|entry| entry.revents & libc::POLLNVAL != 0)
                {
                    return Err(io::ErrorKind::InvalidInput.into());
                }
                if descriptors[0].revents != 0 {
                    return Ok(());
                }
            }
        }
    }
}
