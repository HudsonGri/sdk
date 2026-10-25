use super::*;
use crate::native_mount::NativeMountRecoveryOwner;
use std::os::unix::fs::MetadataExt;

// Read the kernel's cached mount table, never stat or enumerate a potentially
// dead NFS root. getmntinfo's process-global buffer shares the probe's lock.
pub(super) fn mount_table() -> Result<Vec<libc::statfs>, NativeMountError> {
    let _table = MOUNT_TABLE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut mounts = ptr::null_mut();
    // SAFETY: libc returns count initialized records, copied before releasing MOUNT_TABLE.
    let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
    if count <= 0 || mounts.is_null() {
        return Err(std::io::Error::last_os_error().into());
    }
    let count = usize::try_from(count).map_err(|_| driver_errno(libc::EOVERFLOW))?;
    // SAFETY: the table remains valid under MOUNT_TABLE until it has been copied.
    Ok(unsafe { std::slice::from_raw_parts(mounts, count) }.to_vec())
}

pub(super) fn field(name: &[c_char]) -> Result<&CStr, NativeMountError> {
    // SAFETY: c_char and u8 have identical layout; the fixed field is borrowed.
    let bytes = unsafe { std::slice::from_raw_parts(name.as_ptr().cast(), name.len()) };
    CStr::from_bytes_until_nul(bytes).map_err(|_| driver_errno(libc::EINVAL))
}

fn is_loopback_mount(mount: &libc::statfs, uid: u32) -> Result<bool, NativeMountError> {
    let source = field(&mount.f_mntfromname)?.to_bytes();
    let socket_id = source
        .strip_prefix(b"</tmp/acyclic-fs.")
        .and_then(|source| source.strip_suffix(b"/nfs>:/"));
    Ok(mount.f_owner == uid
        && field(&mount.f_fstypename)?.to_bytes() == b"nfs"
        && socket_id.is_some_and(|id| !id.is_empty() && !id.contains(&b'/')))
}

// Bootstrap is deliberately outside a bounded callback invocation. The fence
// is the durable SDK ownership record and its kernel lock is the process
// liveness witness, including after SIGKILL and across PID reuse.
pub(in crate::native_mount) fn reclaim_stale_mounts() -> Result<(), NativeMountError> {
    acyclic_native_runtime::initialize_native_io_services()?;
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let work = Work::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| NativeMountError::from(error))?;
    let mounts = mount_table()?;
    let observed = mounts
        .iter()
        .map(|mount| {
            let destination = field(&mount.f_mntonname)?;
            Ok(super::super::recovery_order::ObservedMount {
                destination: Path::new(std::ffi::OsStr::from_bytes(destination.to_bytes())),
                native: mount,
            })
        })
        .collect::<Result<Vec<_>, NativeMountError>>()?;
    super::super::recovery_order::recover_mounts(&observed, |mount| {
        reclaim_mount(mount, uid, &work)
    })
    .map_err(NativeMountError::RecoveryBatchFailed)
}

fn reclaim_mount(mount: &libc::statfs, uid: u32, work: &Work) -> Result<bool, NativeMountError> {
    if !is_loopback_mount(mount, uid)? {
        return Ok(false);
    }
    let destination = field(&mount.f_mntonname)?;
    let path = Path::new(std::ffi::OsStr::from_bytes(destination.to_bytes()));
    let mut owner = match NativeMountRecoveryOwner::acquire_existing_canonical(path, work) {
        Ok(owner) => owner,
        Err(NativeMountError::DestinationBusy) => return Ok(false),
        Err(NativeMountError::Demand(crate::DemandError::Io(error)))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(false);
        }
        Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Io(
            error,
        ))) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    let authentication = owner.configure_control(|control, fence| {
        work.charge(READ_WORK)?;
        let metadata = fence.metadata()?;
        if !metadata.is_file() || metadata.uid() != uid || metadata.nlink() != 1 {
            return Ok(false);
        }
        control.bind_mount(mount, work)?;
        Ok(true)
    });
    let authenticated = match authentication {
        Ok(authenticated) => authenticated,
        Err(error) => {
            owner.mark_failed();
            return Err(NativeMountError::RecoveryFailed {
                error: Box::new(error),
                owner,
            });
        }
    };
    if !authenticated {
        return Ok(false);
    }
    // The original fence and kernel identity now belong to the same core
    // owner used by explicit recovery. Its driver adapter performs every
    // direct attempt, Disk Arbitration fallback, and physical retirement.
    if let Err(error) = owner.detach(work) {
        owner.mark_failed();
        return Err(NativeMountError::RecoveryFailed {
            error: Box::new(error),
            owner,
        });
    }
    Ok(true)
}
