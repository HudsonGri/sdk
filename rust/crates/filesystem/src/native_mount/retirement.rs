use super::*;

/// Retires the original producers before releasing their owning references.
pub(super) fn retire_sources(
    source: &mut Option<Arc<dyn MountFilesystem>>,
    watch: &mut Option<lazy::NativeMountSourceWatchOwner>,
    work: &NativeSessionStopWork<'_>,
) -> Result<bool, NativeMountError> {
    let mut changed = false;
    if let Some(owner) = source.as_ref() {
        match work {
            NativeSessionStopWork::Meter(meter) => owner.shutdown_with_meter(meter),
            NativeSessionStopWork::AcceptedWait(wait) => owner.shutdown_with_cleanup_wait(wait),
            NativeSessionStopWork::AcceptedDrop => owner.shutdown_accepted_cleanup(),
        }?;
        // Failed or cancelled joins leave the original source owned.
        drop(source.take());
        changed = true;
    }
    if let Some(owner) = watch.as_mut() {
        let result = if owner.is_stopped() {
            Ok(false)
        } else {
            match work {
                NativeSessionStopWork::Meter(meter) => {
                    owner.stop(&mut adapter::NativeDemandCarrier::Native(meter))
                }
                NativeSessionStopWork::AcceptedWait(wait) => owner.stop_with_cleanup_wait(wait),
                NativeSessionStopWork::AcceptedDrop => owner.stop_accepted_cleanup(),
            }
        };
        if owner.is_stopped() {
            // Physical quiescence, including an error after its acknowledgement,
            // permits terminal data transfer without fresh native admission.
            drop(watch.take());
            changed = true;
        }
        result?;
    }
    Ok(changed)
}

/// Called after physical namespace retirement and producer quiescence.
pub(super) fn retire_destination(
    destination: &mut Option<MountDestinationGuard>,
    work: &NativeSessionStopWork<'_>,
) -> Result<bool, NativeMountError> {
    let Some(guard) = destination.as_mut() else {
        return Ok(false);
    };
    guard.remove_destination()?;
    if guard.birth_cleanup.is_some() {
        // The physical namespace has retired. Its accepted fence release owns
        // birth-paid credit even when the caller has stopped waiting.
        guard.release_cleanup(None)?;
    } else {
        match work {
            NativeSessionStopWork::Meter(meter) => guard.release(&mut &**meter),
            NativeSessionStopWork::AcceptedWait(wait) => guard.release_cleanup(Some(wait)),
            NativeSessionStopWork::AcceptedDrop => guard.release_cleanup(None),
        }?;
    }
    drop(destination.take());
    Ok(true)
}
