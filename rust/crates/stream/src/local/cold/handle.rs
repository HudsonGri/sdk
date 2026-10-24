//! Immutable read cuts share an admitted handle instead of reopening or
//! duplicating it for each publication. Its funding follows the final owner.
use super::*;
use acyclic_native_runtime::ReadAllocation;

struct Handle {
    file: File,
    // The file dies before the original allocation custody.
    _allocation: Option<ReadAllocation>,
}

#[derive(Clone)]
pub(super) struct SharedFile(Arc<Handle>);

impl SharedFile {
    #[cfg(test)]
    pub(super) fn funded_bytes(&self) -> u64 {
        self.0._allocation.as_ref().map_or(0, ReadAllocation::bytes)
    }

    pub(super) fn new(
        file: File,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        let allocation = work
            .map(|work| {
                page::observe(
                    Some(work),
                    ReadAllocation::new(
                        &work.meter,
                        acyclic_native_runtime::arc_allocation_bytes::<Handle>(),
                    ),
                )
            })
            .transpose()?;
        Ok(Self(Arc::new(Handle {
            file,
            _allocation: allocation,
        })))
    }
}

impl std::ops::Deref for SharedFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.0.file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_meter::Meter;
    use std::sync::atomic::Ordering;

    #[test]
    fn shared_handles_have_one_original_allocation_until_the_last_cut_dies()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::NamedTempFile::new()?;
        let meter = Arc::new(Meter::local());
        let work = crate::memory::Work::new(meter.original())?;
        let baseline = meter.held.load(Ordering::SeqCst);
        let allocations = meter.allocations.load(Ordering::SeqCst);
        let handle = SharedFile::new(temporary.reopen()?, Some(&work))?;
        // Independent allocator observation includes the Arc's control block.
        let file = temporary.reopen()?;
        let mut control = None;
        let physical = acyclic_test_support::measure(|| {
            control = Some(Arc::new(Handle {
                file,
                _allocation: None,
            }));
        });
        let bytes = u64::try_from(physical.bytes_current)?;
        drop(control);
        assert_eq!(
            meter.held.load(Ordering::SeqCst) - baseline,
            bytes,
            "shared cold handle omitted its original physical allocation"
        );
        assert_eq!(physical.count_current, 1);
        assert_eq!(meter.allocations.load(Ordering::SeqCst) - allocations, 1);
        let mut cut = None;
        let cloned = acyclic_test_support::measure(|| cut = Some(handle.clone()));
        assert_eq!(
            cloned.count_max, 0,
            "capturing a shared handle allocated storage"
        );
        assert_eq!(cloned.bytes_max, 0);
        let cut = cut.ok_or("missing shared handle")?;
        assert!(Arc::ptr_eq(&handle.0, &cut.0));
        assert_eq!(meter.allocations.load(Ordering::SeqCst) - allocations, 1);
        assert_eq!(meter.read_operations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        drop(handle);
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline + bytes);
        drop(cut);
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
        drop(work);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok(())
    }

    #[test]
    fn a_refused_handle_keeps_its_original_denial_and_retains_no_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::NamedTempFile::new()?;
        let meter = Arc::new(Meter::local());
        let work = crate::memory::Work::new(meter.original())?;
        let baseline = meter.held.load(Ordering::SeqCst);
        meter.deny_allocation.store(true, Ordering::SeqCst);
        let result = SharedFile::new(temporary.reopen()?, Some(&work));
        assert!(
            result.is_err(),
            "shared handle bypassed allocation admission"
        );
        assert_eq!(meter.held.load(Ordering::SeqCst), baseline);
        let failure = work
            .finish::<(), crate::StreamError>(Err(crate::StreamError::Unavailable))
            .err()
            .ok_or("shared handle lost the original allocation denial")?;
        assert_eq!(
            failure
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<crate::test_meter::NativeDenial>())
                .map(|cause| cause.0),
            Some("allocation"),
            "shared handle replaced the original allocation denial"
        );
        drop(work);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok(())
    }
}
