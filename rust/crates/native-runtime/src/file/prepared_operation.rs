//! A completion owns its ordering fence before the external effect is issued.
use crate::*;

/// One preallocated file operation. Preparation does not join the file queue.
/// Dropping an unused operation releases its original allocation claim.
pub struct PreparedFileOperation {
    file: NativeFile,
    completion: Arc<OperationFence>,
}

/// Access valid only while a prepared operation owns its place in the file queue.
pub struct PreparedFileAccess<'a> {
    file: &'a NativeFile,
}

impl NativeFile {
    /// Funds a single ordered completion before issuing an external effect.
    pub fn prepare_operation(
        &self,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> io::Result<PreparedFileOperation> {
        meter.check()?;
        let bytes = arc_allocation_bytes::<OperationFence>();
        let allocation = ReadAllocation::new(meter, bytes)?;
        meter.admit_codec(0, bytes as u64, 0)?;
        let completion = Arc::new(OperationFence {
            health: Some(Arc::downgrade(&self.health)),
            _allocation: Some(allocation),
            ..OperationFence::default()
        });
        meter.record_codec(0, bytes as u64, 0)?;
        meter.check()?;
        Ok(PreparedFileOperation {
            file: NativeFile {
                file: Arc::clone(&self.file),
                health: self.health.clone(),
                #[cfg(windows)]
                overlapped: self.overlapped,
            },
            completion,
        })
    }
}

impl PreparedFileOperation {
    /// Acquires the file's existing operation order using only prepared storage.
    /// Cancellation admission belongs to preparation; physical I/O errors still
    /// propagate, and uncertain completion fences every alias of the file.
    pub fn run<T>(
        self,
        operation: impl FnOnce(&PreparedFileAccess<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        if super::scope::in_file_scope(&self.file.health) {
            return self.execute(operation);
        }
        let predecessor = self.completion.enqueue(&self.file.health);
        if let Some(predecessor) = predecessor {
            predecessor.wait_until_complete();
        }
        let result = with_file_scope(Some(&self.file.health), || self.execute(operation));
        self.completion.complete();
        result
    }

    fn execute<T>(
        &self,
        operation: impl FnOnce(&PreparedFileAccess<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        let access = PreparedFileAccess { file: &self.file };
        access.check()?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&access)))
            .unwrap_or_else(|_| std::process::abort());
        access.observe(result)
    }
}

impl PreparedFileAccess<'_> {
    fn check(&self) -> io::Result<()> {
        if self.file.health.uncertain.load(Ordering::Acquire) {
            return Err(io::ErrorKind::Other.into());
        }
        Ok(())
    }

    fn observe<T>(&self, result: io::Result<T>) -> io::Result<T> {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        if result.as_ref().err().is_some_and(is_uncertain_io_error) {
            poison_file_health(&self.file.file, &self.file.health);
        }
        result
    }

    /// Reads admitted initialized storage without allocating an inner fence.
    /// Callers retain responsibility for their preissued byte/operation credit.
    pub fn read_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
        self.check()?;
        #[cfg(target_vendor = "apple")]
        let result = read_at_impl(
            &self.file.file,
            offset,
            file::ReadDestination::Slice(destination),
        );
        #[cfg(not(target_vendor = "apple"))]
        let result = read_at_impl(&self.file.file, offset, destination);
        self.observe(result)
    }
}
