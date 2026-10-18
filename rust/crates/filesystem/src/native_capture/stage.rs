use super::*;
use crate::kernel::allocation::KernelOwnedAllocation;
use acyclic_native_runtime::ReadAllocationReservation;

type NativeSourceStorage = (Vec<u8>, (KernelOwnedAllocation, ReadAllocationReservation));

pub(super) async fn stage_regular_body<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostStat,
    maximum_extent_spans: u32,
    meter: &NativeMaterializationWork,
    staged_file_bytes: &mut u64,
) -> Result<Vec<(HostDataRange, StagedContent)>, CaptureError> {
    meter.cancellation().check()?;
    #[cfg(unix)]
    let file = source_root
        .open_file(host_path, &mut meter.clone())
        .map_err(capture_demand_error)?;
    #[cfg(windows)]
    let file = source_root
        .open_file(
            host_path,
            windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS(0),
            &mut meter.clone(),
        )
        .map_err(capture_demand_error)?;
    let file = file.into_std();
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let (native, data_ranges) = match prepare_capture_file(
        file,
        source_root,
        host_path,
        snapshot,
        maximum_extent_spans,
        meter,
    )? {
        Some(prepared) => prepared,
        None => return Ok(Vec::new()),
    };
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let ranges = Some(data_ranges);
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let (native, metadata, ranges) = {
        let metadata = source_root
            .stat_file_metadata(&file, &mut meter.clone())
            .map_err(capture_demand_error)?;
        if metadata.file_type() != crate::demand::SourceNodeKind::RegularFile {
            return Err(CaptureError::from(crate::FsError::SourceSnapshotStale));
        }
        ensure_same_host_node(snapshot, &metadata)?;
        #[cfg(unix)]
        let ranges = allocated_data_ranges(
            &file,
            0,
            metadata.len(),
            maximum_extent_spans,
            &mut meter.clone(),
        )
        .map(Some)
        .map_err(capture_demand_error)?;
        #[cfg(windows)]
        let ranges = regular_data_ranges(&file, &metadata, maximum_extent_spans, meter)?;
        let skip_import = ranges.as_ref().is_some_and(Vec::is_empty);
        #[cfg(all(test, feature = "memory"))]
        let skip_import = super::empty_stage_tests::after_ranges(skip_import);
        if skip_import {
            let staged = capture_vec(0, meter)?;
            finish_regular_capture(source_root, host_path, snapshot, &file, meter)?;
            let bytes = ranges.as_ref().map_or(0, vector_bytes);
            drop(ranges);
            drop(file);
            meter.clone().release(bytes).map_err(capture_demand_error)?;
            return Ok(staged);
        }
        let native = native_positional_file(file, meter)?;
        (native, metadata, ranges)
    };
    #[cfg(all(test, feature = "memory", any(windows, target_os = "linux")))]
    let native = super::owner_allocation_tests::owner(native, meter);
    #[cfg(windows)]
    let ranges = match ranges {
        Some(ranges) => Some(
            settled_sparse_ranges(&native, &metadata, ranges, maximum_extent_spans, meter).await?,
        ),
        None => None,
    };
    let full = [HostDataRange {
        offset: 0,
        length: snapshot.len(),
    }];
    let data_ranges = ranges.as_deref().unwrap_or(&full);
    let mut staged = capture_vec(data_ranges.len(), meter)?;
    for range in data_ranges {
        let mut bounded = acyclic_native_runtime::AsyncRangeReader::new(
            std::borrow::Borrow::<acyclic_native_runtime::NativeFile>::borrow(&native),
            range.offset,
            range.length,
        );
        let mut accounting = crate::kernel::KernelAccounting::native(meter, meter.cancellation());
        let content = stager
            .stage_content_with_accounting(
                &mut bounded,
                range.length,
                &mut accounting,
                meter.cancellation(),
            )
            .await;
        drop(bounded);

        let content = content?;
        if content.logical_bytes() != range.length {
            return Err(CaptureError::from(crate::FsError::SourceSnapshotStale));
        }
        *staged_file_bytes = staged_file_bytes
            .checked_add(range.length)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        staged.push((*range, content));
    }
    finish_regular_capture(source_root, host_path, snapshot, native.as_file(), meter)?;
    let bytes = ranges.as_ref().map_or(0, vector_bytes);
    drop(ranges);
    drop(native);
    meter.clone().release(bytes).map_err(capture_demand_error)?;
    Ok(staged)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn prepare_capture_file(
    file: std::fs::File,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostStat,
    maximum_extent_spans: u32,
    original: &NativeMaterializationWork,
) -> Result<Option<(acyclic_native_runtime::NativeFile, Vec<HostDataRange>)>, CaptureError> {
    let mut empty_bytes = 0;
    let result = acyclic_native_runtime::NativeFile::from_file_with_metadata(
        file,
        Some(acyclic_native_runtime::NativeFileMeter::Owned(
            original.runtime_meter(),
        )),
        |file, metadata| {
            let observed = HostStat::from_std_metadata(metadata)?;
            if observed.file_type() != crate::demand::SourceNodeKind::RegularFile {
                return Err(CaptureError::from(crate::FsError::SourceSnapshotStale));
            }
            ensure_same_host_node(snapshot, &observed)?;
            let ranges = allocated_data_ranges(
                file,
                0,
                observed.len(),
                maximum_extent_spans,
                &mut original.clone(),
            )
            .map_err(capture_demand_error)?;
            let skip_import = ranges.is_empty();
            #[cfg(all(test, feature = "memory"))]
            let skip_import = super::empty_stage_tests::after_ranges(skip_import);
            if skip_import {
                let _staged = capture_vec::<(HostDataRange, StagedContent)>(0, original)?;
                finish_regular_capture(source_root, host_path, snapshot, file, original)?;
                empty_bytes = vector_bytes(&ranges);
                drop(ranges);
                return Ok(None);
            }
            Ok(Some(ranges))
        },
        |error| capture_native_file_error(original, error),
    )?;
    if result.is_none() {
        original
            .clone()
            .release(empty_bytes)
            .map_err(capture_demand_error)?;
    }
    Ok(result)
}

pub(super) fn finish_regular_capture(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostStat,
    file: &std::fs::File,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    #[cfg(all(test, feature = "memory"))]
    if super::empty_stage_tests::omit_finish() {
        return Ok(());
    }
    meter.cancellation().check()?;
    let after = source_root
        .stat_file_metadata(file, &mut meter.clone())
        .map_err(capture_demand_error)?;
    ensure_same_host_node(snapshot, &after)?;
    let current = source_root
        .stat_source_metadata(host_path, &mut meter.clone())
        .map_err(capture_demand_error)?;
    ensure_same_host_node(snapshot, &current)?;
    meter.clone().check().map_err(capture_demand_error)
}

#[cfg(any(test, not(any(target_os = "linux", target_os = "macos"))))]
#[allow(
    unsafe_code,
    reason = "the measured host opener transfers its fresh positional handle"
)]
pub(super) fn native_positional_file(
    file: std::fs::File,
    original: &crate::NativeMaterializationWork,
) -> Result<acyclic_native_runtime::NativeFile, CaptureError> {
    let meter = original.runtime_meter();
    #[cfg(windows)]
    let result = unsafe {
        // SAFETY: the fresh positional handle is overlapped and has no prior completion port.
        acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(
            file,
            Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)),
        )
    };
    #[cfg(not(windows))]
    let result = acyclic_native_runtime::NativeFile::from_file(
        file,
        Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)),
    );
    result.map_err(|(error, file)| {
        // This is an ordinary std file, not an explicit SDK close operation. Retire its physical handle before projecting the original failure.
        drop(file);
        capture_native_file_error(original, error)
    })
}

fn capture_native_file_error(
    original: &NativeMaterializationWork,
    error: std::io::Error,
) -> CaptureError {
    capture_demand_error(
        original
            .state
            .take_native_io_failure()
            .unwrap_or_else(|| native_demand_error(error)),
    )
}

impl<F: std::borrow::Borrow<acyclic_native_runtime::NativeFile>> crate::kernel::AsyncBlobSource
    for acyclic_native_runtime::AsyncRangeReader<F>
{
    async fn read_owned_with_native_work(
        &mut self,
        maximum: usize,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, crate::FsError> {
        cancellation.check()?;
        original.admit(WorkCounters::UNCHARGED)?;
        let maximum = maximum.min(usize::try_from(self.remaining()).unwrap_or(usize::MAX));
        if maximum == 0 {
            return Ok(Some(crate::kernel::OwnedSourceChunk::intrinsic(
                acyclic_stream::OwnedBytes::new(),
                0,
            )));
        }
        let storage = native_source_storage(maximum, original, cancellation)?;
        self.read_spare(storage, maximum, original.runtime_meter(), true, |result| {
            accept_native_source_chunk(result, maximum, original, cancellation)
        })
        .await
    }

    async fn read<'a>(
        &'a mut self,
        _destination: &'a mut [u8],
        _budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        cancellation
            .check()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Interrupted))?;
        // This internal producer requires the original native invocation, rather than constructing another counter/budget bank for a caller.
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }

    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, crate::FsError> {
        cancellation.check()?;
        original.admit(WorkCounters::UNCHARGED)?;
        let reader = self;
        let maximum = destination
            .len()
            .min(usize::try_from(reader.remaining()).unwrap_or(usize::MAX));
        if maximum == 0 {
            return Ok(0);
        }
        original
            .admit(crate::materializer::NativeResourceCredit::ReadBytes.delta(maximum as u64))?;
        let copy = original
            .reserve_work(crate::materializer::NativeResourceCredit::Copy.delta(maximum as u64))?;
        let runtime = original.runtime_meter();
        let result = reader
            .read(maximum, runtime, true)
            .await
            .map_err(|error| native_source_read_error(error, original))?;
        if result.0.len() != maximum {
            return Err(crate::FsError::SourceSnapshotStale);
        }
        destination
            .get_mut(..maximum)
            .ok_or(WorkError::Overflow)?
            .copy_from_slice(&result.0);
        copy.record_completed(
            crate::materializer::NativeResourceCredit::Copy.delta(maximum as u64),
        )?;
        // The packet retains its buffer until the copy and late checks finish.
        drop(result);
        cancellation.check()?;
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(maximum)
    }
}

pub(super) fn native_source_storage(
    maximum: usize,
    original: &NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<NativeSourceStorage, crate::FsError> {
    original.admit(crate::materializer::NativeResourceCredit::ReadBytes.delta(maximum as u64))?;
    let mut accounting = crate::kernel::KernelAccounting::native(original, cancellation);
    let storage = crate::kernel::Owned::<Vec<u8>>::allocate(maximum, &mut accounting)?;
    let reserved = acyclic_stream::OwnedBytes::reserve_vec_guard::<KernelOwnedAllocation>(
        &original.runtime_meter(),
    )
    .map_err(|error| crate::FsError::from(original.runtime_error(error)))?;
    Ok((storage.value, (storage.allocation, reserved)))
}

pub(super) fn accept_native_source_chunk(
    result: std::io::Result<(NativeSourceStorage, usize)>,
    maximum: usize,
    original: &NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<Option<crate::kernel::OwnedSourceChunk>, crate::FsError> {
    let (storage, filled) = result.map_err(|error| native_source_read_error(error, original))?;
    if filled != maximum {
        return Err(crate::FsError::SourceSnapshotStale);
    }
    if storage.0.len() != filled {
        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
    }
    let capacity = storage.0.capacity();
    cancellation.check()?;
    let bytes = acyclic_stream::OwnedBytes::from_vec_with_reserved_guard(
        (storage.0, storage.1.0),
        storage.1.1,
    )
    .map_err(|failure| match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
            crate::FsError::from(original.runtime_error(error))
        }
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
            crate::NativeTreeMaterializationError::InvalidLayout.into()
        }
    })?;
    cancellation.check()?;
    original.admit(WorkCounters::UNCHARGED)?;
    Ok(Some(crate::kernel::OwnedSourceChunk::intrinsic(
        bytes,
        capacity as u64,
    )))
}

fn native_source_read_error(
    error: std::io::Error,
    original: &NativeMaterializationWork,
) -> crate::FsError {
    original
        .state
        .take_native_io_failure()
        .map(crate::materializer::native_materialization_work_error)
        .map_or_else(
            || {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    crate::FsError::SourceSnapshotStale
                } else {
                    crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Io(error))
                }
            },
            crate::FsError::NativeKernel,
        )
}
