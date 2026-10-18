use super::*;
use proptest::prelude::*;

// Frozen staging and final validation from 1e2bd9373.
#[cfg(target_os = "linux")]
pub(super) async fn baseline_stage<A: AsyncAuthorityStore, O: AsyncObjectStore>(
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
        baseline_finish(source_root, host_path, snapshot, &file, meter)?;
        let bytes = ranges.as_ref().map_or(0, vector_bytes);
        drop(ranges);
        drop(file);
        meter.clone().release(bytes).map_err(capture_demand_error)?;
        return Ok(staged);
    }
    let native = native_positional_file(file, meter)?;
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
        length: metadata.len(),
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
    baseline_finish(source_root, host_path, snapshot, native.as_file(), meter)?;
    let bytes = ranges.as_ref().map_or(0, vector_bytes);
    drop(ranges);
    drop(native);
    meter.clone().release(bytes).map_err(capture_demand_error)?;
    Ok(staged)
}

#[cfg(target_os = "linux")]
fn baseline_finish(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostStat,
    file: &std::fs::File,
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
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

#[test]
fn std_descriptor_metadata_preserves_native_capture_facts_without_allocating() {
    use std::os::unix::fs::PermissionsExt as _;
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(64));
    runner
        .run(
            &(0u64..131073, 0u32..0o1000, any::<bool>()),
            |(length, mode, linked)| {
                let source = tempfile::tempdir().unwrap();
                let path = source.path().join("file");
                let file = std::fs::File::create(&path).unwrap();
                file.set_len(length).unwrap();
                file.set_permissions(std::fs::Permissions::from_mode(mode))
                    .unwrap();
                if linked {
                    std::fs::hard_link(&path, source.path().join("alias")).unwrap();
                }
                let root = HostRoot::open(source.path(), &mut crate::native_host::ordinary_work())
                    .unwrap();
                let expected = root
                    .stat_file_metadata(&file, &mut crate::native_host::ordinary_work())
                    .unwrap();
                let observed = file.metadata().unwrap();
                let mut actual = None;
                let heap = acyclic_test_support::measure(|| {
                    actual = Some(HostStat::from_std_metadata(&observed))
                });
                let actual = actual.unwrap().unwrap();
                prop_assert_eq!(heap.count_total, 0);
                prop_assert_eq!(actual.identity(), expected.identity());
                prop_assert_eq!(actual.nlink(), expected.nlink());
                prop_assert_eq!(actual.file_type(), expected.file_type());
                prop_assert_eq!(actual.len(), expected.len());
                prop_assert_eq!(capture_metadata(&actual), capture_metadata(&expected));
                prop_assert!(ensure_same_host_node(&expected, &actual).is_ok());
                let mut wrong = actual.clone();
                wrong.raw.st_ino ^= 1;
                prop_assert!(ensure_same_host_node(&expected, &wrong).is_err());
                Ok(())
            },
        )
        .unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn missing_std_birth_time_stays_allocation_free() {
    let file = std::fs::File::open("/proc/self/stat").unwrap();
    let metadata = file.metadata().unwrap();
    assert_eq!(
        metadata.created().unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
    let mut result = None;
    let heap =
        acyclic_test_support::measure(|| result = Some(HostStat::from_std_metadata(&metadata)));
    assert!(result.unwrap().unwrap().birth_time().is_none());
    assert_eq!(heap.count_total, 0);
}

#[cfg(target_os = "linux")]
#[test]
fn staging_import_reuses_only_its_held_metadata_and_preserves_complete_output() {
    use crate::native_host::stat_probe::Probe;
    use acyclic_test_support::{differential, measure};
    use std::os::unix::fs::PermissionsExt as _;
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let check = |(body, sparse, cancelled, stale): (Vec<u8>, bool, bool, bool)| {
        let paired = differential::pair::<_, _, &'static str>(
            || {
                // Independent source inodes, stores, decoder caches, health owners,
                // ledgers and cancellation for the frozen and production paths.
                let source = tempfile::tempdir().unwrap();
                let path = source.path().join("file");
                let file = std::fs::File::create(&path).unwrap();
                if sparse {
                    file.set_len(65537).unwrap();
                } else {
                    std::fs::write(&path, &body).unwrap();
                }
                let root = HostRoot::open(source.path(), &mut crate::native_host::ordinary_work())
                    .unwrap();
                let setup = NativeMaterializationWork::new(
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .unwrap();
                let fs = crate::Fs::memory();
                let workspace = runtime
                    .block_on(fs.create_workspace(
                        "import-pair",
                        WorkBudget::UNBOUNDED,
                        setup.cancellation(),
                    ))
                    .unwrap()
                    .value;
                let checkout = runtime
                    .block_on(workspace.checkout(
                        crate::model::GenerationSelector::Head,
                        crate::model::CheckoutMode::tracking_transaction(),
                        &setup,
                        setup.cancellation(),
                    ))
                    .unwrap();
                let health = acyclic_native_runtime::NativeFile::from_file(
                    std::fs::File::open(&path).unwrap(),
                    None,
                )
                .map_err(|(error, _)| error)
                .unwrap();
                let snapshot = test_host_stat(&root, Path::new("file")).unwrap();
                let mut bytes = 0;
                // Warm the same immutable objects independently in each store.
                runtime
                    .block_on(baseline_stage(
                        &checkout,
                        &root,
                        &path_only(),
                        &snapshot,
                        8,
                        &setup,
                        &mut bytes,
                    ))
                    .unwrap();
                if stale {
                    file.set_permissions(std::fs::Permissions::from_mode(0o400))
                        .unwrap();
                }
                (source, root, snapshot, setup, fs, checkout, health)
            },
            |(_source, root, snapshot, _setup, fs, checkout, _health), old| {
                let cancellation = CancellationToken::new();
                let work =
                    NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
                let scope = CaptureWorkScope::new(&work).unwrap();
                if cancelled {
                    cancellation.cancel();
                }
                let before = work.receipt();
                let live = work.live_allocation_bytes();
                let probe = Probe::rescan(false);
                probe.include_import_queries();
                let mut staged = 17;
                let mut result = None;
                let path = path_only();
                let heap = {
                    let mut future = std::pin::pin!(async {
                        if old {
                            baseline_stage(
                                &checkout,
                                &root,
                                &path,
                                &snapshot,
                                8,
                                &scope.native,
                                &mut staged,
                            )
                            .await
                        } else {
                            stage_regular_body(
                                &checkout,
                                &root,
                                &path,
                                &snapshot,
                                8,
                                &scope.native,
                                &mut staged,
                            )
                            .await
                        }
                    });
                    measure(|| result = Some(runtime.block_on(future.as_mut())))
                };
                probe.publish();
                let health_items = probe.examinations();
                drop(probe);
                let result = result.unwrap().map_err(|error| format!("{error:?}"));
                let receipt = work.receipt();
                let live_after = work.live_allocation_bytes();
                let mut bytes = Vec::new();
                if let Ok(ranges) = &result {
                    for (_, content) in ranges {
                        let cancellation = CancellationToken::new();
                        let mut accounting = crate::kernel::KernelAccounting::standalone(
                            WorkBudget::UNBOUNDED,
                            &cancellation,
                        );
                        let read = runtime
                            .block_on(crate::kernel::read_blob_range_with_accounting(
                                fs.objects(),
                                content.root(),
                                crate::ByteRange {
                                    offset: 0,
                                    length: content.logical_bytes(),
                                },
                                crate::kernel::DecodeLimits::for_volume(checkout.volume_config()),
                                &mut accounting,
                                &cancellation,
                                true,
                            ))
                            .unwrap();
                        bytes.extend_from_slice(read.bytes.as_ref());
                    }
                }
                let mut delta = serde_json::to_value(receipt).unwrap();
                let initial = serde_json::to_value(before).unwrap();
                for (name, value) in delta.as_object_mut().unwrap() {
                    *value = serde_json::json!(
                        value.as_u64().unwrap() - initial[name].as_u64().unwrap()
                    );
                }
                delta["itemsExamined"] =
                    serde_json::json!(delta["itemsExamined"].as_u64().unwrap() - health_items);
                (
                    result,
                    staged,
                    bytes,
                    delta,
                    heap.count_total,
                    live_after - live,
                )
            },
        );
        let [(mut old, old_trace), (new, trace)] = paired;
        prop_assert_eq!(&old.0, &new.0);
        prop_assert_eq!(old.1, new.1);
        prop_assert_eq!(&old.2, &new.2);
        prop_assert_eq!(old.4, new.4);
        prop_assert_eq!(old.5, new.5);
        let imported = !cancelled && !stale && !sparse && !body.is_empty();
        if imported {
            prop_assert_eq!(&new.2, &body);
            old.3["backendReadOperations"] =
                serde_json::json!(old.3["backendReadOperations"].as_u64().unwrap() - 1);
        }
        prop_assert_eq!(old.3, new.3);
        let mut expected = old_trace.clone();
        if !cancelled {
            let first = expected.iter().position(|event| *event == "statx").unwrap();
            expected[first] = "metadata";
        }
        expected.retain(|event| *event != "identity");
        prop_assert_eq!(&trace, &expected);
        let oracle = |events: &[&str]| {
            events == expected
                && events.iter().filter(|event| **event == "metadata").count()
                    == usize::from(!cancelled)
        };
        prop_assert!(oracle(&trace));
        if !cancelled {
            // The old duplicate identity query and initial descriptor query fail
            // the same ordered production-trace oracle.
            prop_assert!(!oracle(&old_trace));
            let mut missing = trace.clone();
            missing.pop();
            prop_assert!(!oracle(&missing));
        }
        Ok(())
    };
    for (body, sparse) in [(vec![], false), (vec![0, 255, 42], false), (vec![], true)] {
        for cancelled in [false, true] {
            for stale in [false, true] {
                check((body.clone(), sparse, cancelled, stale)).unwrap();
            }
        }
    }
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(
            &(
                prop::collection::vec(any::<u8>(), 0..96),
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
            ),
            check,
        )
        .unwrap();
}

#[cfg(target_os = "linux")]
fn path_only() -> CaptureHostPath {
    test_host_path(Path::new("file"))
}
