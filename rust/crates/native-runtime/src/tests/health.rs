use super::*;

#[cfg(windows)]
#[test]
fn healthy_registry_does_not_retain_exclusive_handle() -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;

    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("exclusive-native-file");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .open(&path)?;
    let native = NativeFile::from_file(file, None).map_err(|(error, _)| error)?;
    drop(native);
    // No unrelated admission or registry sweep is required to release
    // the caller's exclusive Windows share lock.
    File::open(&path)?;
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn independently_opened_aliases_share_fencing_and_uncertainty() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("identity");
    let alias = temporary.path().join("hard-link");
    std::fs::write(&path, [])?;
    let first = bounded_write_file(&path, true)?;
    std::fs::hard_link(&path, &alias)?;
    let second = bounded_write_file(&alias, true)?;
    #[cfg(target_os = "linux")]
    FileCompletion::new(
        Arc::new(CompletionMutex::new(CompletionState::<()>::default())),
        Some(Arc::new(OperationFence::default())),
        Some(first.health.clone()),
        Some(first.file.clone()),
        obs::OperationSpan::new(tracing::Span::none()),
    )
    .finish(Err(linux::uncertain_completion(
        io::ErrorKind::TimedOut.into(),
    )));
    #[cfg(not(target_os = "linux"))]
    poison_file_health(&first.file, &first.health);
    drop(first);
    assert!(complete(second.set_len_async(1, None)?).is_err());
    drop(second);
    assert!(
        complete(read_batch_async(
            File::open(&path)?,
            vec![OwnedRead {
                offset: 0,
                length: 1
            }]
        ))
        .is_err()
    );
    let direct = OpenOptions::new().read(true).write(true).open(&alias)?;
    assert!(read_at(&direct, 0, &mut [0]).is_err());
    assert!(write_all_at(&direct, 0, b"x", None).is_err());
    assert!(sync_file(&direct, Durability::Full, None).is_err());
    assert!(sync_data(&direct, Durability::Full).is_err());
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    let independent =
        NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    complete(independent.set_len_async(1, None)?)?;
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn admitted_job_keeps_alias_registry_alive_after_owner_drop() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("outstanding");
    let alias = temporary.path().join("outstanding-link");
    std::fs::write(&path, [])?;
    let native = bounded_write_file(&path, true)?;
    std::fs::hard_link(&path, &alias)?;
    let (started_tx, started) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let mut pending = native.control_async(
        move |_, _| {
            started_tx.send(()).map_err(io::Error::other)?;
            released.recv().map_err(io::Error::other)?;
            Ok(())
        },
        None,
    )?;
    let waker = completion_waker();
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    started
        .recv_timeout(Duration::from_secs(5))
        .map_err(io::Error::other)?;
    drop(native);
    let alias = bounded_write_file(&alias, true)?;
    let mut resize = alias.set_len_async(1, None)?;
    assert!(
        Pin::new(&mut resize)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    release.send(()).map_err(io::Error::other)?;
    complete(pending)?;
    complete(resize)?;
    assert_eq!(std::fs::metadata(&path)?.len(), 1);
    Ok(())
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
thread_local! {
    static FILE_QUERIES: std::cell::Cell<Option<(usize, [&'static str; 16])>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(super) fn record_file_query(query: &'static str) {
    FILE_QUERIES.with(|slot| {
        if let Some((length, mut events)) = slot.get() {
            events[length] = query;
            slot.set(Some((length + 1, events)));
        }
    });
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[test]
fn metadata_import_matches_independent_old_calls_at_every_budget_boundary() {
    use acyclic_test_support::{differential, measure};
    use proptest::prelude::*;
    use std::io::Read as _;
    let check =
        |(body, limit, skip, deny, cancelled, reject): (Vec<u8>, u64, bool, bool, bool, bool)| {
            let paired = differential::pair::<_, _, &'static str>(
                || {
                    let temporary = tempfile::NamedTempFile::new().unwrap();
                    std::fs::write(temporary.path(), &body).unwrap();
                    let owner = NativeFile::from_file(temporary.reopen().unwrap(), None)
                        .map_err(|(error, _)| error)
                        .unwrap();
                    let meter = Arc::new(ReadBudgetMeter {
                        deny_allocations: AtomicBool::new(deny),
                        cancelled: AtomicBool::new(cancelled),
                        ..ReadBudgetMeter::new(limit, 0)
                    });
                    (temporary, owner, meter)
                },
                |(temporary, _owner, meter), old| {
                    let file = temporary.reopen().unwrap();
                    FILE_QUERIES.set(Some((0, [""; 16])));
                    let inspect = |_: &File, metadata: &std::fs::Metadata| {
                        record_file_query("inspect");
                        if reject {
                            return Err(io::ErrorKind::InvalidData.into());
                        }
                        Ok((!skip).then_some(metadata.len()))
                    };
                    let project = |error| {
                        record_file_query("project");
                        error
                    };
                    let mut result = None;
                    let heap = measure(|| {
                        result = Some(if old {
                            // Frozen call order: observe/inspect before the ordinary
                            // constructor, whose health lookup queries identity again.
                            match meter.admit_read(0) {
                                Err(error) => {
                                    drop(file);
                                    Err(project(error))
                                }
                                Ok(()) => {
                                    record_file_query("metadata");
                                    match file.metadata() {
                                        Err(error) => {
                                            drop(file);
                                            Err(project(error))
                                        }
                                        Ok(metadata) => match inspect(&file, &metadata) {
                                            Err(error) => Err(error),
                                            Ok(None) => Ok(None),
                                            Ok(Some(value)) => NativeFile::from_file(
                                                file,
                                                Some(NativeFileMeter::Owned(meter.clone())),
                                            )
                                            .map(|native| Some((native, value)))
                                            .map_err(|(error, file)| {
                                                drop(file);
                                                project(error)
                                            }),
                                        },
                                    }
                                }
                            }
                        } else {
                            NativeFile::from_file_with_metadata(
                                file,
                                Some(NativeFileMeter::Owned(meter.clone())),
                                inspect,
                                project,
                            )
                        });
                    });
                    let (length, events) = FILE_QUERIES.replace(None).unwrap();
                    for event in &events[..length] {
                        differential::record(*event);
                    }
                    let mut bytes = Vec::new();
                    let result = result
                        .unwrap()
                        .map(|prepared| {
                            prepared.map(|(native, length)| {
                                native
                                    .as_file()
                                    .try_clone()
                                    .unwrap()
                                    .read_to_end(&mut bytes)
                                    .unwrap();
                                length
                            })
                        })
                        .map_err(|error| error.kind());
                    (
                        result,
                        bytes,
                        meter.used_operations.load(Ordering::Acquire),
                        meter.used_allocations.load(Ordering::Acquire),
                        meter.memory.load(Ordering::Acquire),
                        heap.count_total,
                    )
                },
            );
            let [(old, old_trace), (new, trace)] = paired;
            let started = !cancelled && limit != 0;
            let imported = started && !reject && !skip && !deny;
            prop_assert_eq!(new.0.is_ok(), started && !reject && (skip || !deny));
            prop_assert_eq!(new.2, u64::from(started));
            prop_assert_eq!(new.3, u64::from(imported));
            prop_assert_eq!(new.4, 0);
            prop_assert_eq!(new.5, u64::from(imported));
            if imported {
                prop_assert_eq!(&new.1, &body);
                let oracle = |work, heap, events: &[&str]| {
                    work == 1 && heap == 1 && events == ["metadata", "inspect"]
                };
                prop_assert!(oracle(new.2, new.5, &trace));
                if limit >= 2 {
                    prop_assert_eq!(&old.0, &new.0);
                    prop_assert_eq!(&old.1, &new.1);
                    prop_assert_eq!(old.2, 2);
                    prop_assert_eq!(old.5, 1);
                    prop_assert_eq!(old_trace, ["metadata", "inspect", "identity"]);
                    prop_assert!(!oracle(old.2, old.5, &["metadata", "inspect", "identity"]));
                } else {
                    prop_assert_eq!(old.0, Err(io::ErrorKind::Other));
                }
            } else {
                prop_assert_eq!(old, new);
                prop_assert_eq!(old_trace, trace);
            }
            Ok(())
        };
    for limit in 0..=3 {
        for skip in [false, true] {
            for deny in [false, true] {
                check((vec![1, 2, 3], limit, skip, deny, false, false)).unwrap();
            }
        }
    }
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(64));
    runner
        .run(
            &(
                prop::collection::vec(any::<u8>(), 0..256),
                0u64..4,
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
            ),
            check,
        )
        .unwrap();
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[test]
fn metadata_import_closes_the_file_before_projecting_admission_failure() -> io::Result<()> {
    let _warm = NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    file.lock()?;
    let meter = Arc::new(ReadBudgetMeter {
        deny_allocations: AtomicBool::new(true),
        ..ReadBudgetMeter::new(u64::MAX, 0)
    });
    let projected = std::cell::Cell::new(false);
    let result = NativeFile::from_file_with_metadata(
        file,
        Some(NativeFileMeter::Owned(meter)),
        |_, _| Ok(Some(())),
        |error| {
            assert!(temporary.reopen().unwrap().try_lock().is_ok());
            projected.set(true);
            error
        },
    );
    assert!(result.is_err());
    assert!(projected.get());
    // Negative control: projecting while the input remains owned cannot take
    // its advisory lock through a separately opened descriptor.
    let held = temporary.reopen()?;
    held.lock()?;
    assert!(temporary.reopen()?.try_lock().is_err());
    Ok(())
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[test]
fn inspection_output_cannot_select_another_files_health() -> io::Result<()> {
    let first = tempfile::NamedTempFile::new()?;
    let second = tempfile::NamedTempFile::new()?;
    let a = NativeFile::from_file(first.reopen()?, None).map_err(|(error, _)| error)?;
    let b = NativeFile::from_file(second.reopen()?, None).map_err(|(error, _)| error)?;
    let (native, unrelated_metadata) = NativeFile::from_file_with_metadata(
        first.reopen()?,
        None,
        |_, _| Ok(Some(second.as_file().metadata()?)),
        std::convert::identity,
    )?
    .unwrap();
    assert!(Arc::ptr_eq(&native.health, &a.health));
    assert!(!Arc::ptr_eq(&native.health, &b.health));
    let wrong = crate::health::shared_file_health_from_metadata(&unrelated_metadata, None)?;
    assert!(!Arc::ptr_eq(&native.health, &wrong));
    poison_file_health(&a.file, &a.health);
    let (alias, ()) = NativeFile::from_file_with_metadata(
        first.reopen()?,
        None,
        |_, _| Ok(Some(())),
        std::convert::identity,
    )?
    .unwrap();
    assert!(Arc::ptr_eq(&alias.health, &a.health));
    assert!(alias.health.uncertain.load(Ordering::Acquire));
    Ok(())
}
