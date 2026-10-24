use super::*;

#[test]
fn prepared_operation_keeps_the_original_file_alive_in_a_completion_worker() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    std::fs::write(temporary.path(), b"data")?;
    let file = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let payload = Arc::downgrade(&file.file);
    let health = Arc::downgrade(&file.health);
    let observed = Arc::new(ReadBudgetMeter::new(1, 4));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let prepared = file.prepare_operation(&meter)?;
    drop(file);
    assert!(
        payload.upgrade().is_some(),
        "prepared operation lost its original file"
    );
    assert!(
        health.upgrade().is_some(),
        "prepared operation lost its file health"
    );
    observed.cancelled.store(true, Ordering::Release);
    observed.deny_allocations.store(true, Ordering::Release);
    std::thread::spawn(move || {
        prepared.run(|access| {
            let mut bytes = [0; 4];
            assert_eq!(access.read_at(0, &mut bytes)?, 4);
            assert_eq!(&bytes, b"data");
            Ok(())
        })
    })
    .join()
    .map_err(|_| io::Error::from(io::ErrorKind::Other))??;
    assert!(
        payload.upgrade().is_none(),
        "completed operation retained its file"
    );
    assert!(
        health.upgrade().is_none(),
        "completed operation retained its file health"
    );
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn nested_prepared_operations_preserve_the_enclosing_file_order() -> io::Result<()> {
    let file = NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    let meter: Arc<dyn NativeReadMeter> = Arc::new(ReadBudgetMeter::new(1, 1));
    let outer = file.prepare_operation(&meter)?;
    let inner = file.prepare_operation(&meter)?;
    outer.run(|_| {
        let enclosing = file
            .health
            .tail
            .lock()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))?
            .clone()
            .ok_or(io::ErrorKind::Other)?;
        inner.run(|_| {
            assert!(
                file.health
                    .tail
                    .lock()
                    .map_err(|_| io::Error::from(io::ErrorKind::Other))?
                    .as_ref()
                    .is_some_and(|tail| Arc::ptr_eq(tail, &enclosing)),
                "nested operation replaced the enclosing fence"
            );
            Ok(())
        })
    })?;
    assert!(
        file.health
            .tail
            .lock()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))?
            .is_none()
    );
    Ok(())
}

#[test]
fn prepared_file_operation_owns_its_entire_fence_before_cancellation() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    std::fs::write(temporary.path(), b"data")?;
    let file = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let observed = Arc::new(ReadBudgetMeter::new(1000, 4000));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let mut prepared = None;
    let preparation = acyclic_test_support::measure(|| {
        prepared = Some(file.prepare_operation(&meter));
    });
    let prepared = prepared.ok_or(io::ErrorKind::Other)??;
    let held = observed.memory.load(Ordering::Acquire);
    assert_eq!(
        preparation.bytes_max as u64, held,
        "prepared fence omitted Arc counters"
    );
    assert!(
        file.health
            .tail
            .lock()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))?
            .is_none(),
        "preparation joined the file queue"
    );
    observed.cancelled.store(true, Ordering::Release);
    observed.deny_allocations.store(true, Ordering::Release);
    let mut result = Ok(());
    let allocation = acyclic_test_support::measure(|| {
        result = prepared.run(|access| {
            for _ in 0..1000 {
                let mut bytes = [0; 4];
                assert_eq!(access.read_at(0, &mut bytes)?, 4);
                assert_eq!(&bytes, b"data");
            }
            assert_eq!(observed.memory.load(Ordering::Acquire), held);
            Ok(())
        });
    });
    result?;
    assert_eq!(
        allocation.count_max, 0,
        "prepared operation allocated after issuance"
    );
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    assert!(
        file.health
            .tail
            .lock()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))?
            .is_none()
    );
    Ok(())
}

#[test]
fn unused_prepared_fences_never_block_aliases_and_errors_release_order() -> io::Result<()> {
    let file = NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    let meter: Arc<dyn NativeReadMeter> = Arc::new(ReadBudgetMeter::new(10, 10));
    let unused = file.prepare_operation(&meter)?;
    complete(file.set_len_async(3, None)?)?;
    drop(unused);
    let failed: io::Result<()> = file
        .prepare_operation(&meter)?
        .run(|_| Err(io::ErrorKind::PermissionDenied.into()));
    assert_eq!(
        failed.err().ok_or(io::ErrorKind::Other)?.kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(
        file.health
            .tail
            .lock()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))?
            .is_none()
    );
    complete(file.set_len_async(4, None)?)?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn prepared_uncertain_completion_pins_the_existing_owner_and_fences_aliases() -> io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    let file = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let alias = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let meter: Arc<dyn NativeReadMeter> = Arc::new(ReadBudgetMeter::new(10, 10));
    let prepared = file.prepare_operation(&meter)?;
    let mut result: io::Result<()> = Ok(());
    let uncertain = linux::uncertain_completion(io::ErrorKind::TimedOut.into());
    let allocation = acyclic_test_support::measure(|| {
        result = prepared.run(|_| Err(uncertain));
    });
    assert!(result.is_err());
    assert_eq!(
        allocation.count_max, 0,
        "uncertainty allocated a replacement file owner"
    );
    assert!(Arc::ptr_eq(
        file.health
            .poison_anchor
            .lock()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))?
            .as_ref()
            .ok_or(io::ErrorKind::Other)?,
        &file.file
    ));
    let mut called = false;
    assert!(
        alias
            .prepare_operation(&meter)?
            .run(|_| {
                called = true;
                Ok(())
            })
            .is_err()
    );
    assert!(!called);
    assert!(read_at(alias.as_file(), 0, &mut [0]).is_err());
    Ok(())
}
