use super::*;

struct Fixture {
    packet: crate::native_mount::startup::NativeMountRetirement,
    work: NativeMaterializationWork,
    cancellation: crate::CancellationToken,
    before: crate::WorkCounters,
    live: u64,
    destination: PathBuf,
    fence: PathBuf,
    registry: PathBuf,
    _root: tempfile::TempDir,
}

impl Fixture {
    fn new(shared: bool) -> Result<Self, Box<dyn std::error::Error>> {
        registry::initialize()?;
        let root = tempfile::tempdir()?;
        let destination = root.path().join("owned-mount");
        std::fs::create_dir(&destination)?;
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(
            crate::WorkBudget {
                backend_write_operations: 128,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let mut guard = None;
        MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)?;
        // Startup has released the user-wide registry. Give this retirement
        // fixture its own real registry descriptor, so an independent probe
        // can distinguish its leaked lock from another fixture's valid lock.
        // All retirement, refill, unlink and unlock operations stay unchanged.
        let registry = root.path().join("registry.lock");
        guard.as_mut().unwrap().registry_file = Some(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&registry)?
                .into(),
        );
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        let fence = MountDestinationGuard::paths_for_canonical(&destination.canonicalize()?)?.0;
        let owner = guard.as_mut().unwrap();
        owner.prepare_destination_cleanup(&destination, &work)?;
        if shared {
            // Restore the former shared removal/registry allowance.
            owner
                .birth_cleanup
                .as_mut()
                .unwrap()
                .absorb(&mut owner.destination_cleanup.as_mut().unwrap().2)?;
        }
        Ok(Self {
            packet: crate::native_mount::startup::NativeMountRetirement {
                destination_guard: guard,
                retained_source: None,
                startup_source_watch: None,
            },
            work,
            cancellation,
            before,
            live,
            destination,
            fence,
            registry,
            _root: root,
        })
    }

    fn registry_is_unlocked(&self) -> Result<bool, std::io::Error> {
        is_unlocked(&self.open_registry()?)
    }

    fn open_registry(&self) -> Result<std::fs::File, std::io::Error> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.registry)
    }

    fn failed_remove(&mut self, shared: bool) -> Result<(), Box<dyn std::error::Error>> {
        let saved = self.destination.with_file_name("retained-directory");
        std::fs::rename(&self.destination, &saved)?;
        std::fs::write(&self.destination, b"not a directory")?;
        let result = if shared {
            legacy_remove(self.packet.destination_guard.as_mut().unwrap())
        } else {
            self.packet.retire(None).map(|_| ())
        };
        std::fs::remove_file(&self.destination)?;
        std::fs::rename(saved, &self.destination)?;
        assert!(
            matches!(result, Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Io(ref error)))
            if error.kind() == std::io::ErrorKind::NotADirectory),
            "{result:?}"
        );
        assert!(self.registry_is_unlocked()?);
        Ok(())
    }

    fn exact_retired(&self, failed_removes: u64, failed_unlinks: u64) -> bool {
        let mut expected = super::tests::expected(self.before, self.live, "owned-mount");
        expected.backend_write_operations += failed_removes + 3 * failed_unlinks;
        self.packet.is_retired()
            && !self.destination.exists()
            && !self.fence.exists()
            && self.work.receipt() == expected
            && self.work.live_allocation_bytes() == self.live
    }
}

/// Whether `file`'s lock is free, leaving it free. A probe that takes the lock
/// releases it explicitly: closing the probe releases it only once every copy
/// of its open file description is closed, and a process another test spawns
/// meanwhile holds a copy until it execs, so the next probe would find this
/// probe's own lock still held.
fn is_unlocked(file: &std::fs::File) -> Result<bool, std::io::Error> {
    match fs2::FileExt::try_lock_exclusive(file) {
        Ok(()) => fs2::FileExt::unlock(file).map(|()| true),
        Err(error) if lock_is_contended(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

// Reverted production operations: the failed remove and the whole registry
// transaction spend one reservation, with no refill or exclusive unlock credit.
fn legacy_remove(guard: &mut MountDestinationGuard) -> Result<(), NativeMountError> {
    let (name, _, _) = guard.destination_cleanup.as_ref().unwrap();
    let mut work = NativeFenceReleaseWork {
        credit: guard.birth_cleanup.as_ref().unwrap(),
        wait: None,
        pending: None,
    };
    crate::native_host::remove_prepared_leaf(
        guard.parent_directory.as_ref().unwrap(),
        name,
        true,
        &mut work,
    )?;
    guard.destination_cleanup.take();
    Ok(())
}

fn legacy_release(guard: &mut MountDestinationGuard) -> Result<(), NativeMountError> {
    let registry = guard.registry_file.as_ref().unwrap();
    let mut work = NativeFenceReleaseWork {
        credit: guard.birth_cleanup.as_ref().unwrap(),
        wait: None,
        pending: None,
    };
    lock_mount_file_with_work(registry, true, &mut work)?;
    let removed = crate::native_host::remove_ascii_leaf(
        guard.parent_directory.as_ref().unwrap(),
        guard.cleanup_name.as_ref().unwrap(),
        &mut work,
    );
    let unlocked = crate::native_host::NativeHostWork::attempt_io(
        &mut work,
        crate::materializer::NativeResourceCredit::WriteOperations.delta(1),
        || fence_io::unlock(registry),
    );
    removed?;
    unlocked?;
    guard.file.take();
    Ok(())
}

#[test]
fn registry_probe_distinguishes_fixture_locks_from_unrelated_owners()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new(false)?;
    let global_path = registry::path()?;
    let global = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&global_path)?;
    fs2::FileExt::lock_exclusive(&global)?;
    // Negative control: the former user-wide probe mistakes this unrelated
    // owner for a leaked retirement lock, even though the fixture is unlocked.
    let old_probe = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&global_path)?;
    assert!(lock_is_contended(
        &fs2::FileExt::try_lock_exclusive(&old_probe).unwrap_err()
    ));
    assert!(fixture.registry_is_unlocked()?);
    let private = fixture
        .packet
        .destination_guard
        .as_ref()
        .unwrap()
        .registry_file
        .as_ref()
        .unwrap();
    fence_io::lock(private)?;
    assert!(!fixture.registry_is_unlocked()?);
    fence_io::unlock(private)?;
    assert!(fixture.registry_is_unlocked()?);
    // Released explicitly, like every probe: other tests use this registry.
    fs2::FileExt::unlock(&global)?;
    Ok(())
}

/// A process holding a copy of a test's descriptor, reaped on every exit path.
struct Sharing(std::process::Child);
impl Drop for Sharing {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A probe leaves no lock behind in a process spawned while it was open. Such
/// a process shares the probe's open file description until it execs, as any
/// process spawned by a concurrent test does; the `sleep` child holds it for
/// the whole test. Negative control: a probe released by closing it, as before,
/// leaves the next probe finding the registry locked, which failed
/// `arbitrary_failed_removes_and_unlinks_keep_exact_original_work` under gate
/// load.
#[test]
fn a_registry_probe_leaves_no_lock_in_a_concurrently_spawned_process()
-> Result<(), Box<dyn std::error::Error>> {
    let spawn_sharing = |probe: &std::fs::File| -> std::io::Result<Sharing> {
        std::process::Command::new("sleep")
            .arg("60")
            .stdin(probe.try_clone()?)
            .spawn()
            .map(Sharing)
    };

    // The control locks its own registry: a lock released only by close may
    // linger in any process a concurrent test forks meanwhile, so it must not
    // reach the registry the fixed probe checks below.
    let control = Fixture::new(false)?;
    let closed = control.open_registry()?;
    let _control_sharing = spawn_sharing(&closed)?;
    fs2::FileExt::try_lock_exclusive(&closed)?;
    drop(closed);
    assert!(
        !control.registry_is_unlocked()?,
        "control: a closed probe's lock outlives it"
    );

    let fixture = Fixture::new(false)?;
    let probe = fixture.open_registry()?;
    let _sharing = spawn_sharing(&probe)?;
    assert!(is_unlocked(&probe)?);
    drop(probe);
    assert!(fixture.registry_is_unlocked()?);
    Ok(())
}

#[test]
fn failed_cleanup_retries_keep_registry_release_funded_after_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    for shared in [false, true] {
        let mut fixture = Fixture::new(shared)?;
        fixture.cancellation.cancel();
        fixture.failed_remove(shared)?;
        if shared {
            let guard = fixture.packet.destination_guard.as_mut().unwrap();
            legacy_remove(guard)?;
            assert!(matches!(
                legacy_release(guard),
                Err(NativeMountError::Materialization(
                    crate::NativeTreeMaterializationError::Work(
                        crate::WorkError::BudgetExceeded { .. }
                    )
                ))
            ));
            assert!(
                !fixture.registry_is_unlocked()?,
                "control did not strand the lock"
            );
            assert!(!fixture.exact_retired(1, 0));
        } else {
            assert!(fixture.packet.retire(None)?);
            assert!(fixture.registry_is_unlocked()?);
            assert!(fixture.exact_retired(1, 0));
        }
    }
    Ok(())
}

#[test]
fn insufficient_retry_budget_fails_before_registry_acquisition_and_can_be_refunded()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = Fixture::new(false)?;
    let held = fixture.work.reserve_work(crate::WorkCounters {
        backend_write_operations: fixture.work.remaining()?.backend_write_operations,
        ..crate::WorkCounters::UNCHARGED
    })?;
    fixture.cancellation.cancel();
    fixture.failed_remove(false)?;
    assert!(matches!(
        fixture.packet.retire(None),
        Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { .. })
        ))
    ));
    assert!(fixture.registry_is_unlocked()?);
    assert!(
        fixture
            .packet
            .destination_guard
            .as_ref()
            .unwrap()
            .file
            .is_some()
    );
    drop(held);
    assert!(fixture.packet.retire(None)?);
    assert!(fixture.registry_is_unlocked()?);
    assert!(fixture.exact_retired(1, 0));
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn arbitrary_failed_removes_and_unlinks_keep_exact_original_work(
        removes in 0u64..9, unlinks in 0u64..9,
    ) {
        let mut fixture = Fixture::new(false).unwrap();
        fixture.cancellation.cancel();
        for _ in 0..removes { fixture.failed_remove(false).unwrap(); }
        let fence = fixture.fence.clone();
        for _ in 0..unlinks {
            let saved = fence.with_file_name("retained-fence");
            std::fs::rename(&fence, &saved).unwrap();
            std::fs::create_dir(&fence).unwrap();
            assert!(fixture.packet.retire(None).is_err());
            assert!(fixture.registry_is_unlocked().unwrap());
            std::fs::remove_dir(&fence).unwrap();
            std::fs::rename(saved, &fence).unwrap();
        }
        assert!(fixture.packet.retire(None).unwrap());
        assert!(fixture.registry_is_unlocked().unwrap());
        assert!(fixture.exact_retired(removes, unlinks));
    }
}
