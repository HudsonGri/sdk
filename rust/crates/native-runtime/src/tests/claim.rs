use super::*;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

const CHILD: &str = "ACYCLIC_CLAIM_CHILD";
/// Printed once the child holds its claim. libtest prints it after the
/// test name on the same line.
const HELD: &str = "acyclic-claim-held";

fn contended(result: io::Result<StoreClaim>) -> bool {
    matches!(result, Err(error) if is_exclusive_lock_contention(&error))
}

#[test]
fn a_claim_excludes_a_second_claim_until_it_is_released() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let claim = StoreClaim::acquire(&path)?;
    assert!(contended(StoreClaim::acquire(&path)));
    // Another name for the same file is the same claim.
    let alias = directory.path().join(".").join("owner.lock");
    assert!(contended(StoreClaim::acquire(&alias)));
    drop(claim);
    assert_eq!(
        std::fs::metadata(&path)?.len(),
        0,
        "release clears the record"
    );
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn racing_claims_admit_exactly_one_owner() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = Arc::new(directory.path().join("owner.lock"));
    for _ in 0..32 {
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let claims = (0..8)
            .map(|_| {
                let (path, barrier) = (Arc::clone(&path), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    barrier.wait();
                    StoreClaim::acquire(&path)
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .map_err(|_| io::Error::other("claimant panicked"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut owners = 0;
        for claim in claims {
            match claim {
                Ok(_) => owners += 1,
                Err(error) if is_exclusive_lock_contention(&error) => {}
                Err(error) => return Err(error),
            }
        }
        assert_eq!(owners, 1);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn held_claims_keep_no_descriptor_open() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    // Other tests open descriptors concurrently, so count only this test's.
    let open = || -> io::Result<usize> {
        let mut count = 0;
        for entry in std::fs::read_dir("/proc/self/fd")? {
            if std::fs::read_link(entry?.path())
                .is_ok_and(|target| target.starts_with(directory.path()))
            {
                count += 1;
            }
        }
        Ok(count)
    };
    let claims = (0..256)
        .map(|index| StoreClaim::acquire(&directory.path().join(format!("{index}.lock"))))
        .collect::<io::Result<Vec<_>>>()?;
    assert_eq!(open()?, 0, "{} claims", claims.len());
    drop(claims);
    Ok(())
}

/// A claim held by another process, which keeps it until its input closes.
struct Owner(Child);

impl Owner {
    fn spawn(path: &Path) -> io::Result<Self> {
        Self::spawn_with(path, &[])
    }

    fn spawn_with(path: &Path, environment: &[(&str, &Path)]) -> io::Result<Self> {
        let test = concat!(module_path!(), "::claim_owner_process");
        let test = test.split_once("::").map_or(test, |(_, path)| path);
        let mut child = Command::new(std::env::current_exe()?)
            .args(["--exact", test, "--test-threads=1", "--nocapture"])
            .env(CHILD, path)
            .envs(environment.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("no stdout"))?;
        let mut line = String::new();
        let mut lines = BufReader::new(stdout);
        while !line.contains(HELD) {
            line.clear();
            if lines.read_line(&mut line)? == 0 {
                return Err(io::Error::other("owner process exited before claiming"));
            }
        }
        // Keep reading so the child's later output never meets a closed pipe.
        std::thread::spawn(move || io::copy(&mut lines, &mut io::sink()));
        Ok(Self(child))
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn claim_owner_process() -> io::Result<()> {
    let Some(path) = std::env::var_os(CHILD) else {
        return Ok(());
    };
    let _claim = StoreClaim::acquire(Path::new(&path))?;
    println!("{HELD}");
    io::stdout().flush()?;
    // Hold the claim until the parent closes this input or kills this process.
    io::stdin().read_to_end(&mut Vec::new())?;
    Ok(())
}

/// The process lock a claim file's record names.
fn recorded_process_lock(path: &Path) -> io::Result<PathBuf> {
    let record = std::fs::read(path)?;
    crate::claim::recorded_process_lock(&record)
        .ok_or_else(|| io::Error::other(format!("no record in {record:?}")))
}

#[test]
fn another_process_claim_excludes_until_that_process_dies() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let mut owner = Owner::spawn(&path)?;
    assert!(contended(StoreClaim::acquire(&path)));
    owner.0.kill()?;
    owner.0.wait()?;
    // The dead owner's record remains, but its process lock lapsed with it.
    // Another process's sweep may already have removed the lock itself.
    recorded_process_lock(&path)?;
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn another_process_claim_survives_removal_of_its_process_lock() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let owner = Owner::spawn(&path)?;
    let lock = recorded_process_lock(&path)?;
    match std::fs::remove_file(&lock) {
        // A temporary-file cleaner can remove a live process's lock on Unix.
        Ok(()) => assert!(contended(StoreClaim::acquire(&path))),
        // Windows denies removing it while its owner lives.
        Err(error) if cfg!(windows) => {
            assert!(contended(StoreClaim::acquire(&path)), "{error}");
        }
        Err(error) => return Err(error),
    }
    drop(owner);
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn a_released_claim_admits_another_process() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let mut owner = Owner::spawn(&path)?;
    // Closing its input ends the owner normally, releasing the claim.
    drop(owner.0.stdin.take());
    assert!(owner.0.wait()?.success());
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn a_torn_record_names_no_owner() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let claim = StoreClaim::acquire(&path)?;
    let record = std::fs::read(&path)?;
    drop(claim);
    // A record cut anywhere, as by its writer's death mid-write.
    for cut in [1, record.len() / 2, record.len() - 1] {
        std::fs::write(&path, record.get(..cut).unwrap_or_default())?;
        drop(StoreClaim::acquire(&path)?);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn a_runtime_directory_with_newlines_names_its_owner_exactly() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir()?;
    let runtime = directory.path().join("run\nacyclic-claim-v1\n1\n\n");
    std::fs::create_dir(&runtime)?;
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))?;
    let path = directory.path().join("owner.lock");
    let mut owner = Owner::spawn_with(&path, &[("XDG_RUNTIME_DIR", &runtime)])?;
    let lock = recorded_process_lock(&path)?;
    assert!(lock.starts_with(&runtime), "{lock:?}");
    assert!(lock.exists());
    assert!(contended(StoreClaim::acquire(&path)));
    owner.0.kill()?;
    owner.0.wait()?;
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

/// A record naming process `pid` by `start`, whose process lock is gone.
#[cfg(target_os = "linux")]
fn lockless_record(directory: &Path, pid: u32, start: &[u8]) -> Vec<u8> {
    crate::claim::foreign_record(pid, start, &directory.join("gone.lock"))
}

#[cfg(target_os = "linux")]
#[test]
fn an_owner_whose_death_is_unproven_refuses() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    // No start identity was recorded, so no probe can tell this pid's
    // process from the owner: unknown, which refuses.
    std::fs::write(&path, lockless_record(directory.path(), u32::MAX, b""))?;
    assert!(contended(StoreClaim::acquire(&path)));
    // A start identity of another boot proves the owner died.
    std::fs::write(
        &path,
        lockless_record(directory.path(), std::process::id(), b"another-boot:1"),
    )?;
    drop(StoreClaim::acquire(&path)?);
    // A process that does not exist under a known boot is dead too.
    std::fs::write(
        &path,
        lockless_record(directory.path(), u32::MAX, b"boot:1"),
    )?;
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn release_never_clears_a_record_in_a_file_it_did_not_claim() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let moved = directory.path().join("moved.lock");
    let first = StoreClaim::acquire(&path)?;
    // The claimed file moves away and another store claims a new file there.
    std::fs::rename(&path, &moved)?;
    let second = StoreClaim::acquire(&path)?;
    let recorded = std::fs::read(&path)?;
    drop(first);
    assert_eq!(
        std::fs::read(&path)?,
        recorded,
        "the second claim's record survives"
    );
    drop(second);
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    Ok(())
}
