use super::{FileLock, FileLockMode};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

fn open(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

fn is_free(path: &Path) -> io::Result<bool> {
    match FileLock::try_acquire(open(path)?, FileLockMode::Exclusive) {
        Ok(_probe) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(error),
    }
}

#[test]
fn modes_exclude_exactly_their_conflicts() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("lock");
    let first = FileLock::try_acquire(open(&path)?, FileLockMode::Shared)?;
    let second = FileLock::try_acquire(open(&path)?, FileLockMode::Shared)?;
    assert!(!is_free(&path)?);
    drop(first);
    assert!(!is_free(&path)?);
    drop(second);
    assert!(is_free(&path)?);

    let exclusive = FileLock::try_acquire(open(&path)?, FileLockMode::Exclusive)?;
    let shared = FileLock::try_acquire(open(&path)?, FileLockMode::Shared);
    assert_eq!(
        shared.map(|_| ()).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        exclusive
            .try_lock(FileLockMode::Exclusive)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    exclusive.unlock()?;
    assert!(!exclusive.is_held());
    assert!(is_free(&path)?);
    exclusive.lock(FileLockMode::Exclusive)?;
    assert!(!is_free(&path)?);
    Ok(())
}

/// An explicit release ends this handle's custody only: a lock retaken
/// through another handle on the same description survives its drop.
#[test]
fn a_released_handle_never_unlocks_a_later_holder_of_its_description() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("lock");
    let first = FileLock::try_acquire(open(&path)?, FileLockMode::Exclusive)?;
    let same_description = first.try_clone()?;
    first.unlock()?;
    let second = FileLock::try_acquire(same_description, FileLockMode::Exclusive)?;
    drop(first);
    assert!(!is_free(&path)?);
    drop(second);
    assert!(is_free(&path)?);
    Ok(())
}

/// A process spawned while a lock is held keeps a copy of its description.
/// Dropping the guard still frees the lock; closing a raw lock does not.
#[cfg(unix)]
#[test]
fn a_dropped_lock_is_free_while_a_spawned_process_shares_its_description() -> io::Result<()> {
    struct Sharing(std::process::Child);
    impl Drop for Sharing {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let share = |file: &File| -> io::Result<Sharing> {
        Ok(Sharing(
            std::process::Command::new("sleep")
                .arg("60")
                .stdin(file.try_clone()?)
                .spawn()?,
        ))
    };
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("lock");

    // Control: a raw lock released by close stays held by the child's copy.
    let raw = open(&path)?;
    raw.try_lock().map_err(io::Error::from)?;
    let sharing = share(&raw)?;
    drop(raw);
    assert!(!is_free(&path)?);
    drop(sharing);
    assert!(is_free(&path)?);

    let guarded = FileLock::try_acquire(open(&path)?, FileLockMode::Exclusive)?;
    let _sharing = share(&guarded)?;
    drop(guarded);
    assert!(is_free(&path)?);
    Ok(())
}
