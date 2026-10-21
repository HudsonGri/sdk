use super::cache::{Cache, Entry};
use super::*;
use std::fs::OpenOptions;

/// A cache of its own, so the bound a test sets binds only its files.
fn private_cache(capacity: usize) -> Arc<Cache<Native>> {
    Arc::new(Cache::new(Native, capacity))
}

fn create(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

fn cached(cache: &Arc<Cache<Native>>, path: &Path) -> io::Result<Arc<Entry<Native>>> {
    Entry::new(cache, create(path)?, path, true, true)
}

/// Descriptors this process holds on files under `directory`.
#[cfg(target_os = "linux")]
fn descriptors_under(directory: &Path) -> io::Result<usize> {
    let mut count = 0;
    for entry in std::fs::read_dir("/proc/self/fd")? {
        if let Ok(target) = std::fs::read_link(entry?.path())
            && target.starts_with(directory)
        {
            count += 1;
        }
    }
    Ok(count)
}

fn write_at(entry: &Arc<Entry<Native>>, offset: u64, bytes: &[u8]) -> io::Result<()> {
    entry
        .lease(|| Ok(()))?
        .write(|file| crate::write_all_at(file, offset, bytes, None))
}

fn read(entry: &Arc<Entry<Native>>, length: usize) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; length];
    let lease = entry.lease(|| Ok(()))?;
    let read = crate::read_at(lease.file(), 0, &mut bytes)?;
    bytes.truncate(read);
    Ok(bytes)
}

#[test]
fn idle_files_hold_at_most_the_capacity_in_descriptors() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let directory_path = std::fs::canonicalize(directory.path())?;
    let cache = private_cache(4);
    let files = (0..64)
        .map(|index| cached(&cache, &directory_path.join(format!("{index}.log"))))
        .collect::<io::Result<Vec<_>>>()?;
    for (index, file) in files.iter().enumerate() {
        write_at(file, 0, format!("file {index}").as_bytes())?;
    }
    assert!(cache.cached() <= 4, "{} cached", cache.cached());
    #[cfg(target_os = "linux")]
    assert!(descriptors_under(&directory_path)? <= 4);
    // Every file reopens with its contents.
    for (index, file) in files.iter().enumerate() {
        assert_eq!(read(file, 64)?, format!("file {index}").into_bytes());
    }
    assert!(cache.cached() <= 4);
    Ok(())
}

#[test]
fn a_file_replaced_or_removed_while_closed_is_refused() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = private_cache(1);
    let replaced_path = directory.path().join("replaced.log");
    let removed_path = directory.path().join("removed.log");
    let replaced = cached(&cache, &replaced_path)?;
    let removed = cached(&cache, &removed_path)?;
    // Admitting `removed` closed `replaced`; another file takes its name.
    let other = directory.path().join("other.log");
    std::fs::write(&other, b"another")?;
    std::fs::rename(&other, &replaced_path)?;
    let refusal = replaced.lease(|| Ok(())).map(drop);
    assert_eq!(
        refusal.map_err(|error| error.kind()),
        Err(io::ErrorKind::InvalidData)
    );
    // That reopen failed, so `removed` keeps its descriptor; close it.
    let _ = cached(&cache, &directory.path().join("third.log"))?;
    std::fs::remove_file(&removed_path)?;
    assert_eq!(
        removed
            .lease(|| Ok(()))
            .map(drop)
            .map_err(|error| error.kind()),
        Err(io::ErrorKind::NotFound)
    );
    Ok(())
}

#[test]
fn the_default_capacity_is_a_quarter_of_the_descriptor_limit() {
    let capacity = default_capacity();
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        let limit = match descriptor_limit() {
            0 => 1024,
            limit => limit,
        };
        let quarter = usize::try_from(limit / 4).unwrap_or(usize::MAX);
        assert_eq!(capacity, quarter.clamp(16, 1 << 16));
    }
    assert!((16..=1 << 16).contains(&capacity));
}

#[test]
fn a_metered_lease_charges_only_the_reopen() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = private_cache(1);
    let file = cached(&cache, &directory.path().join("log"))?;
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    let charged = || {
        meter
            .used_operations
            .load(std::sync::atomic::Ordering::Acquire)
    };
    let admit = || {
        crate::NativeReadMeter::check(&meter)?;
        crate::NativeReadMeter::admit_read(&meter, 0)
    };
    // Open: nothing to charge.
    drop(file.lease(admit)?);
    assert_eq!(charged(), 0);
    let _ = cached(&cache, &directory.path().join("other"))?;
    drop(file.lease(admit)?);
    assert_eq!(charged(), 1, "the reopen is one read operation");
    Ok(())
}
