//! Held directory listings against an independent `read_dir` and
//! `symlink_metadata` baseline, with the listing's exact storage and
//! per-entry examination work.

use super::*;
use acyclic_test_support::differential::observe;
use proptest::prelude::*;

type Event = (&'static str, u64, u64);

/// `kind`: 0 file, 1 directory, 2 symbolic link (a file on Windows).
fn populate(root: &std::path::Path, entries: &[(u8, usize)]) -> std::io::Result<()> {
    for (index, (kind, size)) in entries.iter().enumerate() {
        let path = root.join(format!("entry-{index}"));
        match kind {
            1 => std::fs::create_dir(&path)?,
            #[cfg(unix)]
            2 => std::os::unix::fs::symlink("x".repeat(size % 200 + 1), &path)?,
            _ => std::fs::write(&path, vec![7_u8; *size])?,
        }
    }
    Ok(())
}

fn list(root: &std::path::Path) -> Result<Vec<HostListedEntry>, Box<dyn std::error::Error>> {
    let directory = Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
    let work =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
    let (mut reader, _allocation) = read_dir_held(&directory, &mut &work)?;
    let mut entries = Vec::new();
    while let Some((entry, _allocation)) = reader.next(&mut &work)? {
        entries.push(entry);
    }
    Ok(entries)
}

fn old_list(root: &std::path::Path) -> Result<Vec<HostListedEntry>, Box<dyn std::error::Error>> {
    let directory = Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
    let work =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
    let (mut reader, _allocation) =
        listing_baseline::baseline_read_dir_held(&directory, &mut &work)?;
    let mut entries = Vec::new();
    while let Some((entry, _allocation)) = reader.next(&mut &work)? {
        entries.push(entry);
    }
    Ok(entries)
}

#[cfg(unix)]
fn name(entry: &HostListedEntry) -> String {
    entry.name.to_string_lossy().into_owned()
}
#[cfg(windows)]
fn name(entry: &HostListedEntry) -> String {
    String::from_utf16_lossy(&entry.name)
}

/// Every field consumers read, from the listing.
#[cfg(unix)]
fn facts(stat: &HostStat) -> Vec<i128> {
    let raw = &stat.raw;
    let mut facts = vec![
        i128::from(raw.st_dev),
        i128::from(raw.st_ino),
        i128::from(raw.st_mode),
        i128::from(raw.st_nlink),
        i128::from(raw.st_uid),
        i128::from(raw.st_gid),
        i128::from(raw.st_rdev),
        i128::from(raw.st_size),
        i128::from(raw.st_atime),
        i128::from(raw.st_atime_nsec),
        i128::from(raw.st_mtime),
        i128::from(raw.st_mtime_nsec),
        i128::from(raw.st_ctime),
        i128::from(raw.st_ctime_nsec),
    ];
    #[cfg(target_os = "macos")]
    facts.extend([
        i128::from(raw.st_birthtime),
        i128::from(raw.st_birthtime_nsec),
        i128::from(raw.st_flags),
    ]);
    facts
}
/// The same fields from `lstat`, through the standard library.
#[cfg(unix)]
fn baseline(path: &std::path::Path) -> std::io::Result<Vec<i128>> {
    use std::os::unix::fs::MetadataExt as _;
    let meta = std::fs::symlink_metadata(path)?;
    let mut facts = vec![
        i128::from(meta.dev()),
        i128::from(meta.ino()),
        i128::from(meta.mode()),
        i128::from(meta.nlink()),
        i128::from(meta.uid()),
        i128::from(meta.gid()),
        i128::from(meta.rdev()),
        i128::from(meta.size()),
        i128::from(meta.atime()),
        i128::from(meta.atime_nsec()),
        i128::from(meta.mtime()),
        i128::from(meta.mtime_nsec()),
        i128::from(meta.ctime()),
        i128::from(meta.ctime_nsec()),
    ];
    #[cfg(target_os = "macos")]
    {
        use std::os::macos::fs::MetadataExt as _;
        facts.extend([
            i128::from(meta.st_birthtime()),
            i128::from(meta.st_birthtime_nsec()),
            i128::from(meta.st_flags()),
        ]);
    }
    Ok(facts)
}
#[cfg(windows)]
fn facts(stat: &HostStat) -> Vec<i128> {
    let directory = stat.file_type() == crate::demand::SourceNodeKind::Directory;
    vec![
        i128::from(if directory { 0 } else { stat.len() }),
        i128::from(directory),
        i128::from(stat.raw.FileId.cast_unsigned()),
        i128::from(stat.volume_serial_number),
        i128::from(stat.raw.NumberOfLinks),
        i128::from(stat.raw.FileAttributes),
        i128::from(stat.raw.CreationTime),
        i128::from(stat.raw.LastAccessTime),
        i128::from(stat.raw.LastWriteTime),
    ]
}
#[cfg(windows)]
fn baseline(path: &std::path::Path) -> std::io::Result<Vec<i128>> {
    use cap_std::fs::MetadataExt as _;
    let invalid = || io::Error::other("fixture must have a parent and leaf");
    let directory = Dir::open_ambient_dir(
        path.parent().ok_or_else(invalid)?,
        cap_std::ambient_authority(),
    )?;
    let meta = directory.symlink_metadata(path.file_name().ok_or_else(invalid)?)?;
    let length = if meta.is_dir() { 0 } else { meta.len() };
    Ok(vec![
        i128::from(length),
        i128::from(meta.is_dir()),
        i128::from(
            cap_primitives::fs::_WindowsByHandle::file_index(&meta)
                .ok_or_else(|| io::Error::other("missing file index"))?,
        ),
        i128::from(
            cap_primitives::fs::_WindowsByHandle::volume_serial_number(&meta)
                .ok_or_else(|| io::Error::other("missing volume"))?,
        ),
        i128::from(
            cap_primitives::fs::_WindowsByHandle::number_of_links(&meta)
                .ok_or_else(|| io::Error::other("missing link count"))?,
        ),
        i128::from(meta.file_attributes()),
        i128::from(meta.creation_time()),
        i128::from(meta.last_access_time()),
        i128::from(meta.last_write_time()),
    ])
}

/// The work an exact listing performs: one storage event with nothing
/// initialized, then (macOS) one examination per entry, `fstatat` only for
/// directories.
fn expected_work(entries: &[(u8, usize)], listed: &[String], old: bool) -> Vec<Event> {
    let bytes = HostStatReader::BUFFER_BYTES as u64;
    let mut work = vec![("storage", if old { bytes } else { 0 }, bytes)];
    #[cfg(target_os = "macos")]
    for name in listed {
        let index: usize = name["entry-".len()..].parse().unwrap();
        let bulk = !old && entries[index].0 != 1;
        work.push((if bulk { "bulk" } else { "fstatat" }, 1, 0));
    }
    #[cfg(not(target_os = "macos"))]
    let _ = (entries, listed);
    work
}

fn observe_listing(
    root: &std::path::Path,
    old: bool,
) -> (Result<Vec<HostListedEntry>, String>, Vec<Event>) {
    observe::<_, _, Event>(
        || (),
        |()| {
            let run = || list(root).map_err(|error| error.to_string());
            if old {
                reader_work::restoring_old_work(run)
            } else {
                run()
            }
        },
    )
}

/// Lists `entries` and returns whether the work trace matched the exact
/// model; names and facts must always match the baseline.
fn check(entries: &[(u8, usize)], old: bool) -> Result<bool, Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    populate(temporary.path(), entries)?;
    let (listed, work) = observe_listing(temporary.path(), old);
    let listed = listed?;
    let names: Vec<String> = listed.iter().map(name).collect();
    let mut sorted = names.clone();
    sorted.sort();
    let mut expected: Vec<String> = std::fs::read_dir(temporary.path())?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    expected.sort();
    assert_eq!(sorted, expected);
    let old_directory = tempfile::tempdir()?;
    populate(old_directory.path(), entries)?;
    let (old_listed, old_trace) =
        observe::<_, _, Event>(|| (), |()| old_list(old_directory.path()));
    let old_listed = old_listed?;
    let mut old_names: Vec<_> = old_listed.iter().map(name).collect();
    old_names.sort();
    assert_eq!(sorted, old_names);
    assert_eq!(
        old_trace,
        expected_work(
            entries,
            &old_listed.iter().map(name).collect::<Vec<_>>(),
            true
        )
    );
    for entry in &old_listed {
        assert_eq!(
            facts(entry.stat.as_ref().expect("old entry exists")),
            baseline(&old_directory.path().join(name(entry)))?
        );
    }
    for entry in &listed {
        let stat = entry.stat.as_ref().expect("entry exists");
        let path = temporary.path().join(name(entry));
        assert_eq!(facts(stat), baseline(&path)?, "{}", name(entry));
    }
    Ok(work == expected_work(entries, &names, false))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn listings_match_lstat_without_zero_fill_or_file_examinations(
        entries in prop::collection::vec((0_u8..3, 0_usize..3000), 0..24),
    ) {
        prop_assert!(check(&entries, false).map_err(|error| TestCaseError::fail(error.to_string()))?);
    }
}

/// Crosses several refills of the record buffer.
#[test]
fn a_listing_larger_than_one_refill_matches_lstat() -> Result<(), Box<dyn std::error::Error>> {
    let entries: Vec<(u8, usize)> = (0..700)
        .map(|index| (u8::from(index % 7 == 3), index % 5))
        .collect();
    assert!(check(&entries, false)?);
    Ok(())
}

/// The previous behaviour still matches `lstat` but fails the work model.
#[test]
fn restored_zero_fill_and_file_examinations_are_detected() -> Result<(), Box<dyn std::error::Error>>
{
    assert!(!check(&[(0, 3), (1, 0), (2, 4)], true)?);
    Ok(())
}

#[test]
#[ignore = "measurement"]
fn measure_directory_listing() -> Result<(), Box<dyn std::error::Error>> {
    let entries: Vec<_> = (0..1000).map(|n| (u8::from(n % 10 == 0), n % 64)).collect();
    let old_directory = tempfile::tempdir()?;
    let directory = tempfile::tempdir()?;
    populate(old_directory.path(), &entries)?;
    populate(directory.path(), &entries)?;
    for old in [true, false] {
        let run = || {
            if old {
                old_list(old_directory.path())
            } else {
                list(directory.path())
            }
        };
        for _ in 0..5 {
            assert_eq!(run()?.len(), entries.len());
        }
        let start = std::time::Instant::now();
        for _ in 0..40 {
            assert_eq!(run()?.len(), entries.len());
        }
        eprintln!(
            "directory 1000 entries {}: {} ns/list",
            if old { "baseline" } else { "candidate" },
            start.elapsed().as_nanos() / 40
        );
    }
    Ok(())
}

#[test]
fn listing_allocation_count_and_peak_are_exact() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let entries = vec![(0, 3); 10];
    populate(directory.path(), &entries)?;
    let held = Dir::open_ambient_dir(directory.path(), cap_std::ambient_authority())?;
    let run = || -> Result<WorkCounters, Box<dyn std::error::Error>> {
        let work =
            NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                .map_err(|(cause, _)| cause)?;
        let (mut reader, allocation) = read_dir_held(&held, &mut &work)?;
        let mut count = 0;
        while let Some((entry, guard)) = reader.next(&mut &work)? {
            count += 1;
            drop(entry);
            drop(guard);
        }
        assert_eq!(count, 10);
        drop(reader);
        drop(allocation);
        assert_eq!(
            work.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
        Ok(work.receipt())
    };
    let expected_peak = NativeMaterializationWork::accounting_state_bytes()
        + HostStatReader::BUFFER_BYTES as u64
        + 7 * std::mem::size_of::<NativeUnit>() as u64;
    let work = run()?;
    assert_eq!(work.allocation_operations, 12);
    assert_eq!(work.peak_allocation_bytes, expected_peak);
    let broken = reader_work::adding_allocation(run)?;
    assert_eq!(broken.allocation_operations, 13);
    // The added buffer is live at the 64 KiB buffer's allocator call, then retired.
    assert_eq!(
        broken.peak_allocation_bytes,
        NativeMaterializationWork::accounting_state_bytes()
            + HostStatReader::BUFFER_BYTES as u64
            + 17
    );
    assert_ne!(
        (broken.allocation_operations, broken.peak_allocation_bytes),
        (12, expected_peak)
    );
    Ok(())
}

#[test]
fn directory_storage_has_no_zero_fill_site() {
    let source = include_str!("reader.rs");
    assert!(!source.contains(".resize("));
    let broken = format!("{source}\nbuffer.resize(8192, 0);");
    assert!(broken.contains(".resize("));
}
