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
    ]
}
#[cfg(windows)]
fn baseline(path: &std::path::Path) -> std::io::Result<Vec<i128>> {
    let meta = std::fs::symlink_metadata(path)?;
    let length = if meta.is_dir() { 0 } else { meta.len() };
    Ok(vec![i128::from(length), i128::from(meta.is_dir())])
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
