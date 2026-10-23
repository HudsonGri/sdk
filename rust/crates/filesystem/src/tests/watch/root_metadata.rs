use super::*;
use std::os::unix::fs::PermissionsExt as _;
use std::time::Duration;

#[test]
fn root_attributes_are_metadata_hints_and_replacements_still_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("watched");
    std::fs::create_dir(&root)?;
    let mut watch = NativeWatch::open(&root, NativeWatchOptions::new(VolumeLimits::default()))?;
    watch.begin_rescan()?;
    assert!(matches!(watch.finish_rescan()?, WatchBatch::Changes { .. }));
    let identity = watch.root_identity();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    watch.fence(Duration::from_secs(1))?;
    let observed = watch.poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert!(matches!(
        observed.value,
        WatchBatch::Changes { changes, .. }
            if changes.iter().any(|change| matches!(change,
                WatchChange::MetadataChanged(path) if path.is_root()))
    ));
    assert_eq!(watch.root_identity(), identity);
    std::fs::rename(&root, parent.path().join("displaced"))?;
    std::fs::create_dir(&root)?;
    let observed = watch.poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
    assert!(matches!(observed.value, WatchBatch::RescanRequired { .. }));
    Ok(())
}
