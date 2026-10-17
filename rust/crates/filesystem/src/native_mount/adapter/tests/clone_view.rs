use super::*;
use proptest::prelude::*;

fn check(source_bytes: &[u8], destination_bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let (source, destination) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::TrackingSafe,
        MountPublication::Manual,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let from = native_test_path("from");
    let to = native_test_path("to");
    source.create_file(&from, FileMetadata::default(), None, &work)?;
    source.write_range(&from, 0, Bytes::from_static(b"original"))?;
    source.create_file(&to, FileMetadata::default(), None, &work)?;
    source.write_range(&to, 0, Bytes::from_static(b"original"))?;
    source.sync()?;
    destination
        .runtime
        .wait(|| destination.advance_to_head_async())?;
    source.write_range(&from, 0, Bytes::copy_from_slice(source_bytes))?;
    destination.write_range(&from, 0, Bytes::copy_from_slice(destination_bytes))?;
    let input = source.open_file(&from)?;
    let output = destination.open_file(&to)?;
    assert_eq!(input.read_range(0, 8)?.as_ref(), source_bytes);
    assert!(matches!(
        output.clone_range_from(input.as_ref(), 0, 0, 8, &work),
        Err(MountSourceError::NativeUnsupported)
    ));
    assert_eq!(output.read_range(0, 8)?.as_ref(), b"original");
    // The old implementation silently selected this checkout's different bytes.
    let local = destination.open_file(&from)?;
    output.clone_range_from(local.as_ref(), 0, 0, 8, &work)?;
    assert_eq!(output.read_range(0, 8)?.as_ref(), destination_bytes);
    assert_ne!(
        destination_bytes, source_bytes,
        "negative control must distinguish the wrong checkout"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn native_access_clone_cannot_substitute_another_checkouts_bytes(a in any::<[u8;8]>(), b in any::<[u8;8]>()) {
        prop_assume!(a != b);
        check(&a, &b).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
