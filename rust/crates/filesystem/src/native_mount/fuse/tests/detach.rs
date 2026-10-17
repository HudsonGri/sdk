use super::*;

// Exercise every allocation prefix up to the first successful operation. This
// finds the exact post-effect allocation that a single guessed budget misses.
#[test]
fn allocation_refusal_precedes_unlink_and_replacement() -> Result<(), Box<dyn std::error::Error>> {
    for rename in [false, true] {
        let mut succeeded = false;
        for allowance in 0..128 {
            let (source, _) = shared_sources(posix_config())?;
            let source = Arc::new(source);
            let setup = fuse_fixture_work();
            let path = name("retained");
            source.create_file(&path, FileMetadata::default(), None, &setup)?;
            source.write_range(&path, 0, Bytes::from_static(b"original"))?;
            source.create_file(&name("replacement"), FileMetadata::default(), None, &setup)?;
            let projection = super::super::FuseProjection {
                core: projection_core(
                    source.clone(),
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )?,
            };
            let inode = projection
                .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("retained"), &setup)?
                .attr
                .ino
                .0;
            let (handle, _) = projection.open_node(inode, libc::O_RDONLY, &setup)?;
            let work = crate::NativeMaterializationWork::new(
                crate::WorkBudget {
                    allocation_operations: allowance,
                    ..crate::WorkBudget::UNBOUNDED
                },
                &crate::CancellationToken::new(),
            )
            .map_err(|(error, _)| error)?;
            let result = if rename {
                projection.rename_name(
                    ROOT_INODE,
                    std::ffi::OsStr::new("replacement"),
                    ROOT_INODE,
                    std::ffi::OsStr::new("retained"),
                    0,
                    &work,
                )
            } else {
                projection.remove_name(ROOT_INODE, std::ffi::OsStr::new("retained"), &work)
            };
            if result.is_err() {
                assert!(
                    source.lookup(&path)?.is_some(),
                    "allowance {allowance}: {result:?}"
                );
                assert_eq!(
                    source.read_range(&path, 0, 8)?.as_ref(),
                    b"original",
                    "allowance {allowance}: {result:?}"
                );
            } else {
                let file = projection
                    .core
                    .state()
                    .map_err(NativeMountError::from)?
                    .file_handle(inode, handle)
                    .map_err(NativeMountError::from)?
                    .open_file
                    .clone();
                assert_eq!(file.file().read_range(0, 8)?.as_ref(), b"original");
                succeeded = true;
                break;
            }
        }
        assert!(
            succeeded,
            "no successful fixture operation reached within the modeled allocation prefixes"
        );
    }
    Ok(())
}

#[test]
fn post_unlink_allocation_negative_control() -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let setup = fuse_fixture_work();
    let path = name("retained");
    source.create_file(&path, FileMetadata::default(), None, &setup)?;
    let projection = super::super::FuseProjection {
        core: projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("retained"), &setup)?
        .attr
        .ino
        .0;
    projection.open_node(inode, libc::O_RDONLY, &setup)?;
    let detached = super::super::FuseOpenFile(super::super::fuse_open_file(
        source.detach_file_with_meter(&path, &setup)?,
        &setup,
    )?);
    source.remove(&path, None, &setup)?;
    let refused = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            allocation_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    assert!(
        projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .prepare_detached_handles(inode, &detached, &refused)
            .is_err()
    );
    assert!(
        source.lookup(&path)?.is_none(),
        "the old ordering must violate namespace preservation on refusal"
    );
    Ok(())
}
