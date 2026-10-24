//! A creation commit that publishes the workspace's first files.
use super::*;

fn budget() -> Result<WorkBudget, Box<dyn Error>> {
    let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    for allowance in allowances
        .as_object_mut()
        .ok_or("budget fields missing")?
        .values_mut()
    {
        *allowance = serde_json::json!(100_000_000_u64);
    }
    Ok(serde_json::from_value(allowances)?)
}

#[tokio::test]
async fn creation_publishes_files_in_its_one_commit_and_recovers_only_them()
-> Result<(), Box<dyn Error>> {
    let config = crate::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(crate::access::INITIAL_POLICY),
    };
    let operation = IdempotencyKey::from_bytes([0xe1; 16]).operation_id();
    let cancellation = CancellationToken::new();
    let budget = budget()?;
    let files = [
        ("/report.json", OwnedBytes::from_static(b"{\"ok\":true}")),
        ("/request.json", OwnedBytes::from_static(b"{}")),
    ];

    // The zero-files case of the same path is the empty workspace.
    let empty_fs = Fs::memory();
    let empty = empty_fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            operation,
            &[],
            budget,
            &cancellation,
            None,
        )
        .await?;
    let empty_workspace = unbounded!(empty_fs, open_workspace("journal"))?;
    assert!(unbounded!(empty_workspace, stat("/report.json")).is_err());

    let fs = Fs::memory();
    let created = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            operation,
            &files,
            budget,
            &cancellation,
            None,
        )
        .await?;
    // Files cost no authority record beyond the creation commit itself.
    assert_eq!(
        created.work.authority_records_appended,
        empty.work.authority_records_appended
    );
    assert_ne!(created.value.id(), empty.value.id());
    let workspace = unbounded!(fs, open_workspace("journal"))?;
    assert_eq!(unbounded!(workspace, head())?.id(), created.value.id());
    for (path, bytes) in &files {
        assert_eq!(
            unbounded!(workspace, read_range(path, 0, bytes.len() as u64))?.bytes,
            bytes.clone()
        );
    }

    // A retry of the same creation recovers it without appending.
    let retried = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "journal",
            config,
            operation,
            &files,
            budget,
            &cancellation,
            None,
        )
        .await?;
    assert_eq!(retried.value.id(), created.value.id());
    assert_eq!(retried.work.authority_records_appended, 0);

    // Negative controls: the same operation with other files, with no files,
    // or other files under another operation never recovers this creation.
    let other = [("/report.json", OwnedBytes::from_static(b"{\"ok\":false}"))];
    for (operation, files) in [
        (operation, &other[..]),
        (operation, &files[..1]),
        (operation, &[][..]),
        (
            IdempotencyKey::from_bytes([0xe2; 16]).operation_id(),
            &files[..],
        ),
    ] {
        let failure = fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                "journal",
                config,
                operation,
                files,
                budget,
                &cancellation,
                None,
            )
            .await
            .err()
            .ok_or("mismatched creation recovered")?;
        assert!(
            matches!(
                failure.error,
                WorkspaceError::Filesystem(FsError::CreationRejected)
            ),
            "{:?}",
            failure.error
        );
        assert_eq!(failure.work.authority_records_appended, 0);
    }
    assert_eq!(unbounded!(workspace, head())?.id(), created.value.id());
    Ok(())
}
