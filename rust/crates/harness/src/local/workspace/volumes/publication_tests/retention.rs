//! Native scope cost and physical retention, independent of shell startup.
use super::*;

#[tokio::test]
async fn verified_commands_reuse_one_mount_until_disconnect() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = open(root.path(), state.path()).await?;
    let mounts = Arc::clone(
        &harness
            .workspace()
            .ok_or_else(|| Error::Invalid("fixture workspace absent".into()))?
            .mounts,
    );
    let scratch = Arc::clone(
        mounts
            .named(Some("scratch"))?
            .1
            .view
            .source
            .as_ref()
            .ok_or_else(|| Error::Invalid("scratch source absent".into()))?,
    );
    let mut identities = Vec::new();
    let mut retained = 0;
    let mut elapsed = Vec::new();
    for index in 0..4 {
        let bytes = format!("verified {index}");
        let started = std::time::Instant::now();
        sync(&mounts).await?;
        native_scope(&mounts, async |claims| {
            identities
                .push(acyclic_fs::capture_root_identity(&scratch.destination).map_err(storage)?);
            write_projected(&scratch.destination.join("data"), bytes.as_bytes())?;
            Ok(((), claims))
        })
        .await?;
        elapsed.push(started.elapsed().as_micros());
        retained += usize::from(scratch.native().await.is_some());
        assert_eq!(published(&scratch, "/data").await?, bytes.as_bytes());
    }
    eprintln!("MOUNT_COST calls=4 retained={retained} elapsed_us={elapsed:?}");
    assert_eq!(
        retained, 4,
        "verified command exits must retain their native projection"
    );
    assert!(
        identities
            .windows(2)
            .all(|pair| pair.first() == pair.get(1)),
        "commands must reuse the same physical namespace identity"
    );
    harness.close().await?;
    assert!(
        scratch.native().await.is_none(),
        "disconnect must retire retained projections"
    );
    assert!(
        !scratch.destination.exists(),
        "disconnect must remove SDK-owned residue"
    );
    Ok(())
}
