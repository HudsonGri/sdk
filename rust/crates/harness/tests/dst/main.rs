//! Seeded production replays: compare bytes, never rewrite identities or errors.
use acyclic_harness::{
    Error, LocalHarness, Result,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::FilesystemHost,
    model::Model,
    resources::ProviderRef,
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use acyclic_native_runtime::testing::Schedule;
use std::sync::Arc;

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        1_700_000_000_000
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Trace {
    boundaries: Vec<(String, String)>,
    requests: Vec<u8>,
    dispatches: Vec<u8>,
    files: Vec<u8>,
}

fn same_trace(left: &Trace, right: &Trace) -> std::result::Result<(), String> {
    if left.boundaries != right.boundaries {
        let index = left
            .boundaries
            .iter()
            .zip(&right.boundaries)
            .position(|(left, right)| left != right)
            .unwrap_or(left.boundaries.len().min(right.boundaries.len()));
        return Err(format!(
            "boundary {index}: {:?} != {:?}",
            left.boundaries.get(index),
            right.boundaries.get(index)
        ));
    }
    for (name, left, right) in [
        ("requests", &left.requests, &right.requests),
        ("dispatches", &left.dispatches, &right.dispatches),
        ("files", &left.files, &right.files),
    ] {
        if left != right {
            let offset = left
                .iter()
                .zip(right)
                .position(|(left, right)| left != right)
                .unwrap_or(left.len().min(right.len()));
            return Err(format!(
                "{name} byte {offset}: {:?} != {:?}",
                left.get(offset),
                right.get(offset)
            ));
        }
    }
    Ok(())
}

async fn scenario() -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    // The independently initialized stores and settings never discover HOME.
    let directory = tempfile::tempdir().map_err(storage)?;
    let reference = ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
    let volume = VolumeRef::new(
        reference.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("dst".into()),
        &mut None,
    )?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(storage)?;
    FilesystemHost::new(fs, &reference, None)?
        .create_volume(&volume)
        .await?;
    let model = Model::new(
        "scripted".into(),
        "dst".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
        "done".into(),
    )]));
    let config = || -> Result<_> {
        Ok(local_harness_config(
            directory.path().join("filesystem"),
            (model.clone(), provider.clone()),
        )?
        .volume(volume.clone())
        .state_directory(directory.path().join("state"))
        .clock(Arc::new(Clock)))
    };
    let harness = LocalHarness::open(config()?).await?;
    assert_eq!(harness.run("finish this turn").await?.text, "done");
    let files = harness
        .task_files()
        .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
        .await
        .map_err(storage)?
        .snapshot
        .ok_or_else(|| Error::Invalid("missing snapshot".into()))?
        .files;
    drop(harness);
    let reopened = LocalHarness::open(config()?).await?;
    let restored = reopened
        .task_files()
        .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
        .await
        .map_err(storage)?
        .snapshot
        .ok_or_else(|| Error::Invalid("missing reopened snapshot".into()))?
        .files;
    assert_eq!(files, restored, "cold reopen lost acknowledged files");
    Ok((
        serde_json::to_vec(&provider.requests()).map_err(storage)?,
        serde_json::to_vec(&provider.dispatches()).map_err(storage)?,
        serde_json::to_vec(&files).map_err(storage)?,
    ))
}

async fn replay(seed: u64, erase_source: bool) -> Result<Trace> {
    // Negative control removes only entropy injection, preserving real barriers.
    let (schedule, mut events) = if erase_source {
        Schedule::new()
    } else {
        Schedule::seeded(seed)
    };
    let mut turn = Box::pin(schedule.scope(scenario()));
    let mut boundaries = Vec::new();
    let (requests, dispatches, files) = loop {
        tokio::select! {
            biased;
            event = events.recv() => {
                let event = event.ok_or_else(|| Error::Invalid("lost boundary driver".into()))?;
                boundaries.push((event.name.into(), event.detail.to_string()));
                event.release();
            },
            result = &mut turn => break result?,
        }
    };
    schedule.drained().await;
    Ok(Trace {
        boundaries,
        requests,
        dispatches,
        files,
    })
}

#[test]
fn seeded_production_turn_replays_exact_bytes() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        let first = replay(71, false).await?;
        let second = replay(71, false).await?;
        same_trace(&first, &second).expect("same seed diverged");
        assert!(
            !first.boundaries.is_empty(),
            "production boundaries were bypassed"
        );
        let different = replay(72, false).await?;
        assert_ne!(
            first.dispatches, different.dispatches,
            "seed did not reach production identities"
        );
        let broken = replay(71, true).await?;
        assert!(
            same_trace(&first, &broken).is_err(),
            "unseeded trace escaped the replay oracle"
        );
        assert_ne!(
            first.dispatches, broken.dispatches,
            "unseeded production negative control escaped"
        );
        Ok(())
    })
}
