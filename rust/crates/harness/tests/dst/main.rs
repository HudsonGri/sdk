//! Seeded production replays: compare bytes, never rewrite identities or errors.
use acyclic_harness::{
    Error, LocalHarness, Result,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::FilesystemHost,
    model::Model,
    resources::ProviderRef,
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use acyclic_native_runtime::testing::{DurableWrite, Schedule};
use std::sync::Arc;
use std::{collections::BTreeMap, path::Path};

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[derive(Debug, PartialEq, Eq)]
struct Trace {
    polls: Vec<(u64, &'static str)>,
    boundaries: Vec<(String, String)>,
    writes: Vec<DurableWrite>,
    requests: Vec<u8>,
    dispatches: Vec<u8>,
    files: Vec<u8>,
    durable: Vec<u8>,
}

fn same_trace(left: &Trace, right: &Trace) -> std::result::Result<(), String> {
    if left.polls != right.polls {
        return Err("seeded task polling diverged".into());
    }
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
    if left.writes != right.writes {
        let index = left
            .writes
            .iter()
            .zip(&right.writes)
            .position(|(left, right)| left != right)
            .unwrap_or(left.writes.len().min(right.writes.len()));
        return Err(format!(
            "durable operation {index}: {:?} != {:?}",
            left.writes.get(index),
            right.writes.get(index)
        ));
    }
    for (name, left, right) in [
        ("requests", &left.requests, &right.requests),
        ("dispatches", &left.dispatches, &right.dispatches),
        ("files", &left.files, &right.files),
        ("durable", &left.durable, &right.durable),
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

fn durable_bytes(directory: &Path) -> Result<Vec<u8>> {
    let mut pending = vec![directory.to_path_buf()];
    let mut files = BTreeMap::new();
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path).map_err(storage)? {
            let entry = entry.map_err(storage)?;
            let path = entry.path();
            let kind = entry.file_type().map_err(storage)?;
            if kind.is_dir() {
                pending.push(path);
            } else if kind.is_file() {
                let name = path.strip_prefix(directory).map_err(storage)?;
                let name = name
                    .to_str()
                    .ok_or_else(|| Error::Invalid("non-UTF-8 durable path".into()))?;
                files.insert(name.to_owned(), std::fs::read(&path).map_err(storage)?);
            } else {
                return Err(Error::Invalid("unexpected durable entry".into()));
            }
        }
    }
    serde_json::to_vec(&files).map_err(storage)
}

async fn scenario() -> Result<(tempfile::TempDir, Vec<u8>, Vec<u8>, Vec<u8>)> {
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
    let provider = Arc::new(ScriptedModelProvider::new([
        ScriptedTurn::Text("done".into()),
        ScriptedTurn::Text("reopened".into()),
    ]));
    let config = || -> Result<_> {
        Ok(local_harness_config(
            directory.path().join("filesystem"),
            (model.clone(), provider.clone()),
        )?
        .volume(volume.clone())
        .state_directory(directory.path().join("state")))
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
    assert_eq!(
        reopened.run("submit after reopening").await?.text,
        "reopened"
    );
    let files = reopened
        .task_files()
        .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
        .await
        .map_err(storage)?
        .snapshot
        .ok_or_else(|| Error::Invalid("missing final snapshot".into()))?
        .files;
    Ok((
        directory,
        serde_json::to_vec(&provider.requests()).map_err(storage)?,
        serde_json::to_vec(&provider.dispatches()).map_err(storage)?,
        serde_json::to_vec(&files).map_err(storage)?,
    ))
}

async fn replay(seed: u64, erase_source: bool) -> Result<Trace> {
    // Negative control removes seeded execution, preserving real barriers.
    let (schedule, mut events) = if erase_source {
        Schedule::new()
    } else {
        Schedule::seeded(seed)
    };
    let mut turn = Box::pin(schedule.scope(scenario()));
    let mut boundaries = Vec::new();
    let (directory, requests, dispatches, files) = loop {
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
    drop(turn);
    loop {
        tokio::select! {
            biased;
            event = events.recv() => {
                let event = event.ok_or_else(|| Error::Invalid("lost drain driver".into()))?;
                boundaries.push((event.name.into(), event.detail.to_string()));
                event.release();
            },
            () = schedule.drained() => break,
        }
    }
    Ok(Trace {
        polls: schedule.polls(),
        boundaries,
        writes: schedule.durable_writes(),
        requests,
        dispatches,
        files,
        durable: durable_bytes(directory.path())?,
    })
}

#[test]
fn seeded_production_turn_replays_exact_bytes() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let first = replay(71, false).await?;
        let second = replay(71, false).await?;
        same_trace(&first, &second).expect("same seed diverged");
        assert!(
            !first.boundaries.is_empty(),
            "production boundaries were bypassed"
        );
        assert!(
            first.writes.iter().any(|write| write.requested > 0),
            "original durable writes were bypassed"
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
