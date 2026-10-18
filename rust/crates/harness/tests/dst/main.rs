//! Seeded production replays: compare bytes, never rewrite identities or errors.
use acyclic_harness::{
    Error, LocalHarness, Result, client,
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
    jsonl: Vec<u8>,
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
        ("jsonl", &left.jsonl, &right.jsonl),
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

async fn prompt(
    harness: &Arc<LocalHarness>,
    prompt: &str,
    expected: &str,
    headless: bool,
) -> Result<Vec<u8>> {
    if !headless {
        assert_eq!(harness.run(prompt).await?.text, expected);
        return Ok(Vec::new());
    }
    // The same identity, request-file, client and finish path used by graphcoder -p.
    let operation = client::local_identity(harness, Some(prompt), None).await?;
    let command = client::prepare_local_prompt(operation, prompt)?;
    let (mut connection, target) = client::connect_local(harness.clone(), operation).await?;
    let mut jsonl = Vec::new();
    let outcome = client::run_headless(&mut connection, command, target, &mut jsonl).await;
    assert_eq!(connection.finish(outcome).await?.exit_code(), 0);
    assert_eq!(jsonl.last(), Some(&b'\n'), "incomplete JSONL frame");
    let text = std::str::from_utf8(&jsonl).map_err(storage)?;
    let mut lines = text.lines();
    client::jsonl::parse_snapshot(lines.next().expect("missing initial frame"))?;
    let mut completed = false;
    for line in lines {
        let change = client::jsonl::parse_change(line)?;
        for file in change.file.iter().chain(&change.files) {
            if matches!(
                file.key,
                acyclic_harness::task_volume::FileKey::Session {
                    file: acyclic_harness::task_volume::TaskFile::ExecutionResult(_),
                    ..
                }
            ) {
                let outcome: serde_json::Value =
                    serde_json::from_slice(&file.bytes).map_err(storage)?;
                assert_eq!(
                    outcome["ok"]["text"], expected,
                    "final text differs from provider output"
                );
                completed = true;
            }
        }
    }
    assert!(completed, "JSONL omitted the terminal outcome");
    Ok(jsonl)
}

async fn scenario(
    headless: bool,
) -> Result<(tempfile::TempDir, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)> {
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
    let harness = Arc::new(LocalHarness::open(config()?).await?);
    let mut jsonl = prompt(&harness, "finish this turn", "done", headless).await?;
    let files = harness
        .task_files()
        .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
        .await
        .map_err(storage)?
        .snapshot
        .ok_or_else(|| Error::Invalid("missing snapshot".into()))?
        .files;
    drop(harness);
    let reopened = Arc::new(LocalHarness::open(config()?).await?);
    let restored = reopened
        .task_files()
        .observe(acyclic_harness::task_volume::Query::Root, None, 4096)
        .await
        .map_err(storage)?
        .snapshot
        .ok_or_else(|| Error::Invalid("missing reopened snapshot".into()))?
        .files;
    assert_eq!(files, restored, "cold reopen lost acknowledged files");
    jsonl.extend(prompt(&reopened, "submit after reopening", "reopened", headless).await?);
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
        jsonl,
    ))
}

async fn replay(seed: u64, erase_source: bool, headless: bool) -> Result<Trace> {
    // Negative control removes seeded execution, preserving real barriers.
    let (schedule, mut events) = if erase_source {
        Schedule::new()
    } else {
        Schedule::seeded(seed)
    };
    let mut turn = Box::pin(schedule.scope(scenario(headless)));
    let mut boundaries = Vec::new();
    let (directory, requests, dispatches, files, jsonl) = loop {
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
        jsonl,
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
        let first = replay(71, false, false).await?;
        let second = replay(71, false, false).await?;
        same_trace(&first, &second).expect("same seed diverged");
        assert!(
            !first.boundaries.is_empty(),
            "production boundaries were bypassed"
        );
        assert!(
            first.writes.iter().any(|write| write.requested > 0),
            "original durable writes were bypassed"
        );
        let different = replay(72, false, false).await?;
        assert_ne!(
            first.dispatches, different.dispatches,
            "seed did not reach production identities"
        );
        let broken = replay(71, true, false).await?;
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

#[test]
fn seeded_headless_entrypoint_replays_complete_jsonl() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        tokio::time::pause();
        let first = replay(71, false, true).await?;
        let second = replay(71, false, true).await?;
        same_trace(&first, &second).expect("headless same seed diverged");
        assert!(!first.jsonl.is_empty(), "headless output bypassed");
        Ok(())
    })
}
