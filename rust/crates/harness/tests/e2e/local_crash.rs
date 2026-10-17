//! Process interruption belongs to the shared LocalCase driver, not a second harness.
use super::TestResult;
use acyclic_harness::{
    LocalHarness, TaskId,
    context::CompactionPolicy,
    conversation::VolumeRef,
    model::{Model, ModelRequest},
    task_volume::{Change, Cursor, FileKey, Query, TaskFile},
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

const MARKER: &str = "GC_STREAM_CHECKPOINT ";

#[derive(Serialize, Deserialize)]
pub(super) struct Input {
    pub directory: PathBuf,
    pub state: PathBuf,
    pub volume: VolumeRef,
    pub task: TaskId,
    pub turn: ScriptedTurn,
    pub query: Query,
    pub cursor: Cursor,
    pub after: usize,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Checkpoint {
    pub changes: Vec<Change>,
    pub requests: Vec<ModelRequest>,
}

pub(super) async fn interrupt(path: PathBuf, input: Input) -> TestResult<Checkpoint> {
    std::fs::write(&path, serde_json::to_vec(&input)?)?;
    tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command
            .args(["--exact", "scenarios", "--nocapture"])
            .env("GC_STREAM_CHILD", path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn()?;
        let result = (|| {
            let output = child.stdout.take().ok_or("child stdout missing")?;
            for line in std::io::BufReader::new(output).lines() {
                let line = line?;
                if let Some((_, checkpoint)) = line.split_once(MARKER) {
                    return Ok(serde_json::from_str::<Checkpoint>(checkpoint)?);
                }
            }
            Err("child exited before the file-follow checkpoint".into())
        })();
        let killed = child.kill();
        let status = child.wait()?;
        killed?;
        if status.success() {
            return Err("interrupted process exited successfully".into());
        }
        result
    })
    .await?
}

pub(super) async fn child(path: &Path) -> TestResult {
    let input: Input = serde_json::from_slice(&std::fs::read(path)?)?;
    let pending = Arc::new(tokio::sync::Notify::new());
    let signal = pending.clone();
    let provider = Arc::new(
        ScriptedModelProvider::new([input.turn]).with_pending_observer(move || signal.notify_one()),
    );
    let model = Model::new(
        "scripted".into(),
        "local".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(
        local_harness_config(&input.directory, (model, provider.clone()))?
            .state_directory(&input.state)
            .volume(input.volume)
            .compaction(CompactionPolicy::Disabled)
            .goal_continuations(None),
    )
    .await?;
    let mut observation = harness
        .task_files()
        .observe(input.query, Some(input.cursor), 128)
        .await?;
    if observation.reset || observation.snapshot.is_some() {
        return Err("child reset its inherited follower".into());
    }
    let mut changes = Vec::new();
    let mut seen = 0;
    let mut reached_pending = false;
    let mut execution = std::pin::pin!(harness.resume(input.task));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            tokio::select! {
                output = &mut execution => return Err(format!("execution ended before kill: {output:?}").into()),
                _ = pending.notified(), if !reached_pending => reached_pending = true,
                change = observation.changes.next() => {
                    let change = change.ok_or("child file follower ended")??;
                    if change.file.as_ref().is_some_and(|file| matches!(file.key,
                        FileKey::Task { file: TaskFile::Chunk { .. }, .. })) {
                        seen += 1;
                    }
                    changes.push(change);
                }
            }
            if seen == input.after && reached_pending {
                let checkpoint = Checkpoint { changes, requests: provider.requests() };
                let mut stdout = std::io::stdout().lock();
                writeln!(stdout, "{MARKER}{}", serde_json::to_string(&checkpoint)?)?;
                stdout.flush()?;
                // The source barrier follows the executor's awaited publications.
                // Keep that exact future alive until the parent kills this process.
                return std::future::pending::<TestResult>().await;
            }
        }
    }).await?
}
