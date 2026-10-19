use super::*;
use crate::task_volume::Query;
use crate::{Admission, LocalHarness, model::Model, test_support::ScriptedModelProvider};

#[tokio::test]
async fn client_goal_write_between_bash_chunks_preserves_the_command() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let model = Model::new(
        "scripted".into(),
        "output".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(
            directory.path(),
            (model, Arc::new(ScriptedModelProvider::new([]))),
        )?
        .state_directory(state.path()),
    )
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "bash output").await?
    else {
        panic!("durable admission");
    };
    let worker = crate::distributed::Worker {
        id: "output-test".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        panic!("task claim");
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    let task = Arc::new(
        harness
            .runtime
            .task_context(
                task_id,
                crate::scheduler::LeaseFence::from(&lease.reservation),
            )
            .await?,
    );
    let context = ToolContext::new(task, OperationId::new(), "bash-call", None)?;
    let mut output =
        Output::open(&context, Arc::clone(harness.task_files()), b"bash".to_vec()).await?;
    output = tokio::task::spawn_blocking(move || {
        output.observe(ProcessOutput::Stdout(b"before"))?;
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    let files = harness.task_files();
    let snapshot = files
        .observe(Query::Root, None, 16)
        .await?
        .snapshot
        .unwrap();
    files
        .write(
            snapshot.generation,
            &Write::new(FileKey::Goal, b"client goal".to_vec()),
        )
        .await?;
    output = tokio::task::spawn_blocking(move || {
        output.observe(ProcessOutput::Stdout(b"after"))?;
        Ok::<_, std::io::Error>(output)
    })
    .await
    .map_err(storage)?
    .map_err(storage)?;
    output
        .finish(&ToolResult {
            value: serde_json::json!({"success":true}),
        })
        .await?;
    let snapshot = files
        .observe(Query::Root, None, 16)
        .await?
        .snapshot
        .unwrap();
    assert!(
        snapshot
            .files
            .iter()
            .any(|file| file.key == FileKey::Goal && file.bytes.as_ref() == b"client goal")
    );
    let chunks: Vec<_> = snapshot
        .files
        .iter()
        .filter(|file| {
            matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::Tool {
                        file: ToolFile::Stdout(_),
                        ..
                    },
                    ..
                }
            )
        })
        .map(|file| file.bytes.as_ref())
        .collect();
    assert_eq!(chunks, [b"before".as_slice(), b"after".as_slice()]);
    harness.disconnect_unfinished().await?;
    Ok(())
}

#[tokio::test]
async fn text_and_native_tool_files_share_the_full_canonical_model_turn() -> Result<()> {
    use crate::{
        model::ModelEvent,
        test_support::{ScriptedEvent, ScriptedTurn},
    };
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let model = Model::new(
        "scripted".into(),
        "grouped-output".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let provider = Arc::new(ScriptedModelProvider::new([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "before-tool".into(),
            }),
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "canonical-tool".into(),
                "bash".into(),
                serde_json::json!({"command":"printf tool-bytes"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ]),
        ScriptedTurn::Text("after-tool".into()),
    ]));
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model, provider))?
            .state_directory(state.path()),
    )
    .await?;
    harness.run("group model output").await?;
    let snapshot = harness
        .task_files()
        .observe(Query::Root, None, 128)
        .await?
        .snapshot
        .unwrap();
    let (task, turn) = snapshot
        .files
        .iter()
        .find_map(|file| match &file.key {
            FileKey::Task {
                task,
                file: TaskFile::Chunk { turn, .. },
            } if file.bytes.as_ref() == b"before-tool" => Some((task.clone(), *turn)),
            _ => None,
        })
        .expect("canonical text projection");
    let tools: Vec<_> = snapshot
        .files
        .iter()
        .filter_map(|file| match &file.key {
            FileKey::Task {
                task: owner,
                file: TaskFile::Tool { turn, file, .. },
            } if owner == &task => Some((*turn, file)),
            _ => None,
        })
        .collect();
    assert!(!tools.is_empty());
    assert!(tools.iter().all(|(tool_turn, _)| *tool_turn == turn));
    assert!(
        tools
            .iter()
            .any(|(_, file)| matches!(file, ToolFile::Request))
    );
    assert!(
        tools
            .iter()
            .any(|(_, file)| matches!(file, ToolFile::Stdout(_)))
    );
    assert!(
        tools
            .iter()
            .any(|(_, file)| matches!(file, ToolFile::Result))
    );
    let selected = harness
        .task_files()
        .observe(
            Query::Turn {
                task: task.clone(),
                turn,
            },
            None,
            128,
        )
        .await?
        .snapshot
        .unwrap();
    assert!(
        selected
            .files
            .iter()
            .any(|file| file.bytes.as_ref() == b"before-tool")
    );
    assert!(selected.files.iter().any(|file| matches!(
        file.key,
        FileKey::Task {
            file: TaskFile::Tool { .. },
            ..
        }
    )));
    assert!(
        !selected
            .files
            .iter()
            .any(|file| file.bytes.as_ref() == b"after-tool")
    );
    harness.disconnect().await
}
