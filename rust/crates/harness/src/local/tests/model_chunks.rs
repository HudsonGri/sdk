use super::*;
use crate::task_volume::{Channel, Component, Query, TaskFile};
use futures::StreamExt as _;
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    name: String,
    turns: Vec<ScriptedTurn>,
    text: String,
    chunks: Vec<(Channel, String)>,
}

#[tokio::test]
async fn stock_model_deltas_are_exact_task_files_followed_across_reopen() -> Result<()> {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("model_chunks.json")).map_err(super::super::storage)?;
    for case in cases {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let turn_count = case.turns.len();
        let provider = scripted(case.turns);
        let config = volume_config(directory.path(), project.clone(), provider.clone())?
            .without_default_tools();
        let harness = LocalHarness::open(config).await?;
        let operation = OperationId::new();
        let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
            harness.submit(operation, "stream the response").await?
        else {
            panic!("{}: prompt was rejected", case.name);
        };
        let query = Query::Task(Component::new(task_id.to_string())?);
        let mut observed = harness
            .task_files()
            .observe(query.clone(), None, 64)
            .await?;
        assert!(
            observed
                .snapshot
                .take()
                .unwrap()
                .files
                .iter()
                .all(|file| !matches!(
                    file.key,
                    crate::task_volume::FileKey::Task {
                        file: TaskFile::Chunk { .. },
                        ..
                    }
                ))
        );
        let output = harness.resume(task_id).await?;
        assert_eq!(output.text, case.text, "{}", case.name);
        let mut actual = Vec::new();
        let mut cursor = None;
        let mut model_headers = 0;
        while actual.len() < case.chunks.len() {
            let change =
                tokio::time::timeout(std::time::Duration::from_secs(1), observed.changes.next())
                    .await
                    .map_err(super::super::storage)?
                    .ok_or_else(|| Error::Storage("chunk follow ended".into()))??;
            if let Some(file) = &change.file
                && matches!(
                    file.key,
                    crate::task_volume::FileKey::Task {
                        file: TaskFile::TurnRequest(_),
                        ..
                    }
                )
            {
                let header: Value =
                    serde_json::from_slice(&file.bytes).map_err(super::super::storage)?;
                assert_eq!(
                    header["model"],
                    json!({"provider":"scripted","name":"test","revision":"1"})
                );
                assert!(header["request"].is_object());
                model_headers += 1;
            }
            if let Some(file) = change.file
                && let crate::task_volume::FileKey::Task {
                    file: TaskFile::Chunk { .. },
                    ..
                } = &file.key
            {
                actual.push((
                    file.channel.unwrap(),
                    String::from_utf8(file.bytes.to_vec()).map_err(super::super::storage)?,
                ));
            }
            cursor = Some(change.cursor);
        }
        assert_eq!(actual, case.chunks, "{}", case.name);
        assert_eq!(model_headers, turn_count);
        assert_eq!(provider.remaining(), 0);
        assert_eq!(provider.requests().len(), turn_count);
        drop(observed);
        drop(harness);
        let recovered_provider = scripted([]);
        let recovered = LocalHarness::open(
            volume_config(directory.path(), project, recovered_provider.clone())?
                .without_default_tools(),
        )
        .await?;
        assert_eq!(recovered.resume(task_id).await?, output);
        let mut position = cursor.as_ref().unwrap().position;
        let mut resumed = recovered.task_files().observe(query, cursor, 64).await?;
        assert!(!resumed.reset);
        assert!(resumed.snapshot.is_none());
        let settled = recovered.task_files().inner.head().await?.position;
        while position < settled {
            let change =
                tokio::time::timeout(std::time::Duration::from_secs(1), resumed.changes.next())
                    .await
                    .map_err(super::super::storage)?
                    .ok_or_else(|| Error::Storage("resumed chunk follow ended".into()))??;
            assert!(
                !change.file.is_some_and(|file| matches!(
                    file.key,
                    crate::task_volume::FileKey::Task {
                        file: TaskFile::Chunk { .. },
                        ..
                    }
                )),
                "replayed a delivered chunk"
            );
            position = change.cursor.position;
        }
        assert!(recovered_provider.requests().is_empty());
    }
    Ok(())
}
