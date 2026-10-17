use super::TestResult;
use acyclic_harness::{
    LocalHarness, OperationId,
    client::{ClientViewLimits, ClientViews, FileQueryView, QueryViewLimits},
    task_volume::{Change, Cursor, FileKey, FileValue, Observation, Query, TaskFile, ToolFile},
};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) async fn check(
    harness: &LocalHarness,
    query: Query,
    mut observation: Observation,
    prefix: Vec<Change>,
    expected: &[Value],
    operation: OperationId,
) -> TestResult<Cursor> {
    let initial = observation
        .snapshot
        .take()
        .ok_or("missing stream snapshot")?;
    let mut position = initial.cursor.position;
    let mut cursor = initial.cursor.clone();
    let identity = (cursor.volume, cursor.query);
    let mut views = ClientViews::new(
        acyclic_stream::StreamPath::new("chunks")?,
        ClientViewLimits {
            agents: 1,
            selected: 1,
            overview_values: 1,
            overview_bytes: 128,
            query: QueryViewLimits {
                files: 128,
                segments: 128,
                bytes: 1_048_576,
            },
        },
    )?;
    let settled = harness
        .task_files()
        .observe(query.clone(), None, 128)
        .await?
        .snapshot
        .ok_or("missing settled stream snapshot")?;
    let cut = settled.cursor.position;
    let mut turns = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut actual = Vec::new();
    let mut prefix = prefix.into_iter();
    for file in &initial.files {
        if normalize(file.clone(), &mut turns, &mut calls, operation)?.is_some() {
            return Err("streaming started before the admission snapshot".into());
        }
    }
    views.query_snapshot(query.clone(), initial, 128, 1_048_576)?;
    while position < cut {
        let change = match prefix.next() {
            Some(change) => change,
            None => observation
                .changes
                .next()
                .await
                .ok_or("task file follow ended")??,
        };
        if change.cursor.position <= position || change.cursor.position > cut {
            return Err("task file cursor reordered a publication".into());
        }
        views.query_change(Change {
            file: change.file.clone(),
            files: change.files.clone(),
            removed: change.removed.clone(),
            cursor: change.cursor.clone(),
        })?;
        let selection = views
            .selection(&identity)
            .ok_or("client lost its task query")?;
        if selection.cursor() != &change.cursor {
            return Err("client did not consume the exact follower cursor".into());
        }
        position = change.cursor.position;
        cursor = change.cursor.clone();
        if let Some(file) = change.file
            && let Some(file) = normalize(file, &mut turns, &mut calls, operation)?
        {
            actual.push(file);
        }
        // Every delivery crosses a real disconnect/reconnect boundary. Resuming
        // uses the consumed cursor, never a newly sampled publication head.
        drop(observation);
        observation = harness
            .task_files()
            .observe(query.clone(), Some(change.cursor), 128)
            .await?;
        if observation.reset || observation.snapshot.is_some() {
            return Err("task stream cursor could not resume".into());
        }
    }
    let snapshot_view = FileQueryView::from_snapshot(query, settled, 128, 1_048_576)?;
    if views
        .selection(&identity)
        .ok_or("client lost its task query")?
        .files()
        != snapshot_view.files()
    {
        return Err("client snapshot and resumed file fold differ".into());
    }
    if actual != expected {
        return Err(format!("task files differ: expected {expected:?}, got {actual:?}").into());
    }
    Ok(cursor)
}

pub(super) async fn no_replay(harness: &LocalHarness, query: Query, cursor: Cursor) -> TestResult {
    let cut = harness
        .task_files()
        .observe(query.clone(), None, 128)
        .await?
        .snapshot
        .ok_or("missing recovered stream snapshot")?
        .cursor
        .position;
    let mut position = cursor.position;
    let mut observation = harness
        .task_files()
        .observe(query, Some(cursor), 128)
        .await?;
    if observation.reset || observation.snapshot.is_some() {
        return Err("cold reopen reset a valid stream cursor".into());
    }
    while position < cut {
        let change = observation
            .changes
            .next()
            .await
            .ok_or("recovered task follow ended")??;
        position = change.cursor.position;
        if change.file.is_some_and(|file| {
            matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::Error { .. }
                        | TaskFile::TurnRequest(_)
                        | TaskFile::Chunk { .. }
                        | TaskFile::Tool { .. }
                        | TaskFile::Completion(_),
                    ..
                }
            )
        }) {
            return Err("completed task replayed a stream file".into());
        }
    }
    Ok(())
}

fn normalize(
    file: FileValue,
    turns: &mut BTreeMap<u64, usize>,
    calls: &mut BTreeMap<String, usize>,
    operation: OperationId,
) -> TestResult<Option<Value>> {
    let FileKey::Task { file: kind, .. } = &file.key else {
        return Ok(None);
    };
    let turn = match kind {
        TaskFile::TurnRequest(turn) => {
            let next = turns.len();
            let turn = *turns.entry(*turn).or_insert(next);
            let header: Value = serde_json::from_slice(&file.bytes)?;
            if !header["request"].is_object() {
                return Err("model request lost its prepared file reference".into());
            }
            return Ok(Some(
                json!({"path":format!("turns/{turn}/request.json"),"model":header["model"]}),
            ));
        }
        TaskFile::Chunk { turn, .. }
        | TaskFile::Completion(turn)
        | TaskFile::Error { turn, .. }
        | TaskFile::Tool { turn, .. } => *turn,
        _ => return Ok(None),
    };
    let turn = turns
        .get(&turn)
        .ok_or("stream file preceded its model request")?;
    let path = match kind {
        TaskFile::Chunk { sequence, .. } => format!("turns/{turn}/chunks/{sequence}"),
        TaskFile::Completion(_) => format!("turns/{turn}/completion.json"),
        TaskFile::Error { sequence, .. } => format!("turns/{turn}/errors/{sequence}.json"),
        TaskFile::Tool { call, .. } => {
            let next = calls.len();
            let call = calls.entry(call.as_str().to_owned()).or_insert(next);
            let suffix = file
                .key
                .path()
                .split("/tools/")
                .nth(1)
                .ok_or("tool path")?
                .split_once('/')
                .ok_or("tool file path")?
                .1
                .to_owned();
            format!("turns/{turn}/tools/{call}/{suffix}")
        }
        _ => unreachable!(),
    };
    let mut text = std::str::from_utf8(&file.bytes)?.to_owned();
    if matches!(kind, TaskFile::Error { .. }) {
        text = text.replace(&operation.to_string(), "<operation>");
    }
    if let TaskFile::Tool {
        call,
        file: ToolFile::Request,
        ..
    } = kind
    {
        let invocation: acyclic_harness::tool::ToolInvocation =
            serde_json::from_slice(&file.bytes)?;
        if invocation.operation_id.to_string() != call.as_str() {
            return Err("tool request identity differs from its file".into());
        }
        text = text.replace(call.as_str(), "<operation>");
    }
    Ok(Some(
        json!({"path":path,"channel":file.channel,"text":text}),
    ))
}
