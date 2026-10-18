//! An accepted cancellation covers native preparation as well as ordinary tools.
use super::*;
use crate::{
    external_executor::{ExternalHost, request},
    runtime::TaskStateProvider as _,
};
use serde_json::json;

#[tokio::test]
async fn cancelled_native_launch_admits_no_child_and_keeps_the_parent_usable() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let harness = LocalHarness::open(LocalHarnessConfig::external(directory.path())).await?;
    let session = "cancelled-native-preparation";
    let parent = request(
        ExternalHost::Pi,
        "session_start",
        json!({"sessionId":session,"cwd":directory.path()}),
    )?;
    harness.external_request(&parent).await?;
    let cancel = request(
        ExternalHost::Pi,
        "tool_cancel",
        json!({"sessionId":session,"toolCallId":"spawn"}),
    )?;
    let child_session = uuid::Uuid::new_v4();
    let spawn = request(
        ExternalHost::Pi,
        "tool_call",
        json!({
            "sessionId":session,"toolCallId":"spawn","toolName":"acyclic_spawn",
            "input":{"id":"never-started","prompt":"must not execute","executor":{
                "session":{"host":"pi","session_id":child_session,"caller":null},
                "executable":std::env::current_exe().map_err(storage)?,
                "arguments":[{"kind":"prompt"}],"timeout_ms":1000,
                "session_bytes":json!({"type":"session","version":3,"id":child_session,
                    "timestamp":"2026-10-18T00:00:00Z","cwd":directory.path()}).to_string()
            }}
        }),
    )?;
    // Publish the accepted cancellation directly to the request volume before
    // driving this call. There is no race or sleep hiding an untested branch.
    let volumes = harness.external_volumes(parent.task_id()).await?;
    let task = Component::new(parent.task_id().to_string()).map_err(storage)?;
    volumes
        .requests
        .append(
            volumes.requests.head().await.map_err(storage)?,
            &cancel.to_file(task.clone()).map_err(storage)?,
        )
        .await
        .map_err(storage)?;
    let rejected = harness.external_request(&spawn).await?;
    assert_eq!(
        rejected,
        json!({"Failed":{"message":"native tool cancelled before execution"}})
    );
    assert_eq!(harness.external_request(&spawn).await?, rejected);
    assert!(
        harness
            .runtime
            .harness()
            .children(parent.task_id(), None, None, 8)
            .await?
            .entries
            .is_empty()
    );
    let acknowledgement = volumes
        .results
        .snapshot(&Query::File(cancel.result_key(task).map_err(storage)?), 1)
        .await
        .map_err(storage)?;
    assert_eq!(
        acknowledgement.files.len(),
        1,
        "cancellation must settle the original call"
    );

    let write = request(
        ExternalHost::Pi,
        "tool_call",
        json!({
            "sessionId":session,"toolCallId":"after-cancel","toolName":"write",
            "input":{"path":"after.txt","text":"parent still owns its invocation", "media_type":"text/plain","display_name":"after.txt"}
        }),
    )?;
    crate::external_executor::tool_result(harness.external_request(&write).await?)?;
    assert_eq!(
        std::fs::read_to_string(directory.path().join("after.txt")).map_err(storage)?,
        "parent still owns its invocation"
    );
    drop(volumes);
    harness.close().await
}
