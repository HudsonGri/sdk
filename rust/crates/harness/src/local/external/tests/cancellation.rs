//! An accepted cancellation covers native preparation as well as ordinary tools.
use super::*;
use crate::external_executor::{ExternalHost, request};
use serde_json::json;

#[tokio::test]
async fn cancelled_native_launch_admits_no_child_and_keeps_the_parent_usable() -> Result<()> {
    cancelled_launch(None).await
}

#[tokio::test]
async fn cancelled_native_preparation_budget_settles_original_call_and_acknowledgement()
-> Result<()> {
    cancelled_launch(Some("budget")).await
}

#[tokio::test]
async fn cancelled_native_partial_preparation_reconciles_before_acknowledgement() -> Result<()> {
    cancelled_launch(Some("preparation")).await
}

#[tokio::test]
async fn cancelled_native_running_preparation_releases_its_drained_lease() -> Result<()> {
    cancelled_launch(Some("running")).await
}

#[tokio::test]
async fn cancelled_native_admitted_child_is_cancelled_before_acknowledgement() -> Result<()> {
    cancelled_launch(Some("child")).await
}

async fn cancelled_launch(point: Option<&'static str>) -> Result<()> {
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
    let volumes = harness.external_volumes(parent.task_id()).await?;
    let task = Component::new(parent.task_id().to_string()).map_err(storage)?;
    let publish_cancel = async {
        volumes
            .requests
            .append(
                volumes.requests.head().await.map_err(storage)?,
                &cancel.to_file(task.clone()).map_err(storage)?,
            )
            .await
            .map_err(storage)?;
        Ok::<(), Error>(())
    };
    let rejected = if let Some(point) = point {
        // Pause the shipping admission path only at the chosen boundary. The
        // accepted cancellation file must wake its existing push follow and
        // cancel the actual invocation token before preparation continues.
        let reached = super::super::super::children::cancel_at(point);
        let (result, cancelled) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(harness.external_request(&spawn), async {
                reached.await.map_err(storage)?;
                publish_cancel.await
            })
        })
        .await
        .expect("preparation cancellation must settle through the request handler");
        cancelled?;
        result?
    } else {
        publish_cancel.await?;
        harness.external_request(&spawn).await?
    };
    assert_eq!(
        rejected,
        json!({"Failed":{"message":if point.is_some() {
            "native tool cancelled during execution"
        } else {
            "native tool cancelled before execution"
        }}})
    );
    assert_eq!(harness.external_request(&spawn).await?, rejected);
    let descendants = harness
        .runtime
        .harness()
        .children(parent.task_id(), None, None, 8)
        .await?;
    assert_eq!(
        descendants.entries.len(),
        match point {
            Some("preparation" | "running") => 1,
            Some("child") => 2,
            _ => 0,
        }
    );
    for descendant in descendants.entries {
        let terminal = harness
            .runtime
            .task_host()
            .terminal_status(descendant.task_id)
            .await?;
        if point == Some("running") {
            assert_eq!(
                terminal,
                Some(crate::Outcome::Failed {
                    message: "fork preparation cancelled; allocation may remain uncertain".into(),
                })
            );
        } else {
            assert!(matches!(
                terminal,
                Some(crate::Outcome::Succeeded(()) | crate::Outcome::Cancelled)
            ));
        }
    }
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
