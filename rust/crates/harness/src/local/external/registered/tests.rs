use super::*;
use crate::external_executor::{ExternalHost, request};
use crate::test_support::admission_work::measure;
use serde_json::json;

#[tokio::test]
async fn unrelated_task_changes_do_not_repeat_worker_or_view_work() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let project = root.path().join("project");
    std::fs::create_dir(&project).map_err(storage)?;
    let harness = LocalHarness::open(LocalHarnessConfig::external(&project)).await?;
    let attach = |session: &str| {
        request(
            ExternalHost::Pi,
            "session_start",
            json!({"sessionId":session,"cwd":project}),
        )
    };
    let tracked = attach("tracked")?;
    harness.external_request(&tracked).await?;
    let (first, reads) = measure(harness.registered_step(tracked.task_id(), None)).await;
    let (cursor, terminal) = first?;
    assert!(
        reads > 0,
        "first progress must run the real owner/view path"
    );
    assert_eq!(terminal, None);
    harness.external_request(&attach("unrelated")?).await?;
    let (unchanged, reads) = measure(harness.registered_step(tracked.task_id(), cursor)).await;
    assert_eq!(unchanged?, (cursor, None));
    assert_eq!(
        reads, 0,
        "unrelated scheduler records repeated admission/view work"
    );
    let end = request(
        ExternalHost::Pi,
        "session_shutdown",
        json!({"sessionId":"tracked","cwd":project}),
    )?;
    harness.external_request(&end).await?;
    let (changed, reads) = measure(harness.registered_step(tracked.task_id(), cursor)).await;
    let (updated, terminal) = changed?;
    assert_ne!(updated, cursor);
    assert_eq!(terminal, Some(()));
    assert!(
        reads > 0,
        "own terminal change must refresh canonical views"
    );
    harness.close().await
}
