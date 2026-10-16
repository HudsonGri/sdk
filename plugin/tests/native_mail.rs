//! Stock mail and inspection through every host's shared hook handler.
#[path = "support/native_actions.rs"]
mod native_actions;
#[path = "support/native_hooks.rs"]
mod native_hooks;
use acyclic_harness::TaskId;
use native_actions::Tools;
use native_hooks::{Fixture, Result};
use serde_json::json;

#[tokio::test]
async fn mailbox_and_inspection_commands_use_authenticated_task_files_for_every_host() -> Result {
    let fixture = Fixture::new()?;
    let owner = fixture.start_owner().await?;
    let result = async {
        let [codex, claude, pi] = Tools::hosts(fixture.path()).await?;
        tokio::try_join!(
            mailbox(&fixture, codex),
            mailbox(&fixture, claude),
            mailbox(&fixture, pi)
        )
    }
    .await;
    fixture.close_owner(owner).await?;
    result?;
    Ok(())
}

async fn mailbox(fixture: &Fixture, tools: Tools<'_>) -> Result {
    let host = tools.host;
    let child = tools.child("mail-child").await?;
    let filename = format!("{}-message.txt", host.name());
    let written = tools
            .call(
                "message",
                "write",
                json!({"path":filename,"text":"mail payload","media_type":"text/plain","display_name":filename}),
            )
            .await?;
    let file = written
        .get("file")
        .ok_or("write did not return a pinned file")?;
    for (after, name) in ["acyclic_send", "acyclic_steer"].into_iter().enumerate() {
        assert!(
            tools
                .call(name, name, json!({"recipient":child.task,"payload":file}))
                .await?
                .is_null()
        );
        let item = child
            .call(
                &format!("receive-{after}"),
                "acyclic_receive",
                json!({"after":after}),
            )
            .await?;
        assert_eq!(item["sequence"], after + 1);
        assert_eq!(item["payload"], *file);
    }
    child
        .call(
            "reply",
            "acyclic_send",
            json!({"recipient":tools.task,"payload":file}),
        )
        .await?;
    let reply = tools
        .call("reply", "acyclic_receive", json!({"after":0}))
        .await?;
    assert_eq!(reply["payload"], *file);
    let inspected = tools
        .call("inspect", "acyclic_inspect", json!({"task":tools.task}))
        .await?;
    assert_eq!(inspected["task"], tools.task.to_string());
    assert!(inspected["admission"]["parent"].is_null());
    let unknown = json!({"task":TaskId::from_bytes([253;16])});
    let failed = tools
        .reject("unknown", "acyclic_inspect", unknown.clone())
        .await?;
    assert_eq!(
        tools.reject("unknown", "acyclic_inspect", unknown).await?,
        failed
    );
    let next = format!("{}-after-rejection.txt", host.name());
    tools
            .call(
                "after-rejection",
                "write",
                json!({"path":next,"text":"session usable","media_type":"text/plain","display_name":next}),
            )
            .await?;
    assert_eq!(
        std::fs::read_to_string(fixture.path().join(filename))?,
        "mail payload"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.path().join(next))?,
        "session usable"
    );
    Ok(())
}
