//! Run with the complete parallel `local::` module. The old `pre_exec` path
//! invokes this host's atfork handler even when the command itself completes;
//! this catches the allocator-lock convoy without relying on machine speed.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

static HOST_FORKS: AtomicUsize = AtomicUsize::new(0);

extern "C" fn preparing_host_fork() {
    HOST_FORKS.fetch_add(1, Ordering::Relaxed);
}

#[allow(
    unsafe_code,
    reason = "observes libc forks through its native atfork API"
)]
fn observe_host_forks() -> Result<()> {
    unsafe extern "C" {
        fn pthread_atfork(
            prepare: Option<extern "C" fn()>,
            parent: Option<extern "C" fn()>,
            child: Option<extern "C" fn()>,
        ) -> std::ffi::c_int;
    }
    // SAFETY: a permanent callback which only updates a lock-free atomic.
    let error = unsafe { pthread_atfork(Some(preparing_host_fork), None, None) };
    if error != 0 {
        return Err(super::super::storage(std::io::Error::from_raw_os_error(
            error,
        )));
    }
    Ok(())
}

#[tokio::test]
async fn parallel_module_spawns_tools_without_forking_the_host() -> Result<()> {
    observe_host_forks()?;
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir(root.path().join("workspace")).map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf spawned-without-fork"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        checkout_config(root.path().join("workspace"), (model()?, provider.clone()))?
            .state_directory(root.path().join("state")),
    )
    .await?;
    assert_eq!(harness.run("spawn a tool").await?.text, "done");
    let requests = provider.requests();
    let output = requests
        .last()
        .into_iter()
        .flat_map(|request| &request.messages)
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id,
                content: crate::model::ToolResultContent::Json { value },
                ..
            } if call_id == "checkout-0" => Some(value),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("spawn tool result".into()))?;
    assert_eq!(output["success"], true);
    assert_eq!(output["stdout"], "spawned-without-fork");
    assert_eq!(
        HOST_FORKS.load(Ordering::Relaxed),
        0,
        "a tool forked the multithreaded host and took its allocator arena locks"
    );
    Ok(())
}
