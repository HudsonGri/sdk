//! Open provider subscriptions cannot retain the process owner during shutdown.
#[path = "support/native_hooks.rs"]
mod native_hooks;

use acyclic_stream::{ChildrenRequest, StreamPath, StreamProvider as _};
use futures::StreamExt as _;
use native_hooks::*;
use serde_json::json;

#[tokio::test]
async fn owner_shutdown_completes_while_clients_retain_their_follows() -> Result {
    let fixture = Fixture::new()?;
    let owner = fixture.start_owner().await?;
    let attached = hook(fixture.path(), "pi", "session_start", json!({
        "sessionId":"open-follows","cwd":fixture.path(),
    })).await?;
    let task = attached["task"].as_str().ok_or("missing attached task")?;
    let client = client(fixture.path()).await?;
    let mut followed = client.follow(StreamPath::new(format!("external/results/{task}"))?, 0).await?;
    followed.next().await.ok_or("missing result prefix")??;
    let mut children = client.children(ChildrenRequest {
        parent:Some(StreamPath::new("external/requests")?), limit:64,
    }).await?;
    children.next().await.ok_or("missing admitted task child")??;
    // Both SDK follows stay alive until the owner has drained and released its lock.
    tokio::time::timeout(std::time::Duration::from_secs(3), fixture.close_owner(owner)).await??;
    drop((followed, children, client));
    Ok(())
}
