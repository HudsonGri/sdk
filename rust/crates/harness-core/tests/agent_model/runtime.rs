//! The agent handler on the A4 runtime: create, wake on inbox append, spawn.

use super::{Result, adopted, id, intent, user, values};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    guest::Handler,
    local::{Runtime, Store, SystemClock},
};
use acyclic_harness_core::agent::*;
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Spawns child `c` on its first turn and idles afterwards.
#[derive(Default)]
struct Spawner(AtomicUsize);

impl TurnDriver for Spawner {
    fn step<'a>(
        &'a self,
        _agent: &'a AgentState,
        _cursor: u64,
        _input: Option<TurnInput>,
    ) -> TurnFuture<'a> {
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            Ok(TurnStep {
                checkpoint: None,
                outs: if first {
                    vec![Out::Spawn {
                        intent: intent("c"),
                        brief: None,
                        volumes: Vec::new(),
                    }]
                } else {
                    Vec::new()
                },
                wait: Wait::Input,
            })
        })
    }
}

fn store(provider: &Arc<MemoryStream>) -> Result<Store<MemoryStream>> {
    Ok(Store::new(provider.clone(), StreamPath::new(ACTORS)?))
}

#[tokio::test]
async fn an_inbox_append_wakes_the_agent_and_its_spawn_creates_the_child_actor() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let limits = ActorLimits::new(10_000, 1 << 20, 1 << 16)?;
    let handler = Arc::new(AgentHandler::new(store(&provider)?, Spawner::default(), limits)?);
    let runtime = Runtime::new(
        store(&provider)?,
        handler.clone() as Arc<dyn Handler>,
        Arc::new(SystemClock),
    );
    let within = ActorReference::new(b"account", b"agents", b"owner")?;
    let created = handler.create_root(&within, &intent("create"), adopted(id(1)))?;
    let root = created.child.clone();
    assert!(matches!(
        submit(&*provider, created.request).await?,
        Submitted::Committed(_)
    ));
    let root_actor = AgentHandler::<MemoryStream, Spawner>::actor(&within, &root.id())?;
    runtime.track(root_actor.clone())?;
    runtime.wake(&root_actor);
    assert!(runtime.drain().await.is_empty());
    let actors = store(&provider)?;
    assert_eq!(actors.load(&root_actor).await?.state().and_then(|s| s.cursor(INBOX)), Some(1));

    let child = root.child(&AgentId::derive(&root.id(), "c"))?;
    let record = InboxRecord::ApprovalRequested(ApprovalRequested {
        child: child.clone(),
        id: intent("q"),
    });
    let tail = inbox_tail(&*provider, &root).await?;
    assert!(matches!(
        submit(&*provider, send(&root, tail, &user(), &record)?).await?,
        Submitted::Committed(_)
    ));
    // The root's pending inbox follow holds its waker: the append alone wakes it.
    assert!(runtime.drain().await.is_empty());
    let child_actor = AgentHandler::<MemoryStream, Spawner>::actor(&within, &child.id())?;
    let state = actors.load(&child_actor).await?;
    assert_eq!(state.state().and_then(|s| s.cursor(INBOX)), Some(2));
    assert_eq!(values(&provider, child.inbox()?).await?.len(), 3);

    runtime.track(child_actor.clone())?;
    runtime.wake(&child_actor);
    assert!(runtime.drain().await.is_empty());
    let state = actors.load(&child_actor).await?;
    assert_eq!(state.state().and_then(|s| s.cursor(INBOX)), Some(3));
    assert!(state.state().and_then(|s| s.checkpoint()).is_some());
    Ok(())
}
