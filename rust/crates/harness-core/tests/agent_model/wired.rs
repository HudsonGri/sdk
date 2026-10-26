//! The wired path: stock executor and a filesystem journal on the agent's private volume.

use super::{Result, adopted, id, intent};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_harness_core::{
    agent::*,
    context::{CompactionPolicy, ContextPipeline},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    executor::StockExecutor,
    filesystem::{FilesystemHost, FilesystemTurnHost, PRIVATE_VOLUME},
    model::Model,
    resources::ProviderRef,
    test_support::{ScriptedModelProvider, ScriptedTurn},
    tool::ToolRegistry,
};
use acyclic_stream::MemoryStream;
use std::sync::Arc;

#[tokio::test]
async fn a_child_turn_runs_the_stock_executor_and_replays_without_redispatch() -> Result {
    let parent = AgentPath::root(&id(1));
    let path = parent.child(&id(2))?;
    let provider = ProviderRef::new("agents".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::<MemoryAuthorityBackend, MemoryObjectBackend>::new(
        Fs::memory(),
        &provider,
        None,
    )?;
    let owner = acyclic_harness_core::AgentId::from_bytes(path.id().to_bytes());
    host.create_volume(&VolumeRef::new(
        provider,
        "agent-2-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?)
    .await?;
    let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]));
    let executor = StockExecutor::new(
        Model::new("test".into(), "text".into(), "1".into(), serde_json::Value::Null)?,
        model.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_compaction_policy(CompactionPolicy::Disabled);
    let driver = ExecutorDriver::new(FilesystemTurnHost::new(
        Arc::new(MemoryStream::default()),
        host,
        ("agents".into(), [7; 32]),
        executor,
        (64 * 1024, 4),
    ));
    let mut adoption = adopted(id(2));
    adoption.parent = Some(parent.clone());
    adoption.volumes = vec![VolumeBinding {
        name: PRIVATE_VOLUME.into(),
        volume: "agent-2-private".into(),
    }];
    let state = AgentState::adopted(path.clone(), &adoption);
    let wake = || Wake::Inbox {
        sequence: 2,
        record: InboxRecord::ApprovalRequested(ApprovalRequested {
            child: parent.clone(),
            id: intent("r1"),
        }),
    };
    let first = step(&driver, &path, Some(state.clone()), 2, wake()).await?;
    let Phase::Done {
        outcome: AgentOutcome::Succeeded { result: Some(_) },
    } = &first.state.phase
    else {
        return Err(format!("unexpected phase {:?}", first.state.phase).into());
    };
    let entries: Vec<_> = first
        .outs
        .iter()
        .filter_map(|out| match out {
            Out::Conversation(bytes) => Some(ConversationEntry::decode(bytes)),
            _ => None,
        })
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(entries.len(), 1);
    // A crash before the step commit re-runs the step; the journal replays it.
    let again = step(&driver, &path, Some(state), 2, wake()).await?;
    assert_eq!(again.outs, first.outs);
    assert_eq!(model.requests().len(), 1);
    Ok(())
}
