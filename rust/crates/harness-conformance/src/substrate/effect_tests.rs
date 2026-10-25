//! Placement custody must not serialize unrelated actors or weaken fencing.
use super::*;
use acyclic_harness::{
    conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use futures::FutureExt;
use std::sync::atomic::{AtomicUsize, Ordering};

struct ReadyVolume(FileRef);
impl Volume for ReadyVolume {
    fn put(&self, _: OperationId, _: Value) -> BoxProviderFuture<'_, Result<FileRef>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>> {
        Box::pin(
            async move { Ok(json!({"recipe":{"blocked":file.path() == "blocked"},"paths":[]})) },
        )
    }
    fn lookup(&self, _: OperationId) -> BoxProviderFuture<'_, Result<Option<FileRef>>> {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Default)]
struct Executor(AtomicUsize);
impl ExecutorFixture for Executor {
    fn collect(
        &self,
        recipe: Value,
        _: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<CaseObservation>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            if recipe.get("blocked") == Some(&Value::Bool(true)) {
                futures::future::pending::<()>().await;
            }
            Ok(CaseObservation::Executor(crate::observation::Observation {
                result: Ok(Value::Null),
                text: recipe
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                interruptions: vec![],
                replay_error: None,
                requests: vec![],
                request_files: vec![],
                reconciliations: 0,
                unused_turns: 0,
                unused_recoveries: 0,
                events: vec![],
                files: Default::default(),
            }))
        })
    }
}

fn file(path: &str) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("fixture".into(), "filesystem".into(), "1".into(), &mut None)?,
            "effects".into(),
            VolumeClass::SessionShared,
            VolumeOwner::Session("test".into()),
            &mut None,
        )?,
        path.into(),
        "one".into(),
        FileDescriptor::from_bytes(b"{}", "application/json", &mut None)?,
        "fixture.json".into(),
        &mut None,
    )
}

#[test]
fn effects_are_serial_within_a_placement_and_independent_across_placements() -> TestResult {
    let executor = Arc::new(Executor::default());
    let effects = FixtureEffects::new(
        Arc::new(ReadyVolume(file("ready")?)),
        executor.clone(),
        &fixture_bindings(),
    );
    let placement = Placement {
        actor: AgentId::new(),
        node: "host".into(),
        epoch: 1,
    };
    let blocked = WorkflowCommand {
        operation_id: OperationId::new(),
        kind: "harness.scenario.compute".into(),
        payload: file("blocked")?,
    };
    let ready = WorkflowCommand {
        payload: file("ready")?,
        ..blocked.clone()
    };
    let mut first = effects.execute(placement.clone(), blocked.clone());
    assert!(first.as_mut().now_or_never().is_none());
    let mut duplicate = effects.execute(placement.clone(), blocked);
    assert!(duplicate.as_mut().now_or_never().is_none());
    let mut fence = effects.fence(placement.clone());
    assert!(
        fence.as_mut().now_or_never().is_none(),
        "fence escaped live work"
    );
    let other = Placement {
        actor: AgentId::new(),
        ..placement.clone()
    };
    let completed = effects
        .execute(other.clone(), ready.clone())
        .now_or_never()
        .ok_or("unrelated actor waited for blocked placement")??;
    assert_eq!(
        effects
            .execute(other, ready.clone())
            .now_or_never()
            .ok_or("duplicate blocked")??,
        completed
    );
    let new_epoch = Placement {
        epoch: 2,
        ..placement.clone()
    };
    assert!(
        effects
            .execute(new_epoch.clone(), ready.clone())
            .now_or_never()
            .ok_or("new epoch blocked")?
            .is_ok()
    );
    assert_eq!(
        executor.0.load(Ordering::SeqCst),
        3,
        "duplicate dispatched compute"
    );
    drop(duplicate);
    drop(first);
    fence
        .now_or_never()
        .ok_or("fence did not acquire retired placement")??;
    assert!(matches!(
        effects.execute(placement, ready.clone()).now_or_never(),
        Some(Err(Error::Conflict(_)))
    ));
    assert!(
        effects
            .execute(new_epoch, ready)
            .now_or_never()
            .ok_or("foreign fence affected new epoch")?
            .is_ok()
    );
    assert_eq!(executor.0.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn effect_recovery_is_scoped_to_actor_and_namespace_but_survives_placement_changes()
-> TestResult {
    let volume = Arc::new(volume::FixtureVolume::in_process(Fs::memory())?);
    let executor = Arc::new(Executor::default());
    let bindings = fixture_bindings();
    let effects = FixtureEffects::new(volume.clone(), executor.clone(), &bindings);
    let mut other_bindings = bindings.clone();
    other_bindings.streams = "another-namespace".into();
    let other_effects = FixtureEffects::new(volume.clone(), executor.clone(), &other_bindings);
    let placement = Placement {
        actor: AgentId::new(),
        node: "first".into(),
        epoch: 1,
    };
    let operation = OperationId::new();
    let mut events = Vec::new();
    for (index, (host, actor)) in [
        (&effects, placement.actor),
        (&effects, AgentId::new()),
        (&other_effects, placement.actor),
    ]
    .into_iter()
    .enumerate()
    {
        let text = format!("result-{index}");
        let command = WorkflowCommand {
            operation_id: operation,
            kind: "harness.scenario.compute".into(),
            payload: volume
                .put(
                    OperationId::new(),
                    json!({"recipe":{"text":text},"paths":[]}),
                )
                .await?,
        };
        let owner = Placement {
            actor,
            ..placement.clone()
        };
        let event = host.execute(owner.clone(), command.clone()).await?;
        let output: FileRef = serde_json::from_value(field(&event.input, "output")?.clone())?;
        let observed = volume.get(output).await?;
        assert_eq!(
            field(field(&observed, "observation")?, "text")?
                .as_str()
                .ok_or("missing observed text")?,
            text.as_str(),
            "another actor or namespace supplied the result"
        );
        assert_eq!(event, host.execute(owner.clone(), command.clone()).await?);
        host.fence(owner.clone()).await?;
        let recovered = FixtureEffects::new(
            volume.clone(),
            executor.clone(),
            if index == 2 {
                &other_bindings
            } else {
                &bindings
            },
        );
        assert_eq!(
            event,
            recovered
                .execute(
                    Placement {
                        epoch: 2,
                        node: "recovered".into(),
                        ..owner
                    },
                    command
                )
                .await?,
            "placement change lost its completed result"
        );
        events.push(event.input);
    }
    let [first, second, third] = events.as_slice() else {
        return Err("missing scoped effect observations".into());
    };
    assert_ne!(first, second);
    assert_ne!(first, third);
    assert_eq!(
        executor.0.load(Ordering::SeqCst),
        3,
        "recovery dispatched completed compute again"
    );
    Ok(())
}
