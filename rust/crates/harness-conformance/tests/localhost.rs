//! Canonical localhost service conformance and negative controls.
use acyclic_harness_conformance::{
    runner,
    substrate::{self, Substrate},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::Value;
use std::sync::Arc;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Corrupted(Arc<substrate::localhost::Localhost>);
impl Substrate for Corrupted {
    fn capabilities(&self) -> std::collections::BTreeSet<String> {
        self.0.capabilities()
    }
    fn execute(
        &self,
        recipe: Value,
        paths: Vec<String>,
    ) -> BoxProviderFuture<'_, TestResult<acyclic_harness_conformance::CaseObservation>> {
        Box::pin(async move {
            let mut observation = self.0.execute(recipe, paths).await?;
            if let acyclic_harness_conformance::CaseObservation::Executor(ref mut executor) =
                observation
            {
                executor.text = Some("different deployed answer".into());
            }
            Ok(observation)
        })
    }
}

#[tokio::test]
async fn complete_corpus_has_identical_results_on_both_hosts() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness/tests/scenarios");
    let host = Arc::new(substrate::localhost::Localhost::start().await?);
    let (reference, deployed) = futures::join!(
        runner::run(&root, Arc::new(substrate::InProcess), None, 4),
        runner::run(&root, host.clone(), None, 4),
    );
    let reference = reference?;
    let deployed = deployed?;
    assert!(
        reference.success(),
        "{}",
        serde_json::to_string(&reference)?
    );
    assert!(deployed.success(), "{}", serde_json::to_string(&deployed)?);
    // Compare every discovered case and capability result, not just aggregate
    // pass counts. Both hosts use the original corpus and the same oracle.
    assert_eq!(
        serde_json::to_value(&deployed)?,
        serde_json::to_value(&reference)?
    );
    Arc::into_inner(host)
        .ok_or("host still borrowed")?
        .shutdown()
        .await?;
    Ok(())
}

#[tokio::test]
async fn differing_localhost_observation_fails_the_shared_oracle() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness/tests/scenarios");
    let original = runner::run(
        &root,
        Arc::new(substrate::InProcess),
        Some("core/text.json"),
        1,
    )
    .await?;
    assert!(original.success());
    let host = Arc::new(substrate::localhost::Localhost::start().await?);
    let deployed = runner::run(&root, host.clone(), Some("core/text.json"), 1).await?;
    for record in &deployed.records {
        assert_ne!(
            record.status,
            runner::Status::Fail,
            "{}: {}",
            record.case,
            record.reason
        );
    }
    assert!(deployed.success());
    let changed = runner::run(
        &root,
        Arc::new(Corrupted(host.clone())),
        Some("core/text.json"),
        1,
    )
    .await?;
    assert_eq!(changed.failed, 1);
    assert!(!changed.success());
    Arc::into_inner(host)
        .ok_or("host still borrowed")?
        .shutdown()
        .await?;
    Ok(())
}

#[tokio::test]
async fn identical_epoch_cancellation_histories_and_complete_actor_addresses() -> TestResult {
    use acyclic_harness::{
        OperationId,
        package::{ActorPackage, ActorRuntime, conformance::history},
        workflow::WorkflowCommand,
    };
    use substrate::Volume;
    let host = substrate::localhost::Localhost::start().await?;
    let bindings = substrate::fixture_bindings();
    let package = Arc::new(substrate::HarnessPackage::new(bindings.clone()));
    let identity = package
        .manifest()
        .actors
        .first()
        .ok_or("missing actor implementation")?
        .clone();
    let volume = Arc::new(substrate::localhost::HostedVolume::connect(&host.endpoint()).await?);
    let command = WorkflowCommand {
        operation_id: OperationId::new(),
        kind: "harness.scenario.compute".into(),
        payload: volume
            .put(
                OperationId::new(),
                serde_json::json!({"recipe":{"prompt":"hello", "turns":["hello"]},"paths":[]}),
            )
            .await?,
    };
    let actions = cancellation_history(identity.clone(), &command)?;
    prove_idle_recovery(volume.clone(), package.clone(), &actions).await?;
    let directory = tempfile::tempdir()?;
    let reference = fixture_runtime(
        volume,
        Arc::new(acyclic_stream::LocalStream::open(directory.path(), Default::default()).await?),
    );
    reference.install(package.clone()).await?;
    let expected = history(&reference, &actions).await;
    assert!(
        matches!(
            expected.get(3),
            Some(Err(acyclic_harness::Error::Conflict(_)))
        ),
        "negative control: stale epoch must be rejected"
    );
    for (account, namespace) in [(vec![1], "first"), (vec![2], "first"), (vec![1], "second")] {
        let client = acyclic_local_server::client::Client::connect(
            &host.endpoint(),
            acyclic_local_server::Scope {
                account,
                namespace: namespace.into(),
            },
            identity.digest,
            package.manifest().clone(),
        )
        .await?;
        client.install(package.clone()).await?;
        let actual = history(&client, &actions).await;
        for (index, (actual, expected)) in actual.into_iter().zip(&expected).enumerate() {
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => assert_eq!(&actual, expected, "action {index}"),
                (
                    Err(acyclic_harness::Error::Conflict(_)),
                    Err(acyclic_harness::Error::Conflict(_)),
                ) => {}
                (actual, expected) => panic!("action {index}: {actual:?} != {expected:?}"),
            }
        }
    }
    host.shutdown().await
}

fn cancellation_history(
    identity: acyclic_harness::workflow::MachineIdentity,
    command: &acyclic_harness::workflow::WorkflowCommand,
) -> TestResult<Vec<acyclic_harness::package::conformance::Action>> {
    use acyclic_harness::{
        AgentId, IdempotencyKey, OperationId,
        package::{Event, Placement, conformance::Action},
    };
    let current = Placement {
        actor: AgentId::new(),
        node: "localhost".into(),
        epoch: 2,
    };
    let previous = Placement {
        node: "previous".into(),
        epoch: 1,
        ..current
    };
    let event = Event {
        operation_id: OperationId::new(),
        idempotency_key: IdempotencyKey::new("cancel")?,
        input: Value::Null,
    };
    let input = serde_json::json!({"command":command});
    Ok(vec![
        Action::Place {
            placement: previous.clone(),
            machine: identity.clone(),
            input: input.clone(),
        },
        Action::Recover(previous.clone()),
        Action::Place {
            placement: current.clone(),
            machine: identity,
            input,
        },
        Action::Deliver {
            placement: previous,
            event: event.clone(),
        },
        Action::Cancel {
            placement: current.clone(),
            event: event.clone(),
        },
        Action::Recover(current.clone()),
        Action::Cancel {
            placement: current,
            event,
        },
    ])
}

async fn prove_idle_recovery(
    volume: Arc<substrate::localhost::HostedVolume>,
    package: Arc<substrate::HarnessPackage>,
    actions: &[acyclic_harness::package::conformance::Action],
) -> TestResult {
    use acyclic_harness::package::{ActorPackage, ActorRuntime, conformance::Action};
    // A single poll is sufficient with ready memory providers. The old adapter
    // waited for a future input and returned Pending here, without any timer.
    use futures::FutureExt;
    let idle = substrate::Supervised::new(fixture_runtime(
        volume,
        Arc::new(acyclic_stream::MemoryStream::default()),
    ));
    idle.install(package.clone()).await?;
    let Some(Action::Place {
        placement, input, ..
    }) = actions.first()
    else {
        return Err("missing initial placement".into());
    };
    idle.place(
        placement.clone(),
        package
            .manifest()
            .actors
            .first()
            .ok_or("missing identity")?
            .clone(),
        input.clone(),
    )
    .await?;
    idle.recover(placement.clone())
        .now_or_never()
        .ok_or("idle recovery awaited subscription completion")??;
    Ok(())
}

fn fixture_runtime<P: acyclic_stream::StreamProvider>(
    volume: Arc<substrate::localhost::HostedVolume>,
    stream: Arc<P>,
) -> acyclic_harness::package::in_process::InProcessRuntime<P> {
    acyclic_harness::package::in_process::InProcessRuntime::new(
        stream.clone(),
        substrate::fixture_bindings(),
        Arc::new(substrate::FixturePolicy),
        Arc::new(substrate::FixtureEffects::new(
            volume,
            Arc::new(substrate::Providers {
                stream,
                filesystem: acyclic_fs::Fs::memory(),
            }),
        )),
    )
}
