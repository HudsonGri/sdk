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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_corpus_has_identical_results_on_both_hosts() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness/tests/scenarios");
    let host = Arc::new(substrate::localhost::Localhost::start().await?);
    let corpus = runner::Corpus::load(&root)?;
    let (reference, deployed) = futures::join!(
        corpus.run(Arc::new(substrate::InProcess::default()), None, 4),
        corpus.run(host.clone(), None, 4),
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
    let corpus = runner::Corpus::load(&root)?;
    let original = corpus
        .run(
            Arc::new(substrate::InProcess::default()),
            Some("core/text.json"),
            1,
        )
        .await?;
    assert!(original.success());
    let host = Arc::new(substrate::localhost::Localhost::start().await?);
    let deployed = corpus.run(host.clone(), Some("core/text.json"), 1).await?;
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
    let changed = corpus
        .run(Arc::new(Corrupted(host.clone())), Some("core/text.json"), 1)
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
    let volume = Arc::new(substrate::volume::FixtureVolume::connect(&host.endpoint()).await?);
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
    let client = acyclic_local_server::client::Client::connect(
        &host.endpoint(),
        acyclic_local_server::Scope {
            account: vec![1],
            namespace: "first".into(),
        },
        identity.digest,
        package.manifest().clone(),
    )
    .await?;
    assert!(
        client
            .for_scope(acyclic_local_server::Scope {
                account: vec![],
                namespace: "first".into()
            })
            .is_err()
    );
    assert!(
        client
            .for_scope(acyclic_local_server::Scope {
                account: vec![1],
                namespace: String::new()
            })
            .is_err()
    );
    for (account, namespace) in [(vec![1], "first"), (vec![2], "first"), (vec![1], "second")] {
        let scope = acyclic_local_server::Scope {
            account,
            namespace: namespace.into(),
        };
        let mut bindings = substrate::fixture_bindings();
        bindings.streams = scope.resource_prefix();
        let directory = tempfile::tempdir()?;
        let reference = fixture_runtime(
            volume.clone(),
            Arc::new(
                acyclic_stream::LocalStream::open(directory.path(), Default::default()).await?,
            ),
            bindings.clone(),
        );
        reference
            .install(Arc::new(substrate::HarnessPackage::new(bindings)))
            .await?;
        let expected = history(&reference, &actions).await;
        assert!(
            matches!(
                expected.get(3),
                Some(Err(acyclic_harness::Error::Conflict(_)))
            ),
            "negative control: stale epoch must be rejected"
        );
        let client = client.for_scope(scope)?;
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
    volume: Arc<substrate::volume::FixtureVolume>,
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
        substrate::fixture_bindings(),
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
    volume: Arc<substrate::volume::FixtureVolume>,
    stream: Arc<P>,
    bindings: acyclic_harness::package::Bindings,
) -> acyclic_harness::package::in_process::InProcessRuntime<P> {
    let effects = Arc::new(substrate::FixtureEffects::new(
        volume,
        Arc::new(substrate::Providers {
            stream: stream.clone(),
            filesystem: acyclic_fs::Fs::memory(),
        }),
        &bindings,
    ));
    acyclic_harness::package::in_process::InProcessRuntime::new(
        stream,
        bindings,
        Arc::new(substrate::FixturePolicy),
        effects,
    )
}
