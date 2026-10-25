//! A host binding installs once; running cases cannot re-enter deployment.
use super::*;
use acyclic_harness::package::Snapshot;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Once<R> {
    runtime: R,
    installs: Arc<AtomicUsize>,
}
impl<R: ActorRuntime> ActorRuntime for Once<R> {
    fn install(&self, package: Arc<dyn ActorPackage>) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            if self.installs.fetch_add(1, Ordering::SeqCst) != 0 {
                return Err(Error::Conflict(
                    "case re-entered package installation".into(),
                ));
            }
            self.runtime.install(package).await
        })
    }
    fn place(
        &self,
        placement: Placement,
        machine: MachineIdentity,
        input: Value,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        self.runtime.place(placement, machine, input)
    }
    fn deliver(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        self.runtime.deliver(placement, event)
    }
    fn recover(&self, placement: Placement) -> BoxProviderFuture<'_, Result<Snapshot>> {
        self.runtime.recover(placement)
    }
    fn cancel(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        self.runtime.cancel(placement, event)
    }
}

async fn check<R: ActorRuntime, V: Volume>(runtime: R, volume: V) -> TestResult {
    let installs = Arc::new(AtomicUsize::new(0));
    let host = Actors::install(
        Once {
            runtime,
            installs: installs.clone(),
        },
        volume,
        fixture_bindings(),
    )
    .await?;
    assert_eq!(
        installs.load(Ordering::SeqCst),
        1,
        "binding escaped before install"
    );
    for answer in ["first", "second"] {
        let observed = host
            .execute(json!({"prompt":"Say hello", "turns":[answer]}), vec![])
            .await?;
        let CaseObservation::Executor(observed) = observed else {
            return Err("host returned another driver".into());
        };
        let case = crate::scenario::Scenario::from_value(json!({
            "prompt":"Say hello", "turns":[answer], "expect":{"text":answer}
        }))?;
        crate::observation::check(&case, &observed)?;
        assert_eq!(
            installs.load(Ordering::SeqCst),
            1,
            "case installed its package"
        );
    }
    Ok(())
}

#[tokio::test]
async fn in_process_binding_installs_before_publication_and_never_per_case() -> TestResult {
    let providers = Arc::new(LocalProviders::local().await?);
    let stream = providers.stream.clone();
    let filesystem = providers.filesystem.clone();
    let volume = Arc::new(volume::FixtureVolume::in_process(filesystem.clone())?);
    let bindings = fixture_bindings();
    let runtime = InProcessRuntime::new(
        stream.clone(),
        bindings.clone(),
        Arc::new(FixturePolicy),
        Arc::new(FixtureEffects::new(volume.clone(), providers, &bindings)),
    );
    check(Supervised::new(runtime), volume).await
}

#[tokio::test]
async fn localhost_binding_installs_before_publication_and_never_per_case() -> TestResult {
    let server = localhost::Localhost::start().await?;
    let package = HarnessPackage::new(fixture_bindings());
    let client = acyclic_local_server::client::Client::connect(
        &server.endpoint(),
        acyclic_local_server::Scope {
            account: vec![1; 16],
            namespace: "conformance".into(),
        },
        package.identity.digest,
        package.manifest().clone(),
    )
    .await?;
    let volume = volume::FixtureVolume::connect(&server.endpoint()).await?;
    check(client, volume).await?;
    server.shutdown().await
}
