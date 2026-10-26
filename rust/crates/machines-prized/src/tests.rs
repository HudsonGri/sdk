use super::*;
use crate::fake::{self, Fake};
use acyclic_machines::{Budgets, ExpirationPolicy};
use futures::StreamExt as _;

fn provider(fake: &Fake, tenant: Option<&str>) -> PrizedProvider {
    let mut config = PrizedConfig::new(fake::KEY);
    config.api_url = format!("{}/api/v1", fake.url);
    config.default_template = Some("prized-base-v1".into());
    config.register_network_policy([8; 32]);
    config.tenant = tenant.map(str::to_owned);
    config.poll_interval = Duration::from_millis(2);
    config.ready_timeout = Duration::from_secs(5);
    PrizedProvider::new(config).unwrap()
}

fn key(suffix: u8) -> IdempotencyKey {
    IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000001{suffix:02x}")).unwrap()
}

fn request(key: IdempotencyKey) -> CreateMachine {
    CreateMachine::new(key, Image::custom([7; 32]).unwrap(), [8; 32])
}

async fn created(provider: &PrizedProvider, key: IdempotencyKey) -> MachineObservation {
    let MutationOutcome::Created(machine) = provider.create(request(key)).await.unwrap() else {
        panic!("create returned the wrong outcome")
    };
    machine
}

fn count(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap()
}

#[tokio::test]
async fn the_conformance_suite_passes_against_the_prized_api() {
    let fake = Fake::start().await;
    let provider = provider(&fake, Some("conformance"));
    acyclic_conformance::machines(&provider).await.unwrap();
    // Checkpoint (one copy), fork from the checkpoint (two), live fork (two): one call each.
    let counts: Vec<u64> = fake
        .calls("POST", "/fork")
        .iter()
        .map(|call| call.body["count"].as_u64().unwrap())
        .collect();
    assert_eq!(counts, [1, 2, 2]);
    // Left alive: only the two checkpoint-fork children, which the suite never destroys.
    let alive = fake.alive();
    assert_eq!(alive.len(), 2, "{alive:?}");
    assert!(
        alive
            .iter()
            .all(|sandbox| sandbox.labels["acyclic.kind"] == "fork")
    );
}

#[tokio::test]
async fn one_fork_call_makes_every_child_in_order() {
    let fake = Fake::start().await;
    let provider = provider(&fake, Some("t1"));
    let machine = created(&provider, key(1)).await;
    let outcome = provider
        .fork_machine(machine.id, count(8), key(2))
        .await
        .unwrap();
    let MutationOutcome::MachineForked {
        source,
        fidelity,
        children,
    } = outcome
    else {
        panic!("wrong outcome")
    };
    assert_eq!(
        (source, fidelity),
        (machine.id, ForkFidelity::MemoryAndDisk)
    );
    let forks = fake.calls("POST", "/fork");
    assert_eq!(
        forks.len(),
        1,
        "no fan-out tree: one call for eight children"
    );
    let call = &forks[0];
    assert_eq!(call.body["count"], 8);
    assert_eq!(
        call.idempotency_key.as_deref(),
        Some(format!("acyclic/t1/{}", key(2)).as_str())
    );
    assert_eq!(call.body["labels"]["acyclic.kind"], "live-fork");
    assert_eq!(
        call.body["labels"]["acyclic.parent"],
        machine.id.to_string()
    );
    let sandbox_ids: Vec<String> = children
        .iter()
        .map(|child| map::machine_sandbox(child.id).unwrap())
        .collect();
    let mut ordered = sandbox_ids.clone();
    ordered.sort();
    assert_eq!(sandbox_ids, ordered, "children come back in fork order");
    for child in &children {
        assert_eq!(child.state, MachineState::Running);
        assert_eq!(child.contract, machine.contract);
        assert_eq!(child.endpoints.len(), 1);
    }
    // A source can go before its children: they are independent once running.
    provider.destroy_machine(machine.id, key(3)).await.unwrap();
    assert_eq!(
        provider
            .inspect_machine(children[0].id)
            .await
            .unwrap()
            .state,
        MachineState::Running
    );
}

#[tokio::test]
async fn refusals_happen_before_any_request_and_release_the_key() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let mut budgeted = request(key(1));
    budgeted.budgets = Budgets {
        spend_micros: 1,
        concurrency: 0,
    };
    let mut idle = request(key(1));
    idle.expiration = ExpirationPolicy::Idle(Duration::from_secs(60));
    let mut too_long = request(key(1));
    too_long.expiration = ExpirationPolicy::MaxAge(Duration::from_secs(7 * 86_400 + 60));
    let mut network = request(key(1));
    network.network_policy_digest = [9; 32];
    let mut elastic = request(key(1));
    elastic.compatibility = CompatibilityPolicy::Require(BTreeSet::from([Capability::ElasticCpu]));
    for refused in [budgeted, idle, too_long, network, elastic] {
        assert!(matches!(
            provider.create(refused).await,
            Err(ProviderError::Unsupported(_))
        ));
    }
    assert!(fake.calls("POST", "/sandboxes").is_empty());
    // The key was released, so the corrected request goes through under it.
    created(&provider, key(1)).await;
    assert_eq!(fake.calls("POST", "/sandboxes").len(), 1);
}

#[tokio::test]
async fn exec_runs_argv_once_without_warm_up_or_waking() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let machine = created(&provider, key(1)).await;
    let argv = vec!["printf".to_owned(), "%s".to_owned(), "a b's".to_owned()];
    let command = ExecCommand::new(argv, 1_500).unwrap();
    let frames: Vec<_> = provider
        .exec(machine.id, command.clone())
        .await
        .unwrap()
        .collect()
        .await;
    let line = r"exec 'printf' '%s' 'a b'\''s' 2>&1";
    assert_eq!(
        frames,
        [
            Ok(ExecOutput::Data(line.as_bytes().to_vec())),
            Ok(ExecOutput::ExitCode(0))
        ]
    );
    let calls = fake.calls("POST", "/process/execute");
    let commands: Vec<&str> = calls
        .iter()
        .map(|call| call.body["command"].as_str().unwrap())
        .collect();
    assert_eq!(
        commands,
        [line],
        "a running sandbox takes the command at once"
    );
    assert_eq!(calls[0].body["timeout"], 1.5);
    assert!(
        calls
            .iter()
            .all(|call| call.auto_resume.as_deref() == Some("false"))
    );

    // A lost answer, or a sandbox deleted under a running command, may have run it:
    // indeterminate, never retried.
    for code in ["sandbox_unreachable", "sandbox_deleted"] {
        let status = if code == "sandbox_deleted" { 410 } else { 502 };
        fake.world().exec_reply = Some((status, serde_json::json!({ "error": code })));
        let before = fake.calls("POST", "/process/execute").len();
        assert!(matches!(
            provider.exec(machine.id, command.clone()).await,
            Err(ProviderError::CommandIndeterminate(id)) if id == machine.id
        ));
        assert_eq!(fake.calls("POST", "/process/execute").len(), before + 1);
    }
    // A deadline kill ran the command: its output, then indeterminate.
    fake.world().exec_reply = Some((
        200,
        serde_json::json!({"stdout": "partial", "stderr": "", "exitCode": -1, "timedOut": true}),
    ));
    let frames: Vec<_> = provider
        .exec(machine.id, command.clone())
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(frames[0], Ok(ExecOutput::Data(b"partial".to_vec())));
    assert!(matches!(
        frames[1],
        Err(ProviderError::CommandIndeterminate(_))
    ));

    // A suspended machine is never woken by a command.
    provider.suspend(machine.id, key(2)).await.unwrap();
    let before = fake.calls("POST", "/process/execute").len();
    assert!(matches!(
        provider.exec(machine.id, command).await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(fake.calls("POST", "/process/execute").len(), before);
    assert!(fake.calls("POST", "/resume").is_empty());
}

#[tokio::test]
async fn the_key_never_goes_to_an_edge_outside_prized() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let machine = created(&provider, key(1)).await;
    let sandbox_id = map::machine_sandbox(machine.id).unwrap();
    fake.world()
        .sandboxes
        .get_mut(&sandbox_id)
        .unwrap()
        .toolbox_base = "https://attacker.example/edge".into();
    let command = ExecCommand::new(vec!["true".into()], 1_000).unwrap();
    assert!(matches!(
        provider.exec(machine.id, command).await,
        Err(ProviderError::Rejected(_))
    ));
}

#[tokio::test]
async fn a_restarted_provider_recovers_forks_from_prized_idempotency() {
    let fake = Fake::start().await;
    let first = provider(&fake, Some("t1"));
    let machine = created(&first, key(1)).await;
    let forked = first
        .fork_machine(machine.id, count(3), key(2))
        .await
        .unwrap();
    let before = fake.world().sandboxes.len();

    let restarted = provider(&fake, Some("t1"));
    assert_eq!(restarted.recover(key(2)).await.unwrap(), forked);
    assert_eq!(
        restarted.recover(key(1)).await.unwrap(),
        MutationOutcome::Created(restarted.inspect_machine(machine.id).await.unwrap())
    );
    assert_eq!(
        fake.world().sandboxes.len(),
        before,
        "recovery made nothing"
    );
    let replays = fake.calls("POST", "/fork");
    assert_eq!(replays.len(), 2);
    assert_eq!(replays[0].body, replays[1].body);
    assert!(matches!(
        restarted.recover(key(9)).await,
        Err(ProviderError::NotFound(_))
    ));
}

#[tokio::test]
async fn tenants_never_see_each_other() {
    let fake = Fake::start().await;
    let one = provider(&fake, Some("one"));
    let other = provider(&fake, Some("other"));
    let machine = created(&one, key(1)).await;
    assert!(matches!(
        other.inspect_machine(machine.id).await,
        Err(ProviderError::NotFound(_))
    ));
    assert!(
        other
            .list_machines(None, 16)
            .await
            .unwrap()
            .machines
            .is_empty()
    );
    assert_eq!(one.list_machines(None, 16).await.unwrap().machines.len(), 1);
    // Prized filters by the provider's labels; the provider checks the tenant again.
    let lists: Vec<String> = fake
        .world()
        .calls
        .iter()
        .filter(|call| call.method == "GET" && call.path.starts_with("/sandboxes?"))
        .map(|call| call.path.clone())
        .collect();
    assert_eq!(
        lists,
        [
            "/sandboxes?label=acyclic.managed%3Dtrue&label=acyclic.tenant%3Dother",
            "/sandboxes?label=acyclic.managed%3Dtrue&label=acyclic.tenant%3Done",
        ]
    );
}

#[tokio::test]
async fn checkpoints_are_paused_copies_that_fork_without_running() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let machine = created(&provider, key(1)).await;
    let MutationOutcome::Checkpointed(checkpoint) =
        provider.checkpoint(machine.id, key(2)).await.unwrap()
    else {
        panic!("wrong outcome")
    };
    let copy = map::checkpoint_sandbox(checkpoint.id).unwrap();
    assert_eq!(fake.sandbox(&copy).state, "paused");
    assert_eq!(
        (checkpoint.source, &checkpoint.contract),
        (machine.id, &machine.contract)
    );
    let as_machine = MachineId::parse(&checkpoint.id.to_string()).unwrap();
    assert!(matches!(
        provider.inspect_machine(as_machine).await,
        Err(ProviderError::NotFound(_))
    ));
    assert!(
        provider
            .list_machines(None, 16)
            .await
            .unwrap()
            .machines
            .len()
            == 1
    );

    let MutationOutcome::Forked(children) = provider
        .fork(checkpoint.id, count(2), key(3))
        .await
        .unwrap()
    else {
        panic!("wrong outcome")
    };
    for child in &children {
        assert_eq!(child.last_checkpoint, Some(checkpoint.id));
        assert_eq!(child.contract.image, Image::Checkpoint(checkpoint.id));
    }
    assert_eq!(fake.sandbox(&copy).state, "paused");
    let order: Vec<String> = fake
        .world()
        .calls
        .iter()
        .filter(|call| call.method == "POST" && call.path.contains(&copy))
        .map(|call| call.path.rsplit('/').next().unwrap().to_owned())
        .collect();
    assert_eq!(
        order,
        ["pause", "fork"],
        "the copy is forked where it is paused"
    );

    // A copy resumed outside the provider no longer holds the checkpoint.
    fake.world().sandboxes.get_mut(&copy).unwrap().state = "running".into();
    assert!(matches!(
        provider.fork(checkpoint.id, count(1), key(4)).await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(fake.calls("POST", "/fork").len(), 2, "nothing was forked");
}

#[tokio::test]
async fn a_wake_keeps_max_age_from_creation() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let mut aged = request(key(1));
    aged.expiration = ExpirationPolicy::MaxAge(Duration::from_secs(2 * 3_600));
    let MutationOutcome::Created(machine) = provider.create(aged).await.unwrap() else {
        panic!("wrong outcome")
    };
    let sandbox_id = map::machine_sandbox(machine.id).unwrap();
    assert_eq!(
        fake.calls("POST", "/sandboxes")[0].body["maxRuntimeMin"],
        120
    );
    provider.suspend(machine.id, key(2)).await.unwrap();
    // Half an hour later Prized's resume would give it two more hours; the wake sets the end
    // back to its creation plus two hours.
    fake.world().clock_offset_ms = 30 * 60_000;
    provider.wake(machine.id, key(3)).await.unwrap();
    let deadline = machine.created_at_unix_ms + 2 * 3_600_000;
    let pins = fake.calls("POST", "/timeout");
    assert_eq!(pins.len(), 1);
    assert_eq!(pins[0].body["expiresAt"], map::format_rfc3339_ms(deadline));
    assert_eq!(fake.sandbox(&sandbox_id).expires_at_ms, Some(deadline));
    // A resume made outside the provider restarts the clock; a wake of the running machine
    // pins it again, and an end already in place is left alone.
    fake.world()
        .sandboxes
        .get_mut(&sandbox_id)
        .unwrap()
        .expires_at_ms = Some(deadline + 3_600_000);
    provider.wake(machine.id, key(4)).await.unwrap();
    provider.wake(machine.id, key(5)).await.unwrap();
    assert_eq!(fake.calls("POST", "/timeout").len(), 2);
    assert_eq!(fake.sandbox(&sandbox_id).expires_at_ms, Some(deadline));

    // A machine whose age ran out while suspended is not woken.
    let mut old = request(key(6));
    old.expiration = ExpirationPolicy::MaxAge(Duration::from_secs(3_600));
    fake.world().clock_offset_ms = -2 * 3_600_000;
    let MutationOutcome::Created(expired) = provider.create(old).await.unwrap() else {
        panic!("wrong outcome")
    };
    fake.world().clock_offset_ms = 0;
    provider.suspend(expired.id, key(7)).await.unwrap();
    let resumes = fake.calls("POST", "/resume").len();
    assert!(matches!(
        provider.wake(expired.id, key(8)).await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(fake.calls("POST", "/resume").len(), resumes);

    // Without a MaxAge, a wake leaves Prized's run time alone.
    let plain = created(&provider, key(9)).await;
    provider.suspend(plain.id, key(10)).await.unwrap();
    provider.wake(plain.id, key(11)).await.unwrap();
    assert_eq!(fake.calls("POST", "/timeout").len(), 2);
}

#[tokio::test]
async fn a_wake_that_cannot_pin_max_age_leaves_the_machine_suspended() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let mut aged = request(key(1));
    aged.expiration = ExpirationPolicy::MaxAge(Duration::from_secs(3_600));
    let MutationOutcome::Created(machine) = provider.create(aged).await.unwrap() else {
        panic!("wrong outcome")
    };
    provider.suspend(machine.id, key(2)).await.unwrap();
    fake.world().fail_timeouts = 1;
    assert!(matches!(
        provider.wake(machine.id, key(3)).await,
        Err(ProviderError::Unavailable)
    ));
    assert!(matches!(
        provider.inspect_machine(machine.id).await.unwrap().state,
        MachineState::Suspending | MachineState::Suspended
    ));
    // A later wake goes through and pins the end.
    provider.wake(machine.id, key(4)).await.unwrap();
    let sandbox_id = map::machine_sandbox(machine.id).unwrap();
    assert_eq!(
        fake.sandbox(&sandbox_id).expires_at_ms,
        Some(machine.created_at_unix_ms + 3_600_000)
    );
}

#[tokio::test]
async fn a_failed_fork_deletes_the_children_it_made() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let machine = created(&provider, key(1)).await;
    fake.world().fail_children = true;
    assert!(matches!(
        provider.fork_machine(machine.id, count(2), key(2)).await,
        Err(ProviderError::Failed)
    ));
    assert_eq!(fake.calls("DELETE", "").len(), 2);
    assert_eq!(fake.alive().len(), 1, "only the source is left");
    assert!(matches!(
        provider.fork_machine(machine.id, count(2), key(2)).await,
        Err(ProviderError::Failed)
    ));
}

#[tokio::test]
async fn unbound_operations_are_unsupported_without_requests() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let machine = created(&provider, key(1)).await;
    let before = fake.world().calls.len();
    assert!(matches!(
        provider.read_output(machine.id, 0, 16).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        provider
            .set_suspension_policy(machine.id, SuspensionPolicy::Manual, key(2))
            .await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        provider.fork_machine(machine.id, count(257), key(3)).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert_eq!(fake.world().calls.len(), before);
    let usage = provider.usage(machine.id, 1, 2).await.unwrap();
    assert!(!usage.receipt.is_empty());
    assert_eq!(
        usage.elastic_cpu_ns, 0,
        "no overlap with the machine's life"
    );
}

#[tokio::test]
async fn throttled_polls_are_retried_not_fatal() {
    let fake = Fake::start().await;
    let provider = provider(&fake, None);
    let machine = created(&provider, key(1)).await;
    fake.world().throttle_reads = 6;
    let MutationOutcome::MachineForked { children, .. } = provider
        .fork_machine(machine.id, count(2), key(2))
        .await
        .unwrap()
    else {
        panic!("wrong outcome")
    };
    assert_eq!(children.len(), 2);
    assert_eq!(
        fake.world().throttle_reads,
        0,
        "the throttled reads happened"
    );
    provider
        .destroy_machine(children[0].id, key(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn extra_labels_reach_every_sandbox_and_cannot_shadow_the_provider() {
    let fake = Fake::start().await;
    let mut config = provider(&fake, None).config().clone();
    config.labels = BTreeMap::from([("agent".to_owned(), "ops".to_owned())]);
    let labelled = PrizedProvider::new(config.clone()).unwrap();
    let machine = created(&labelled, key(1)).await;
    labelled
        .fork_machine(machine.id, count(2), key(2))
        .await
        .unwrap();
    assert!(
        fake.alive()
            .iter()
            .all(|sandbox| sandbox.labels["agent"] == "ops")
    );
    config.labels = BTreeMap::from([("acyclic.key".to_owned(), "x".to_owned())]);
    assert!(matches!(
        PrizedProvider::new(config),
        Err(ProviderError::Invalid(_))
    ));
}
