//! Live checks against a real Prized workspace. All are ignored by default and need
//! `PRIZED_API_KEY` (a key with Sandboxes: Write); see the crate README for the commands.
//! Every sandbox is 1 vCPU with 1 GB, carries a fresh `acyclic.tenant`, and is deleted
//! whatever happens.

#![allow(clippy::expect_used, clippy::panic, reason = "test assertions")]

use std::{
    num::NonZeroU32,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use acyclic_machines::{
    ExecCommand, ExecOutput, ExpirationPolicy, IdempotencyKey, Image, MachineId,
    MachineObservation, MachinesProvider, MutationOutcome,
};
use acyclic_machines_prized::{PrizedConfig, PrizedProvider, api::Resources, map};
use futures::StreamExt as _;

fn provider(label: &str) -> PrizedProvider {
    let mut config = PrizedConfig::from_env().expect("PRIZED_API_KEY must be set");
    config
        .default_template
        .get_or_insert_with(|| "prized-base-v1".to_owned());
    config.resources = Some(Resources {
        vcpu: Some(1),
        memory_mb: Some(1_024),
        disk_gb: None,
    });
    // The suite commits every machine to network policy `[8; 32]`: Prized's only policy.
    config.register_network_policy([8; 32]);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    config.tenant = Some(format!("{label}-{stamp}"));
    PrizedProvider::new(config).expect("provider builds")
}

/// Deletes every sandbox of the provider's tenant.
async fn sweep(provider: &PrizedProvider) {
    let tenant = provider.config().tenant.clone().unwrap_or_default();
    let listed = provider
        .api()
        .list(&[(map::LABEL_TENANT, &tenant)])
        .await
        .unwrap_or_default();
    for sandbox in listed {
        if !map::owned_by(&sandbox, Some(&tenant)) {
            continue;
        }
        match provider.api().delete(&sandbox.id).await {
            Ok(()) => eprintln!("swept {}", sandbox.id),
            Err(error) => eprintln!("delete {} failed: {error}", sandbox.id),
        }
    }
}

/// Full Machines conformance suite: about nine short-lived sandboxes, at most six at once.
#[tokio::test]
#[ignore = "needs PRIZED_API_KEY and creates billable sandboxes"]
async fn conformance_suite_against_prized() {
    let provider = provider("conformance");
    let started = Instant::now();
    let outcome = acyclic_conformance::machines(&provider).await;
    eprintln!(
        "conformance suite: {outcome:?} in {} ms",
        started.elapsed().as_millis()
    );
    sweep(&provider).await;
    outcome.expect("machines conformance suite passes");
}

/// A `MaxAge` machine keeps its end after a suspend and wake, though Prized restarts a resumed
/// sandbox's run time on a paid workspace. The end is first moved an hour later through the
/// API, as such a resume would, so the wake has to pin it back whatever the workspace's plan.
#[tokio::test]
#[ignore = "needs PRIZED_API_KEY and creates one short-lived 1 vCPU sandbox"]
async fn wake_keeps_max_age() {
    let provider = provider("max-age");
    let outcome = max_age(&provider).await;
    sweep(&provider).await;
    outcome.unwrap_or_else(|error| panic!("{error}"));
}

async fn max_age(provider: &PrizedProvider) -> Result<(), String> {
    let image = Image::custom([7; 32]).map_err(|error| error.to_string())?;
    let mut request = acyclic_machines::CreateMachine::new(IdempotencyKey::new(), image, [8; 32]);
    request.expiration = ExpirationPolicy::MaxAge(Duration::from_secs(2 * 3_600));
    let MutationOutcome::Created(machine) = provider
        .create(request)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("wrong create outcome".into());
    };
    let sandbox_id = map::machine_sandbox(machine.id).map_err(|error| error.to_string())?;
    let ends = |sandbox: &acyclic_machines_prized::api::Sandbox| {
        sandbox
            .expires_at
            .as_deref()
            .and_then(map::parse_rfc3339_ms)
            .unwrap_or(0)
    };
    let deadline = machine.created_at_unix_ms + 2 * 3_600_000;
    let at_create = ends(
        &provider
            .api()
            .get(&sandbox_id)
            .await
            .map_err(|e| e.to_string())?,
    );
    let later = map::format_rfc3339_ms(deadline + 3_600_000);
    let moved = ends(
        &provider
            .api()
            .timeout(&sandbox_id, &later)
            .await
            .map_err(|e| e.to_string())?,
    );
    provider
        .suspend(machine.id, IdempotencyKey::new())
        .await
        .map_err(|error| error.to_string())?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    provider
        .wake(machine.id, IdempotencyKey::new())
        .await
        .map_err(|error| error.to_string())?;
    let after_wake = ends(
        &provider
            .api()
            .get(&sandbox_id)
            .await
            .map_err(|e| e.to_string())?,
    );
    eprintln!(
        "deadline {deadline}: Prized's end {at_create} at create ({} ms off), {moved} moved later ({} ms off), {after_wake} after the wake ({} ms off)",
        i128::from(at_create) - i128::from(deadline),
        i128::from(moved) - i128::from(deadline),
        i128::from(after_wake) - i128::from(deadline)
    );
    if after_wake > deadline || after_wake + 1_000 < deadline {
        return Err(format!(
            "the wake left the end at {after_wake}, past the deadline {deadline}"
        ));
    }
    Ok(())
}

async fn shell(
    provider: &PrizedProvider,
    machine: MachineId,
    script: &str,
) -> Result<String, String> {
    let command = ExecCommand::new(vec!["sh".into(), "-c".into(), script.into()], 30_000)
        .map_err(|error| error.to_string())?;
    let frames: Vec<_> = provider
        .exec(machine, command)
        .await
        .map_err(|error| error.to_string())?
        .collect()
        .await;
    let mut output = Vec::new();
    for frame in frames {
        match frame.map_err(|error| error.to_string())? {
            ExecOutput::Data(data) => output.extend(data),
            ExecOutput::ExitCode(0) => {
                return Ok(String::from_utf8_lossy(&output).trim().to_owned());
            }
            ExecOutput::ExitCode(code) => return Err(format!("{script:?} exited {code}")),
        }
    }
    Err("no exit code".into())
}

fn children(outcome: MutationOutcome) -> Result<Vec<MachineObservation>, String> {
    match outcome {
        MutationOutcome::MachineForked { children, .. } => Ok(children),
        other => Err(format!("wrong outcome {other:?}")),
    }
}

/// One running machine with a counter that lives only in a process, forked into eight
/// children in one call; then the same eight made as a doubling tree (one child per node per
/// round, four rounds), the fan-out a provider needs when a VM forks one child at a time.
#[tokio::test]
#[ignore = "needs PRIZED_API_KEY and creates seventeen short-lived 1 vCPU sandboxes"]
async fn fan_out_eight_children() {
    let provider = provider("fan-out");
    let outcome = fan_out(&provider).await;
    sweep(&provider).await;
    outcome.unwrap_or_else(|error| panic!("{error}"));
}

async fn fan_out(provider: &PrizedProvider) -> Result<(), String> {
    let image = Image::custom([7; 32]).map_err(|error| error.to_string())?;
    let request = acyclic_machines::CreateMachine::new(IdempotencyKey::new(), image, [8; 32]);
    let MutationOutcome::Created(parent) = provider
        .create(request)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("wrong create outcome".into());
    };
    shell(
        provider,
        parent.id,
        "nohup sh -c 'i=0; while :; do i=$((i+1)); echo $i > /tmp/tick; sleep 0.1; done' >/dev/null 2>&1 &",
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let started = Instant::now();
    let forked = provider
        .fork_machine(
            parent.id,
            NonZeroU32::new(8).ok_or("eight")?,
            IdempotencyKey::new(),
        )
        .await
        .map_err(|error| error.to_string())?;
    let one_call = started.elapsed();
    let one_call_children = children(forked)?;
    for child in &one_call_children {
        // The loop kept counting in every child: memory and processes were copied live.
        let first = shell(provider, child.id, "cat /tmp/tick").await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let later = shell(provider, child.id, "cat /tmp/tick").await?;
        eprintln!("child {}: tick {first} then {later}", child.id);
        if later.parse::<u64>().unwrap_or(0) <= first.parse::<u64>().unwrap_or(u64::MAX) {
            return Err(format!("the counter is not running in child {}", child.id));
        }
    }
    eprintln!("one call, 8 running children: {} ms", one_call.as_millis());
    for (index, child) in one_call_children.iter().enumerate() {
        let key = IdempotencyKey::new();
        provider
            .destroy_machine(child.id, key)
            .await
            .map_err(|error| format!("destroy child {index}: {error}"))?;
    }

    let started = Instant::now();
    let mut nodes = vec![parent.id];
    let mut made = 0_usize;
    let mut rounds = 0;
    while made < 8 {
        let batch: Vec<MachineId> = nodes.iter().copied().take(8 - made).collect();
        let forks = batch
            .iter()
            .map(|node| provider.fork_machine(*node, NonZeroU32::MIN, IdempotencyKey::new()));
        for outcome in futures::future::join_all(forks).await {
            let child = children(outcome.map_err(|error| error.to_string())?)?;
            nodes.extend(child.iter().map(|value| value.id));
            made += 1;
        }
        rounds += 1;
    }
    eprintln!(
        "doubling tree, 8 running children in {rounds} rounds: {} ms",
        started.elapsed().as_millis()
    );
    Ok(())
}
