use super::*;
use crate::*;

fn machine() -> Result<MachineObservation, ProviderError> {
    Ok(MachineObservation {
        id: MachineId::new(),
        state: MachineState::Starting,
        contract: MachineContract {
            image: Image::custom([7; 32])?,
            capabilities: Default::default(),
            compatibility: CompatibilityPolicy::BestEffort,
            compatibility_revision: [1; 32],
            suspension: SuspensionPolicy::Manual,
            expiration: ExpirationPolicy::Never,
            network_policy_digest: [8; 32],
            budgets: Budgets::default(),
        },
        endpoints: Vec::new(),
        last_checkpoint: None,
        created_at_unix_ms: 1,
        changed_at_unix_ms: 1,
    })
}

#[test]
fn terminal_cancel_and_rollback_cannot_reclaim_a_successful_create()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = OperationRegistry::default();
    let key = IdempotencyKey::new();
    let Admission::Fresh(operation) = registry.admit(key, &"create")? else {
        return Err("expected fresh admission".into());
    };
    let observation = machine()?;
    assert!(registry.bind_created_machine(operation, "owned", observation.clone()));
    registry.complete(operation, MutationOutcome::Created(observation.clone()));
    let (terminal, resources) = registry.cancel(operation).ok_or("missing operation")?;
    assert_eq!(terminal.phase, OperationPhase::Succeeded);
    assert!(resources.is_empty());
    assert!(registry.take_created(operation).is_empty());
    assert!(
        matches!(registry.admit(key, &"create")?, Admission::Replay(outcome) if *outcome == MutationOutcome::Created(observation))
    );
    Ok(())
}

#[test]
fn cancellation_prevents_late_create_and_readiness_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = OperationRegistry::default();
    let Admission::Fresh(operation) = registry.admit(IdempotencyKey::new(), &"create")? else {
        return Err("expected fresh admission".into());
    };
    let mut observation = machine()?;
    assert!(registry.bind_created_machine(operation, "owned", observation.clone()));
    let (_, resources) = registry.cancel(operation).ok_or("missing operation")?;
    assert_eq!(resources, ["owned"]);
    registry.mark_destroyed(observation.id, 2);
    observation.state = MachineState::Running;
    assert!(!registry.observe_pending_machine(operation, observation.clone()));
    assert!(!registry.bind_created_machine(operation, "late", observation.clone()));
    registry.observe_machine(observation.clone(), 3);
    let current = registry.machine(observation.id).ok_or("missing machine")?;
    assert_eq!(current.state, MachineState::Destroyed);
    assert_eq!(current.changed_at_unix_ms, 2);
    assert_eq!(registry.events(observation.id, None, 16).0.len(), 2);
    Ok(())
}

#[test]
fn state_reads_keep_current_contract_and_cannot_resurrect_destroyed_machine()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = OperationRegistry::default();
    let mut observation = machine()?;
    registry.observe_machine(observation.clone(), 1);
    observation.contract.suspension =
        SuspensionPolicy::AfterIdle(std::time::Duration::from_secs(3));
    registry.observe_machine(observation.clone(), 2);
    let updated = registry
        .observe_state(observation.id, MachineState::Running, 3)
        .ok_or("missing machine")?;
    assert_eq!(updated.contract, observation.contract);
    registry.mark_destroyed(observation.id, 4);
    let terminal = registry
        .observe_state(observation.id, MachineState::Running, 5)
        .ok_or("missing machine")?;
    assert_eq!(terminal.state, MachineState::Destroyed);
    assert_eq!(terminal.changed_at_unix_ms, 4);
    Ok(())
}
