//! Fuzz-only driver through the real capture, queue and record serializer.
use super::*;
use crate::access::{Context, PathTrie, Policy, Resource};

/// Exercise one already decoded access input without starting a storage worker.
pub fn check(
    policy: &Policy,
    principal: Principal,
    capability: Capability,
    path: &str,
    config: crate::VolumeConfig,
    now: u64,
) {
    let Ok(mut view) = PathTrie::compile(
        policy,
        &principal,
        Resource {
            config,
            lineage: &principal.lineage,
            volume: "project",
            generation: 10,
            head: 10,
            path: "",
        },
        Context { now, approvals: 2 },
        &capability,
    ) else {
        return;
    };
    if view.prepare(path).is_err() {
        return;
    }
    let expected_principal = serde_json::to_value(&principal).unwrap();
    let expected_capability = serde_json::to_value(&capability).unwrap();
    let queue = Arc::new(queue::Queue::new(Action::ALL.len()));
    view.audit = Some(Arc::new(Binding {
        log: Arc::new(AuditLog {
            id: uuid::Uuid::nil(),
            queue: queue.clone(),
        }),
        identity: Arc::new(Identity {
            principal,
            capability_chain: capability,
            credential_fingerprint: None,
            volume_id: crate::VolumeId::from_bytes([0; 16]),
            config,
            rules: view.rules.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            _custody: None,
        }),
    }));
    for action in Action::ALL {
        assert_eq!(
            view.decide(action, path, now).unwrap(),
            view.evaluate(action, path, now)
        );
    }
    let mut records = Vec::new();
    assert!(queue.take(&mut records, Action::ALL.len()).unwrap());
    assert_eq!(records.len(), Action::ALL.len());
    for (record, action) in records.iter().zip(Action::ALL) {
        let encoded = serde_json::to_vec(record).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded["principal"], expected_principal);
        assert_eq!(decoded["capability_chain"], expected_capability);
        assert_eq!(decoded["action"], serde_json::to_value(action).unwrap());
        let decision = view.evaluate(action, path, now);
        assert_eq!(decoded["allowed"], decision.allowed);
        assert_eq!(
            decoded["rule_id"],
            serde_json::to_value(decision.rule_id).unwrap()
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&serde_json::to_vec(&decoded).unwrap())
                .unwrap(),
            decoded
        );
    }
    queue.committed(records.len()).unwrap();
    queue.flush().unwrap();
}
