use super::*;
use proptest::prelude::*;

fn file(reference: &Component, kind: ChildFile, body: Vec<u8>) -> FileValue {
    FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: kind,
        },
        body,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn resume_acknowledges_only_real_completed_receive_checkpoints(turns in 1u64..9) {
        use crate::workflow::{MachineCheckpoint, MachineRegistry, ResumableMachine};
        use std::borrow::Cow;
        let seed: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../harness-fuzz/corpus/child_control/child-agent-turn"))).unwrap();
        let machine = Arc::new(crate::filesystem::ChildAgentMachine::new());
        let mut registry = MachineRegistry::default();
        registry.register(machine.clone(), &mut None).unwrap();
        let initial = MachineCheckpoint { machine: machine.identity().clone(), revision: 0, state: machine.initialize(&seed["initial"]).unwrap() };
        let mut checkpoint = registry.step(&initial, Cow::Owned(serde_json::Value::Null), None).unwrap().0;
        let task: TaskId = serde_json::from_value(seed["input"]["commands"][0]["value"]["Succeeded"]["task_id"].clone()).unwrap();
        for inputs in 1..=turns {
            let status = Some(ChildStatus { task, state: ChildState::Paused, settled: true, inputs });
            prop_assert!(!settled_turn(status, task, inputs, Some(checkpoint.revision)));
            let mut incoming = seed["input"].clone();
            incoming["commands"][0]["operation_id"] = checkpoint.state["receive"]["operation_id"].clone();
            incoming["commands"][0]["value"]["Succeeded"]["sequence"] = serde_json::json!(inputs);
            checkpoint = registry.step(&checkpoint, Cow::Owned(incoming), None).unwrap().0;
            prop_assert!(!settled_turn(status, task, inputs, Some(checkpoint.revision)));
            let mut completed = seed["model_result"].clone();
            completed["commands"][0]["operation_id"] = checkpoint.state["model"]["operation_id"].clone();
            checkpoint = registry.step(&checkpoint, Cow::Owned(completed), None).unwrap().0;
            prop_assert!(settled_turn(status, task, inputs, Some(checkpoint.revision)));
            prop_assert_eq!(checkpoint.state["stage"].as_str(), Some("receive"));
        }
    }

    #[test]
    fn lifecycle_only_resume_never_replays_a_settled_turn(
        task in any::<[u8;16]>(), inputs in 1u64..u64::MAX / 2,
    ) {
        let task = TaskId::from_bytes(task);
        let revision = Some(inputs * 2 + 1);
        prop_assert!(!settled_turn(None, task, inputs, revision));
        for state in [ChildState::Running, ChildState::Paused, ChildState::Interrupted, ChildState::Cancelled, ChildState::Archived] {
            let status = ChildStatus { task, state, settled: true, inputs };
            prop_assert_eq!(settled_turn(Some(status), task, inputs, revision), matches!(state, ChildState::Running | ChildState::Paused | ChildState::Interrupted));
            for invalid in [None, Some(1), Some(inputs * 2)] {
                prop_assert!(!settled_turn(Some(status), task, inputs, invalid));
            }
            prop_assert!(!settled_turn(Some(status), task, inputs + 1, revision));
            prop_assert!(!settled_turn(Some(status), task, u64::MAX, revision));
            let unfinished = ChildStatus { settled: false, ..status };
            prop_assert!(!settled_turn(Some(unfinished), task, inputs, revision));
            let mut other = task.into_bytes(); other[0] ^= 1;
            prop_assert!(!settled_turn(Some(status), TaskId::from_bytes(other), inputs, revision));
        }
    }

    // Exhaust the callback before, between, and after the two ordered follow
    // records. Only the follow may update cached files; completion releases its
    // worker only after the accepted status cursor has been observed.
    #[test]
    fn completed_jobs_never_hide_new_inputs(prefix in 1u64..9, suffix in 1u64..9) {
        let reference = Component::new("worker").unwrap();
        let task = TaskId::from_bytes([8;16]);
        for callback_at in 0..3 {
            let mut values = BTreeMap::new();
            let request = file(&reference, ChildFile::Request, b"work".to_vec());
            values.insert(request.key.clone(), request);
            for sequence in 1..prefix {
                let value = file(&reference, ChildFile::Inbox(Component::new(format!("message-{sequence}")).unwrap()), vec![1]);
                values.insert(value.key.clone(), value);
            }
            let status = ChildStatus { task, state: ChildState::Running, settled: false, inputs: prefix };
            let started = file(&reference, ChildFile::Status, serde_json::to_vec(&status).unwrap());
            values.insert(started.key.clone(), started);
            let mut active = BTreeSet::from([reference.clone()]);
            let mut completions = BTreeMap::new();
            let mut position = 9;
            let mut scheduled = 0;
            let mut followed = 0;
            for step in 0..3 {
                let input_changed = if step == callback_at {
                    completions.insert(reference.clone(), 10);
                    false
                } else {
                    followed += 1;
                    position += 1;
                    if followed == 1 {
                        let done = file(&reference, ChildFile::Status, serde_json::to_vec(&ChildStatus { settled: true, ..status }).unwrap());
                        values.insert(done.key.clone(), done);
                        false
                    } else {
                        for sequence in prefix..prefix+suffix {
                            let value = file(&reference, ChildFile::Inbox(Component::new(format!("message-{sequence}")).unwrap()), vec![2]);
                            values.insert(value.key.clone(), value);
                        }
                        true
                    }
                };
                let ready = release_completed(position, &mut active, &mut completions);
                if (input_changed || ready.contains(&reference))
                    && !active.contains(&reference) && needs_work(&reference, &values).unwrap() {
                    scheduled += 1;
                    active.insert(reference.clone());
                }
            }
            prop_assert!(completions.is_empty());
            prop_assert_eq!(scheduled, 1);
            prop_assert_eq!(input_count(&reference, &values), prefix + suffix);
        }
    }
}
