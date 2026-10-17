use crate::scheduler::{OperationPhase, OperationState, WorkflowSuspension};

pub(super) fn revision(operation: &OperationState) -> Option<u64> {
    ready_for_input(
        operation.phase,
        operation.cancellation_requested,
        operation.workflow.as_ref(),
    )
    .then_some(operation.revision)
}

fn ready_for_input(
    phase: OperationPhase,
    cancelled: bool,
    slot: Option<&WorkflowSuspension>,
) -> bool {
    phase == OperationPhase::Suspended
        && !cancelled
        && slot.is_some_and(|slot| slot.waiting_command.is_none() && slot.input.is_none())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use crate::conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef};
    use proptest::prelude::*;

    fn input() -> FileRef {
        let volume = VolumeRef::new(
            crate::resources::ProviderRef::new(
                "idle-test".into(),
                "memory".into(),
                "1".into(),
                &mut None,
            )
            .unwrap(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])),
            &mut None,
        )
        .unwrap();
        FileRef::new(
            volume,
            "input".into(),
            "1".into(),
            FileDescriptor::new([1; 32], 1, "text/plain".into()).unwrap(),
            "input".into(),
            &mut None,
        )
        .unwrap()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        // Exhaust every lifecycle/slot/cancellation combination for arbitrary
        // cursor and command identities. Command dependencies never qualify.
        #[test]
        fn only_an_empty_suspended_input_slot_can_skip_worker_work(
            revision in any::<u64>(), operation in any::<[u8;16]>(),
        ) {
            let input = input();
            let command = crate::OperationId::from_bytes(operation);
            let phases = [OperationPhase::WaitingForDependencies, OperationPhase::WaitingForCapacity,
                OperationPhase::Admitted, OperationPhase::Running, OperationPhase::WaitingForChildren,
                OperationPhase::Suspended, OperationPhase::Reconciling, OperationPhase::Terminal];
            for phase in phases {
                for cancelled in [false, true] {
                    prop_assert!(!ready_for_input(phase, cancelled, None));
                    for (waiting_command, input) in [(None,None), (Some(command),None),
                        (None,Some(input.clone())), (Some(command),Some(input.clone()))] {
                        let slot = WorkflowSuspension { revision, waiting_command, input };
                        let expected = match (phase, cancelled, slot.waiting_command, slot.input.as_ref()) {
                            (OperationPhase::Suspended, false, None, None) => true,
                            _ => false,
                        };
                        prop_assert_eq!(ready_for_input(phase, cancelled, Some(&slot)), expected);
                    }
                }
            }
        }
    }
}
