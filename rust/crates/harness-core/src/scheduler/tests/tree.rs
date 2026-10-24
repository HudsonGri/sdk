use super::*;

#[test]
fn child_pages_seek_by_parent_local_reference() -> Result<()> {
    let scheduler = two_slot_parent(Orchestration::Join)?;
    let entries = scheduler
        .children_after(id(1), Some("a"))
        .take(1)
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "b");
    assert_eq!(entries[0].1.spec.operation_id, id(3));
    assert_eq!(scheduler.child(id(1), "b"), scheduler.operation(id(3)));
    assert_eq!(scheduler.parent(id(3)), scheduler.operation(id(1)));
    assert!(scheduler.parent(id(1)).is_none());
    assert!(scheduler.children_after(id(1), Some("b")).next().is_none());
    assert!(scheduler.children_after(id(2), None).next().is_none());
    Ok(())
}

// Derive the reference only from immutable declarations, live reservations and
// retained dispatch identities, never from the counters being checked.
pub(super) fn assert_tree(scheduler: &Scheduler) {
    assert_eq!(scheduler.tree.len(), scheduler.operations.len());
    for (id, entry) in &scheduler.tree {
        let expected = scheduler
            .completion_order
            .iter()
            .copied()
            .filter(|completed| {
                scheduler.operations[completed]
                    .spec
                    .parent
                    .as_ref()
                    .is_some_and(|parent| parent.operation_id == *id)
            })
            .collect::<Vec<_>>();
        let actual = scheduler
            .completed_children(*id)
            .take(scheduler.operations.len() + 1)
            .map(|child| child.spec.operation_id)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "parent completion order");
        let mut root = *id;
        let mut depth = 0;
        while let Some(parent) = &scheduler.operations[&root].spec.parent {
            root = parent.operation_id;
            depth += 1;
            assert!(
                depth <= scheduler.operations.len(),
                "ancestry must be acyclic"
            );
        }
        assert_eq!((entry.root, entry.depth as usize), (root, depth));
        if root != *id {
            continue;
        }
        let members = scheduler
            .tree
            .iter()
            .filter(|(_, child)| child.root == root);
        let mut total = 0;
        let mut active = 0;
        let mut model_steps = 0;
        for (member, _) in members {
            total += 1;
            active += u64::from(scheduler.operations[member].reservation.is_some());
            if let Some(claims) = scheduler.model_claims.get(member) {
                let used = claims
                    .attempts
                    .values()
                    .map(|steps| steps.len() as u64)
                    .sum::<u64>();
                assert_eq!(claims.used, used);
                model_steps += used;
            }
        }
        assert_eq!(
            (entry.total, entry.active, entry.model_steps),
            (total, active, model_steps)
        );
    }
}

#[test]
#[should_panic(expected = "assertion `left == right` failed")]
fn negative_control_dropped_reservation_counter_is_detected() {
    let mut scheduler = two_slot_parent(Orchestration::Join).unwrap();
    let root = scheduler.tree.get_mut(&id(1)).unwrap();
    root.active = 0;
    assert_tree(&scheduler);
}

#[test]
#[should_panic(expected = "parent completion order")]
fn negative_control_dropped_completion_index_is_detected() {
    let mut scheduler = two_slot_parent(Orchestration::Race).unwrap();
    complete(&mut scheduler, id(2), Outcome::Cancelled, Some(fence(0))).unwrap();
    scheduler.tree.get_mut(&id(1)).unwrap().first_completed = None;
    assert_tree(&scheduler);
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn one_hundred_thousand_children_do_not_enlarge_transition_work() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let root = id(1);
    let mut scheduler = session(
        spec(root, Orchestration::Join)?,
        SessionLimits {
            total_tasks: 100_002,
            active_tasks: 1,
            depth: 1,
            model_steps: 2,
        },
    )?;
    let template = spec(id(2), Orchestration::Leaf)?;
    for index in 0_u64..100_000 {
        let mut spec = template.clone();
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        spec.operation_id = OperationId::from_bytes(bytes);
        let slot = index.to_string();
        spec.parent = Some(ParentLink {
            operation_id: root,
            slot: slot.clone(),
        });
        // Population setup uses the actual projection mutator; the measured
        // transition below enters through public validation and application.
        scheduler.apply_declaration(
            SchedulerEvent::Declared {
                spec: Box::new(spec),
            },
            Some(slot),
            &mut None,
        )?;
    }
    let budget = WorkBudget {
        items_examined: 256,
        ..native_budget()
    };
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let event = SchedulerEvent::Declared {
        spec: Box::new(child(2, 1, "measured")?),
    };
    scheduler.apply(&event, &mut Some(&mut io.history_io()))?;
    assert!(scheduler.child(root, "measured").is_some());
    scheduler.require_session_capacity(id(2), &mut Some(&mut io.history_io()))?;
    assert_eq!(
        scheduler.session_root(id(2), &mut Some(&mut io.history_io()))?,
        (root, 1)
    );
    // Restore the former session-wide scan: the same admitted budget rejects it.
    let mut meter = io.history_io();
    let mut observer = Some(&mut meter as &mut dyn HistoryIo);
    assert!(
        scheduler
            .operations
            .keys()
            .try_for_each(|_| observe(&mut observer))
            .is_err()
    );
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        allocation_operations: 1_024,
        peak_allocation_bytes: 1_048_576,
        ..Default::default()
    }
}
