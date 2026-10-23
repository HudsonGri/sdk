use super::*;
use std::cell::Cell;

#[test]
fn backend_conditions_exhaust_presence_tail_and_forged_head_without_unneeded_reads()
-> Result<(), StreamError> {
    let path = StreamPath::new("facts")?;
    let mut absence_control_failed = false;
    let mut head_control_failed = false;
    for actual in [None, Some(0), Some(1), Some(2)] {
        for expected in 0..=2 {
            for matching_head in [false, true] {
                let expected_head = CommitId::from_bytes([1; 32]);
                let actual_head = CommitId::from_bytes([u8::from(matching_head); 32]);
                let conditions = [
                    CommitCondition::Tail {
                        path: path.clone(),
                        expected,
                    },
                    CommitCondition::Head {
                        path: path.clone(),
                        expected,
                        commit_id: expected_head,
                    },
                    CommitCondition::Absent { path: path.clone() },
                ];
                let tail_matches = actual == Some(expected);
                let accepted = [
                    tail_matches,
                    tail_matches && matching_head,
                    actual.is_none(),
                ];
                for (index, (condition, accepted)) in conditions.iter().zip(accepted).enumerate() {
                    let reads = Cell::new(0);
                    let conflict = condition_conflict(condition, actual, || {
                        reads.set(reads.get() + 1);
                        Ok(actual_head)
                    })?;
                    assert_eq!(conflict.is_none(), accepted);
                    assert_eq!(reads.get(), usize::from(index == 1 && tail_matches));
                    if index == 0 {
                        // A cold lookup must preserve missing vs existing-empty.
                        absence_control_failed |= (actual.unwrap_or(0) == expected) != accepted;
                    }
                    if index == 1 {
                        // A matching sequence is not proof of a matching head.
                        head_control_failed |= tail_matches != accepted;
                        if tail_matches && !matching_head {
                            assert_eq!(
                                conflict,
                                Some(CommitConflict::Head {
                                    path: path.clone(),
                                    expected: expected_head,
                                    actual: actual_head,
                                })
                            );
                        }
                    }
                }
            }
        }
    }
    assert!(
        absence_control_failed,
        "missing-as-zero control escaped the state space"
    );
    assert!(
        head_control_failed,
        "tail-only Head control escaped the state space"
    );
    Ok(())
}

#[test]
fn backend_identity_and_time_survive_working_set_replacement() {
    let digest = [17; 32];
    let mut identities = std::collections::BTreeSet::new();
    let mut reset_control_duplicates = 0;
    let mut previous = 0;
    let mut clock_control_regresses = false;
    for decision in 1..=1_000 {
        let id = commit_identity(decision, digest);
        assert!(identities.insert(id));
        // A freshly seeded per-operation state would reset its global ordinal.
        reset_control_duplicates += usize::from(commit_identity(1, digest) == id);
        let sampled_millis = if decision % 2 == 0 { 1 } else { decision };
        let time = committed_time(previous, sampled_millis);
        assert!(time >= previous);
        assert!(time >= sampled_millis * 1_000);
        clock_control_regresses |= sampled_millis * 1_000 < previous;
        previous = time;
    }
    assert_eq!(reset_control_duplicates, 1);
    assert!(clock_control_regresses);
    assert_eq!(committed_time(0, u64::MAX), u64::MAX);
    assert_eq!(committed_time(u64::MAX, 0), u64::MAX);
}
