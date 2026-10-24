use super::*;

pub(super) fn validate_join_side_selections(
    conflicts: &[MergeConflict],
    truncated: bool,
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(), DrivenJoinError> {
    if truncated || conflicts.len() != selections.len() {
        return Err(DrivenJoinError::StaleCandidate);
    }
    // Kernel traversal order interleaves file and binding conflicts. Sort only
    // borrowed keys with the shared metered sorter, then walk both sets once.
    let mut ordered = crate::kernel::Owned::allocate(conflicts.len(), accounting)
        .map_err(WorkspaceError::from)?;
    let validation = (|| {
        for (ordinal, key) in conflicts.iter().enumerate() {
            let copied = crate::WorkCounters {
                items_examined: 1,
                bytes_copied: std::mem::size_of::<crate::kernel::IndexedKey<'_, MergeConflict>>()
                    as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied).map_err(WorkspaceError::from)?;
            ordered
                .value
                .push(crate::kernel::IndexedKey { ordinal, key });
            accounting
                .record_completed(copied)
                .map_err(WorkspaceError::from)?;
        }
        crate::kernel::sort_indexed(&mut ordered.value, accounting)
            .map_err(WorkspaceError::from)?;
        for (conflict, key) in ordered.value.iter().zip(selections.keys()) {
            accounting.charge_items(1).map_err(WorkspaceError::from)?;
            if conflict.key != key {
                return Err(DrivenJoinError::StaleCandidate);
            }
        }
        Ok(())
    })();
    ordered.retire(accounting).map_err(WorkspaceError::from)?;
    validation
}

#[cfg(test)]
mod selection_tests {
    use super::*;
    use crate::facade::FsWork as _;

    fn conflict(index: u128) -> MergeConflict {
        MergeConflict::File(crate::FileId::from_bytes(index.to_be_bytes()))
    }

    #[test]
    fn shuffled_conflicts_use_the_shared_sorter_and_reject_duplicate_regions() {
        let cancellation = crate::CancellationToken::new();
        let budget = crate::performance::work_budget(32_768, 1024 * 1024);
        let selections = (0..512)
            .map(|i| (conflict(i), ConflictSide::Ours))
            .collect();
        let conflicts: Vec<_> = (0..512).rev().map(conflict).collect();
        let mut accounting = budget.accounting(&cancellation);
        validate_join_side_selections(&conflicts, false, &selections, &mut accounting).unwrap();
        assert!(accounting.spent().items_examined < 32_768);

        // Actual old traversal restarts at the first selection for each key.
        // The same allowance rejects it rather than hiding the quadratic work.
        let mut old = budget.accounting(&cancellation);
        let mut denied = false;
        'conflicts: for conflict in &conflicts {
            for key in selections.keys() {
                if old.charge_items(1).is_err() {
                    denied = true;
                    break 'conflicts;
                }
                if key == conflict {
                    break;
                }
            }
        }
        assert!(denied, "restarted key scans escaped the original allowance");

        let mut duplicate = conflicts;
        duplicate.pop();
        duplicate.push(conflict(511));
        let mut accounting = budget.accounting(&cancellation);
        assert!(matches!(
            validate_join_side_selections(&duplicate, false, &selections, &mut accounting),
            Err(DrivenJoinError::StaleCandidate)
        ));
    }

    #[test]
    fn one_hundred_thousand_sorted_selections_have_linear_validation_work() {
        let cancellation = crate::CancellationToken::new();
        let selections = (0..100_000)
            .map(|i| (conflict(i), ConflictSide::Theirs))
            .collect();
        let conflicts: Vec<_> = (0..100_000).map(conflict).collect();
        let budget = crate::performance::work_budget(300_000, 4 * 1024 * 1024);
        let mut accounting = budget.accounting(&cancellation);
        validate_join_side_selections(&conflicts, false, &selections, &mut accounting).unwrap();
        assert_eq!(accounting.spent().items_examined, 299_999);
    }
    #[test]
    fn equal_lengths_and_membership_cannot_authorize_duplicate_conflicts() {
        let conflicts = vec![conflict(0), conflict(0)];
        let selections = BTreeMap::from([
            (conflict(0), ConflictSide::Ours),
            (conflict(1), ConflictSide::Theirs),
        ]);
        // The former ordinary validator accepted this set despite an omitted
        // conflict and an extra selection. Exact region equality must reject it.
        assert_eq!(conflicts.len(), selections.len());
        assert!(conflicts.iter().all(|key| selections.contains_key(key)));
        let cancellation = crate::CancellationToken::new();
        let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
        assert!(matches!(
            validate_join_side_selections(&conflicts, false, &selections, &mut accounting),
            Err(DrivenJoinError::StaleCandidate)
        ));
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]
        #[test]
        fn every_host_requires_the_same_exact_conflict_set(
            keys in proptest::collection::vec(0u8..16, 0..32),
            selected in proptest::collection::btree_set(0u8..16, 0..16),
            truncated in proptest::bool::ANY,
        ) {
            let expected = !truncated && keys.len() == selected.len()
                && keys.iter().copied().collect::<BTreeSet<_>>() == selected;
            let conflicts: Vec<_> = keys.into_iter().map(|key| conflict(u128::from(key))).collect();
            let selections = selected.into_iter()
                .map(|key| (conflict(u128::from(key)), ConflictSide::Ours)).collect();
            let cancellation = crate::CancellationToken::new();
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
            proptest::prop_assert_eq!(
                validate_join_side_selections(&conflicts, truncated, &selections, &mut accounting).is_ok(),
                expected,
            );
            #[cfg(not(target_arch = "wasm32"))]
            {
                let work = crate::NativeMaterializationWork::new(
                    crate::performance::BOUNDARY_BUDGET, &cancellation,
                ).unwrap_or_else(|(error, _)| panic!("{error:?}"));
                let mut accounting = crate::kernel::allocation::KernelAccounting::native(&work, &cancellation);
                proptest::prop_assert_eq!(
                    validate_join_side_selections(&conflicts, truncated, &selections, &mut accounting).is_ok(),
                    expected,
                );
                proptest::prop_assert_eq!(work.receipt().backend_write_operations, 0);
            }
        }
    }
}
