use super::*;

fn dependency(byte: u8) -> Dependency {
    Dependency {
        region: DependencyRegion::FileRecord(FileId::from_bytes([byte; 16])),
        expected: DependencyState::Absent,
    }
}

fn model<const SPLIT: bool>(reject: bool) {
    loom::model(move || {
        let ledger = Arc::new(DependencyLedger::sharing_with_custody(
            Proof::new(CheckoutDependencies::new([], [], 8).expect("empty proof")),
            Arc::new(ProofDataCustody::default()),
        ));
        let writer = ledger.clone();
        let mutation = loom::thread::spawn(move || {
            if SPLIT {
                // Restore the old snapshot/unlock/install transaction boundary.
                let mut prepared = writer.proof().dependencies.clone();
                loom::thread::yield_now();
                let extension = prepared
                    .prepare_mutations(vec![dependency(1)], 8)
                    .expect("mutation");
                prepared
                    .commit_with_data_custody::<WorkError>(extension, None, |_, _, _, _| Ok(()))
                    .expect("unbounded work");
                if !reject {
                    writer.proof().dependencies = prepared;
                }
            } else {
                // Actual production proof lock and commit path. Its lock order is
                // proof -> custody / original-work; neither acquires proof in reverse.
                let mut proof = writer.proof();
                let extension = proof
                    .prepare_mutations(vec![dependency(1)], 8)
                    .expect("mutation");
                let result = proof.commit_with_data_custody_then(
                    extension,
                    Some(&writer.data_custody),
                    |_, _, _, _| Ok(()),
                    || {
                        if reject {
                            Err(WorkError::Overflow)
                        } else {
                            Ok(())
                        }
                    },
                );
                assert_eq!(result.is_err(), reject);
            }
        });
        let reader = ledger.clone();
        let observation = loom::thread::spawn(move || {
            let token = CancellationToken::new();
            let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &token);
            reader
                .extend_observations(
                    vec![dependency(2)],
                    &mut KernelOwnedAllocation::Standalone,
                    8,
                    &mut accounting,
                    &token,
                )
                .expect("observation");
        });
        mutation.join().expect("mutation thread");
        observation.join().expect("observation thread");
        let proof = ledger.proof();
        assert!(
            proof.observes(&dependency(2).region),
            "committing a delta cannot lose an observation"
        );
        assert_eq!(proof.len(), if reject { 1 } else { 2 });
    });
}

#[test]
fn mutation_and_observation_commit_under_one_proof_lock() {
    model::<false>(false);
}

#[test]
fn rejected_root_handoff_preserves_concurrent_observation() {
    model::<false>(true);
}

#[test]
#[should_panic(expected = "committing a delta cannot lose an observation")]
fn split_snapshot_install_loses_observation_control() {
    model::<true>(false);
}
