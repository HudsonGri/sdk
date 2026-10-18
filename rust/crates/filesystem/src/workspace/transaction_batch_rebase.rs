use super::*;

/// How a transaction stages its directories and files.
#[derive(Clone, Copy, Debug)]
enum Stage {
    /// The reference: one `create_dirs` per directory, then one write per file.
    Serial,
    Batched,
    /// Negative control: batched, plus the file record of the existing `/out`
    /// prefix, which the batch once recorded for every query.
    BatchedWithPrefixRecord,
}

const DIRECTORIES: [&str; 3] = ["/out/a", "/out/a/b", "/out/c"];
const FILES: [&str; 5] = [
    "/out/existing",
    "/out/a/x",
    "/out/a/b/y",
    "/out/c/z",
    "/out/w",
];
/// What a concurrent writer commits first: sibling additions, the same names
/// the transaction stages, and an unrelated root file.
const SIBLINGS: [&str; 6] = [
    "/out/sibling",
    "/out/a/s",
    "/out/existing",
    "/out/a/x",
    "/keep",
    "/out/c/",
];

#[derive(Clone, Debug)]
struct Case {
    /// Path and mutation chunk bound; small bounds split directory prefixes
    /// and files across lookup chunks.
    maximum: u32,
    directories: Vec<usize>,
    files: Vec<(usize, Vec<u8>)>,
    siblings: Vec<usize>,
}

fn parent(path: &str) -> Option<&str> {
    let parent = &path[..path.rfind('/')?];
    (parent != "/out" && !parent.is_empty()).then_some(parent)
}

/// The staged directory list: chosen directories, then each file's parent
/// chain, so every case reaches the commit.
fn directories(case: &Case) -> Vec<&'static str> {
    let mut directories: Vec<_> = case.directories.iter().map(|i| DIRECTORIES[*i]).collect();
    for (file, _) in &case.files {
        let mut next = parent(FILES[*file]);
        while let Some(directory) = next {
            directories.push(directory);
            next = parent(directory);
        }
    }
    directories
}

async fn stage(
    transaction: &mut MemoryTransaction,
    stage: Stage,
    case: &Case,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    let directories = directories(case);
    let files: Vec<_> = case
        .files
        .iter()
        .map(|(file, bytes)| {
            (
                FILES[*file],
                acyclic_stream::OwnedBytes::copy_from_slice(bytes),
            )
        })
        .collect();
    if matches!(stage, Stage::Serial) {
        for directory in &directories {
            transaction
                .create_dirs(directory, accounting, cancellation)
                .await?;
        }
        for (path, bytes) in &files {
            super::batch_original::write(
                transaction,
                path,
                bytes.clone(),
                accounting,
                cancellation,
            )
            .await?;
        }
        return Ok(());
    }
    if matches!(stage, Stage::BatchedWithPrefixRecord) {
        let prefix = transaction.mutation_path("/out", accounting, cancellation)?;
        let (lookup, allocation) = transaction
            .checkout
            .lookup_batch_no_follow_with_accounting(
                std::slice::from_ref(&prefix.value),
                0,
                false,
                accounting,
                cancellation,
            )
            .await?;
        let bytes = lookup.retained_allocation_bytes;
        drop(lookup);
        crate::kernel::retire_allocation(allocation, bytes, accounting)?;
        prefix.retire(accounting)?;
    }
    transaction
        .write_batch_with_accounting::<true>(&directories, &files, accounting, cancellation)
        .await
}

async fn initialize_bounded(
    maximum: u32,
    existing: &[&str],
) -> (MemoryWorkspace, MemoryTransaction) {
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    config.limits.maximum_paths_per_batch = maximum;
    config.limits.maximum_mutations_per_batch = maximum;
    let workspace = value!(crate::Fs::memory().create_workspace_with_config(
        "staging-rebase",
        config,
        budget,
        &cancellation
    ))
    .unwrap();
    let mut setup = value!(workspace.begin_transaction(
        crate::IdempotencyKey::from_bytes([1; 16]),
        budget,
        &cancellation
    ))
    .unwrap();
    value!(setup.create_dir_all("/out", budget, &cancellation)).unwrap();
    for directory in existing {
        value!(setup.create_dir_all(directory, budget, &cancellation)).unwrap();
    }
    setup.write_text("/out/existing", "original").await.unwrap();
    setup.write_text("/keep", "untouched").await.unwrap();
    commit(&mut setup).await.unwrap();
    let transaction = value!(workspace.begin_transaction(
        crate::IdempotencyKey::from_bytes([2; 16]),
        budget,
        &cancellation
    ))
    .unwrap();
    (workspace, transaction)
}

/// Commits as the harness host does: a head conflict retries once when the
/// candidate's recorded observations prove it still rebases.
async fn commit_rebasing(transaction: &mut MemoryTransaction) -> bool {
    let cancellation = CancellationToken::new();
    for _ in 0..2 {
        let committed = value!(transaction.commit::<WorkspaceError, _, _>(
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| async {
                Ok(crate::OperationReceipt {
                    value: (),
                    work: Default::default(),
                })
            }
        ))
        .unwrap();
        match committed {
            crate::TransactionCommit::Committed(_) => return true,
            crate::TransactionCommit::Conflict { .. } => {}
            _ => panic!("unexpected publication outcome"),
        }
        match value!(transaction.rebase(1, WorkBudget::UNBOUNDED, &cancellation)).unwrap() {
            crate::TransactionRebase::Rebased(_) => {}
            crate::TransactionRebase::Conflicted { .. } => return false,
        }
    }
    panic!("a rebased candidate conflicted again without a new writer")
}

/// Whether the staged commit lands after the siblings, and every file after it.
type Outcome = (bool, BTreeMap<&'static str, Result<Vec<u8>, String>>);

async fn outcome(stage_as: Stage, case: &Case) -> Outcome {
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let (workspace, mut transaction) = initialize_bounded(case.maximum, &[]).await;
    let mut accounting = KernelAccounting::standalone(budget, &cancellation);
    let staged = stage(
        &mut transaction,
        stage_as,
        case,
        &mut accounting,
        &cancellation,
    )
    .await;
    assert!(staged.is_ok(), "{stage_as:?} staging failed: {staged:?}");
    let mut sibling = value!(workspace.begin_transaction(
        crate::IdempotencyKey::from_bytes([3; 16]),
        budget,
        &cancellation
    ))
    .unwrap();
    for index in &case.siblings {
        let path = SIBLINGS[*index];
        if let Some(directory) = path.strip_suffix('/') {
            value!(sibling.create_dir_all(directory, budget, &cancellation)).unwrap();
        } else {
            if let Some(directory) = parent(path) {
                value!(sibling.create_dir_all(directory, budget, &cancellation)).unwrap();
            }
            sibling.write_text(path, "beside").await.unwrap();
        }
    }
    commit(&mut sibling).await.unwrap();
    let committed = commit_rebasing(&mut transaction).await;
    let mut files = BTreeMap::new();
    for path in FILES.into_iter().chain(SIBLINGS) {
        let read = workspace
            .read(path, 4096, budget, &cancellation)
            .await
            .map(|read| read.value.to_vec())
            .map_err(|failure| format!("{:?}", failure.error));
        files.insert(path, read);
    }
    (committed, files)
}

/// The staged transaction must rebase over a concurrent commit exactly when
/// the serial reference does, and leave the same files: no false conflict and
/// no missed one.
fn check(stage_as: Stage, case: &Case) -> TestCaseResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let serial = runtime.block_on(outcome(Stage::Serial, case));
    let staged = runtime.block_on(outcome(stage_as, case));
    prop_assert_eq!(
        &staged,
        &serial,
        "{:?} diverges from serial for {:?}",
        stage_as,
        case
    );
    Ok(())
}

fn cases() -> impl Strategy<Value = Case> {
    (
        prop::sample::select(vec![2_u32, 3, 1024]),
        prop::collection::vec(0..DIRECTORIES.len(), 0..3),
        prop::collection::vec(
            (0..FILES.len(), prop::collection::vec(any::<u8>(), 0..8)),
            1..4,
        ),
        prop::collection::vec(0..SIBLINGS.len(), 1..3),
    )
        .prop_map(|(maximum, directories, files, siblings)| Case {
            maximum,
            directories,
            files,
            siblings,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn batched_staging_rebases_exactly_like_serial(case in cases()) {
        check(Stage::Batched, &case)?;
    }
}

#[test]
fn recording_the_prefix_file_record_fails_the_rebase_property() {
    let case = Case {
        maximum: 1024,
        directories: vec![0],
        files: vec![(0, b"exact".to_vec())],
        siblings: vec![0],
    };
    check(Stage::Batched, &case).unwrap();
    let error = check(Stage::BatchedWithPrefixRecord, &case).unwrap_err();
    assert!(error.to_string().contains("diverges from serial"));
}

/// Every path batch a batched staging observes, in walk order, over a base
/// that already holds `existing` directories.
fn reservations(case: &Case, existing: &[&str]) -> Vec<crate::kernel::DependencySlots> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    crate::kernel::DEPENDENCY_SLOTS.set(Some(Vec::new()));
    runtime.block_on(async {
        let cancellation = CancellationToken::new();
        let (_workspace, mut transaction) = initialize_bounded(case.maximum, existing).await;
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        stage(
            &mut transaction,
            Stage::Batched,
            case,
            &mut accounting,
            &cancellation,
        )
        .await
        .unwrap();
    });
    crate::kernel::DEPENDENCY_SLOTS.take().unwrap_or_default()
}

/// Each walk reserves what `reserve` names, never fills past it, and fills
/// every slot when all its queries resolve: a walk that stops at a missing
/// name records fewer bindings, never more.
fn exact(
    slots: &[crate::kernel::DependencySlots],
    reserve: impl Fn(&crate::kernel::DependencySlots) -> Option<usize>,
) -> TestCaseResult {
    prop_assert!(!slots.is_empty(), "staging observed no path batch");
    for slot in slots {
        prop_assert_eq!(slot.capacity, slot.reserved, "reallocated {:?}", slot);
        prop_assert!(slot.filled <= slot.reserved, "overfilled {:?}", slot);
        if slot.resolved {
            prop_assert_eq!(slot.filled, slot.reserved, "unfilled slots in {:?}", slot);
        }
        prop_assert_eq!(
            Some(slot.reserved),
            reserve(slot),
            "reservation differs for {:?}",
            slot
        );
    }
    Ok(())
}

fn reserved_by_batch(slot: &crate::kernel::DependencySlots) -> Option<usize> {
    crate::kernel::PathObservation::Paths {
        records_from: slot.records_from,
    }
    .dependency_slots(slot.components, slot.count)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn batched_staging_reserves_exactly_the_dependencies_it_records(
        case in cases(),
        existing in prop::sample::subsequence(DIRECTORIES.to_vec(), 0..=DIRECTORIES.len()),
    ) {
        exact(&reservations(&case, &existing), reserved_by_batch)?;
    }
}

/// Negative control: the former reservation, a record slot for every query,
/// leaves one slot per resolved directory prefix unfilled.
#[test]
fn reserving_a_record_slot_per_prefix_fails_the_exact_property() {
    let case = Case {
        maximum: 1024,
        directories: vec![0, 1],
        files: vec![(0, b"exact".to_vec())],
        siblings: vec![0],
    };
    let slots = reservations(&case, &DIRECTORIES);
    assert!(
        slots
            .iter()
            .any(|slot| slot.resolved && slot.records_from > 0),
        "the control needs a resolved walk with prefixes: {slots:?}"
    );
    exact(&slots, reserved_by_batch).unwrap();
    let former: Vec<_> = slots
        .iter()
        .map(|slot| crate::kernel::DependencySlots {
            reserved: slot.components + slot.count,
            capacity: slot.components + slot.count,
            ..*slot
        })
        .collect();
    let error = exact(&former, |slot| slot.components.checked_add(slot.count)).unwrap_err();
    assert!(error.to_string().contains("unfilled slots"));
}
