use super::*;
use crate::model::FilesystemProfile;
use crate::{MemoryAuthorityBackend, MemoryObjectBackend, WorkCounters};
use acyclic_test_support::{
    AllocationInfo,
    differential::{pair, record},
    measure,
};
use proptest::prelude::*;

#[derive(Debug, Eq, PartialEq)]
enum Event {
    Observations(crate::kernel::CheckoutDependencies),
    Operation(String),
}

type TestTransaction = Transaction<MemoryAuthorityBackend, MemoryObjectBackend>;

#[derive(Clone, Copy, Debug)]
struct Case {
    depth: usize,
    existing: usize,
    collision: u8,
    concurrent: bool,
    maximum_paths: u32,
}

fn path(depth: usize) -> String {
    if depth == 0 {
        return "/".into();
    }
    (0..depth).map(|index| format!("/d{index}")).collect()
}

async fn fixture(case: Case) -> TestTransaction {
    fixture_with_profile(case, FilesystemProfile::Posix).await
}

async fn fixture_with_profile(case: Case, profile: FilesystemProfile) -> TestTransaction {
    let fs = Fs::memory();
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = profile;
    config.limits.maximum_paths_per_batch = case.maximum_paths;
    fs.create_workspace_with_operation::<_, (), WorkspaceError>(
        "directory-work",
        config,
        OperationId::from_bytes([1; 16]),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        None,
    )
    .await
    .expect("create");
    let workspace = unbounded!(fs, open_workspace("directory-work")).expect("open");
    let mut seed = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([2; 16]))
    )
    .expect("seed");
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    seed.create_dirs_individually(&path(case.existing), &mut accounting, &cancellation)
        .await
        .expect("seed directories");
    seed.write_text("/untouched", "unchanged bytes\n")
        .await
        .expect("seed bytes");
    if case.collision != 0 {
        let name = path(case.existing + 1);
        if case.collision == 1 {
            seed.write_text(&name, "not a directory")
                .await
                .expect("seed file");
        } else {
            unbounded!(seed, create_symbolic_link(&name, Bytes::from_static(b"/")))
                .expect("seed link");
        }
    }
    commit(&mut seed).await.expect("seed commit");
    unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([3; 16]))
    )
    .expect("transaction")
}

// The original production implementation remains the fallback for folded profiles
// and batches smaller than the path. Both variants start on independent providers.
async fn original(
    transaction: &mut TestTransaction,
    requested: &str,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<()>, WorkspaceError> {
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
    let result = async {
        accounting.check()?;
        let depth = PortablePath::validate(requested, transaction.checkout.volume_config().limits)
            .map_err(|error| {
                WorkspaceError::from(crate::kernel::NamespacePathError::Path(error))
            })?;
        if depth == 0 {
            return Ok(());
        }
        transaction
            .create_dirs_individually(requested, &mut accounting, &cancellation)
            .await
    }
    .await;
    result
        .map(|value| crate::OperationReceipt {
            value,
            work: accounting.spent(),
        })
        .map_err(|error| crate::OperationFailure::new(error, accounting.spent()))
}

async fn trace(
    transaction: &mut TestTransaction,
    case: Case,
    result: Result<(), String>,
) -> Vec<String> {
    let mut trace = vec![format!("create: {result:?}")];
    if case.concurrent && case.existing != 0 {
        let mut other = unbounded!(
            transaction.workspace,
            begin_transaction(IdempotencyKey::from_bytes([4; 16]))
        )
        .expect("concurrent transaction");
        let metadata = FileMetadata {
            posix_mode: MetadataField::Value(0o700),
            ..FileMetadata::default()
        };
        unbounded!(other, set_metadata("/d0", metadata)).expect("metadata");
        commit(&mut other).await.expect("concurrent commit");
    }
    // Every outcome is followed by a real mutation and publication. Observations
    // must survive, including observations made before an error.
    transaction
        .write_text("/later", "updated bytes\n")
        .await
        .expect("later write");
    let committed = commit(transaction).await.expect("commit");
    trace.push(match committed {
        TransactionCommit::Committed(generation) => format!("committed: {:?}", generation.id()),
        TransactionCommit::AlreadyCommitted(generation) => {
            format!("already: {:?}", generation.id())
        }
        TransactionCommit::Conflict { actual } => format!("conflict: {:?}", actual.id()),
        _ => panic!("unexpected commit outcome"),
    });
    let workspace = &transaction.workspace;
    for depth in 0..=case.depth {
        let name = path(depth);
        trace.push(format!(
            "stat {name}: {:?}",
            unbounded!(workspace, stat(&name))
        ));
    }
    trace.push(format!(
        "bytes: {:?}",
        unbounded!(workspace, read("/untouched", 64))
    ));
    trace.push(format!(
        "later bytes: {:?}",
        unbounded!(workspace, read("/later", 64))
    ));
    trace
}

fn compare(case: Case) -> [(WorkCounters, AllocationInfo); 2] {
    compare_path(case, &path(case.depth))
}

fn compare_path(case: Case, requested: &str) -> [(WorkCounters, AllocationInfo); 2] {
    compare_variant(case, requested, false)
}

fn compare_variant(
    case: Case,
    requested: &str,
    omit_prefix_observations: bool,
) -> [(WorkCounters, AllocationInfo); 2] {
    compare_profile(
        case,
        requested,
        omit_prefix_observations,
        FilesystemProfile::Posix,
    )
}

fn compare_profile(
    case: Case,
    requested: &str,
    omit_prefix_observations: bool,
    profile: FilesystemProfile,
) -> [(WorkCounters, AllocationInfo); 2] {
    let pair = pair::<_, _, Event>(
        || {
            let start = std::time::Instant::now();
            let transaction = futures::executor::block_on(fixture_with_profile(case, profile));
            (transaction, start.elapsed())
        },
        |(mut transaction, setup), old| {
            let mut result = None;
            let cancellation = CancellationToken::new();
            let start = std::time::Instant::now();
            let allocations = measure(|| {
                result = Some(futures::executor::block_on(async {
                    if old {
                        original(&mut transaction, requested, &cancellation).await
                    } else if omit_prefix_observations {
                        let mut accounting =
                            KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
                        let parsed = transaction
                            .mutation_path(requested, &mut accounting, &cancellation)
                            .unwrap();
                        transaction
                            .checkout
                            .lookup_no_follow_with_accounting(
                                &parsed.value,
                                &mut accounting,
                                &cancellation,
                                true,
                            )
                            .await
                            .unwrap();
                        parsed.retire(&mut accounting).unwrap();
                        Ok(crate::OperationReceipt {
                            value: (),
                            work: accounting.spent(),
                        })
                    } else {
                        transaction
                            .create_dir_all(requested, WorkBudget::UNBOUNDED, &cancellation)
                            .await
                    }
                }));
            });
            let call = start.elapsed();
            let start = std::time::Instant::now();
            let result = result.expect("production call completed");
            record(Event::Observations(
                transaction.checkout.directory_observations(),
            ));
            let work = match &result {
                Ok(receipt) => receipt.work,
                Err(failure) => *failure.work,
            };
            for event in futures::executor::block_on(trace(
                &mut transaction,
                case,
                result
                    .map(|_| ())
                    .map_err(|failure| format!("{:?}", failure.error)),
            )) {
                record(Event::Operation(event));
            }
            (work, allocations, [setup, call, start.elapsed()])
        },
    );
    assert_eq!(pair[0].1, pair[1].1, "complete production trace");
    if std::env::var_os("GC_LOCAL_DIRECTORY_TIMINGS").is_some() {
        for (index, ((_, _, phases), _)) in pair.iter().enumerate() {
            eprintln!(
                "DIRECTORY_PHASE {}",
                serde_json::json!({
                    "depth": case.depth, "existing": case.existing, "old": index == 0,
                    "setup_ns": phases[0].as_nanos() as u64,
                    "call_ns": phases[1].as_nanos() as u64,
                    "audit_ns": phases[2].as_nanos() as u64,
                })
            );
        }
    }
    pair.map(|((work, allocations, _), _)| (work, allocations))
}

#[test]
fn shared_directory_frontier_preserves_outputs_errors_and_observations() {
    for (depth, existing, collision, concurrent, maximum_paths) in [
        (0, 0, 0, false, 64),
        (1, 1, 0, false, 64),
        (4, 4, 0, false, 64),
        (8, 8, 0, false, 64),
        (4, 0, 0, false, 64),
        (4, 2, 0, false, 64),
        (4, 2, 1, false, 64),
        (4, 2, 2, false, 64),
        (4, 4, 0, true, 64),
        (4, 2, 1, true, 64),
        (4, 2, 0, false, 1),
    ] {
        let case = Case {
            depth,
            existing,
            collision,
            concurrent,
            maximum_paths,
        };
        let work = compare(case);
        eprintln!("DIRECTORY_WORK {case:?} {work:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn shared_directory_frontier_matches_original_for_every_bounded_prefix(
        depth in 1usize..7, existing in 0usize..7, collision in 0u8..3,
        concurrent in any::<bool>(), maximum_paths in prop_oneof![Just(1u32), Just(64u32)],
    ) {
        let existing = existing.min(depth);
        compare(Case { depth, existing, collision: if existing < depth { collision } else { 0 }, concurrent, maximum_paths });
    }
}

#[test]
fn invalid_directory_paths_preserve_errors_and_later_mutations() {
    let case = Case {
        depth: 3,
        existing: 2,
        collision: 0,
        concurrent: false,
        maximum_paths: 64,
    };
    for requested in [
        "",
        "relative",
        "/d0//d1",
        "/d0/../d1",
        "/d0/./d1",
        "/d0/",
        "/d0/\0",
    ] {
        compare_path(case, requested);
    }
}

#[test]
fn native_directory_frontier_preserves_the_original_custody() {
    for existing in [0, 2, 4] {
        let case = Case {
            depth: 4,
            existing,
            collision: 0,
            concurrent: false,
            maximum_paths: 64,
        };
        let results = pair::<_, _, Event>(
            || {
                let transaction = futures::executor::block_on(fixture(case));
                let cancellation = CancellationToken::new();
                let original = crate::NativeMaterializationWork::new(
                    selected_transaction_budget(),
                    &cancellation,
                )
                .map_err(|(error, _)| error)
                .expect("native work");
                let transaction =
                    futures::executor::block_on(transaction.workspace.begin_transaction(
                        IdempotencyKey::from_bytes([3; 16]),
                        &original,
                        &cancellation,
                    ))
                    .expect("native checkout");
                (transaction, original, cancellation)
            },
            |(mut transaction, original, cancellation), old| {
                let requested = path(case.depth);
                let result = futures::executor::block_on(async {
                    if old {
                        let mut accounting = KernelAccounting::native(&original, &cancellation);
                        transaction
                            .create_dirs_individually(&requested, &mut accounting, &cancellation)
                            .await
                    } else {
                        transaction
                            .create_dir_all(&requested, &original, &cancellation)
                            .await
                    }
                });
                record(Event::Observations(
                    transaction.checkout.directory_observations(),
                ));
                result.expect("native directory creation");
                futures::executor::block_on(async {
                    let mut accounting = KernelAccounting::native(&original, &cancellation);
                    for depth in 0..=case.depth {
                        let name = path(depth);
                        let parsed = transaction
                            .mutation_path(&name, &mut accounting, &cancellation)
                            .expect("native path");
                        let lookup = transaction
                            .checkout
                            .lookup_no_follow_with_accounting(
                                &parsed.value,
                                &mut accounting,
                                &cancellation,
                                true,
                            )
                            .await
                            .expect("native lookup")
                            .0;
                        record(Event::Operation(format!("record: {:?}", lookup.record)));
                        parsed.retire(&mut accounting).expect("retire path");
                    }
                });
                original
                    .receipt()
                    .verify(selected_transaction_budget())
                    .expect("bounded work");
            },
        );
        assert_eq!(results[0], results[1]);
    }
}

#[test]
fn cancelled_directory_call_dispatches_no_storage_work() {
    let case = Case {
        depth: 4,
        existing: 4,
        collision: 0,
        concurrent: false,
        maximum_paths: 64,
    };
    let mut transaction = futures::executor::block_on(fixture(case));
    let before = transaction.checkout.directory_observations();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = futures::executor::block_on(transaction.create_dir_all(
        &path(case.depth),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .expect_err("cancelled");
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::UNCHARGED);
    assert_eq!(before, transaction.checkout.directory_observations());
}

#[test]
fn missing_prefix_observation_fails_the_same_trace_comparison() {
    let case = Case {
        depth: 4,
        existing: 4,
        collision: 0,
        concurrent: false,
        maximum_paths: 64,
    };
    let failure = std::panic::catch_unwind(|| compare_variant(case, &path(case.depth), true))
        .expect_err("lost ancestor observations must fail the real comparison");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("complete production trace"),
        "wrong failure: {message}"
    );
}

fn costs(measurement: (WorkCounters, AllocationInfo)) -> serde_json::Value {
    let (work, allocation) = measurement;
    serde_json::json!({
        "work": work,
        "allocation": [allocation.count_total as i64, allocation.count_current, allocation.count_max as i64,
            allocation.bytes_total as i64, allocation.bytes_current, allocation.bytes_max as i64],
    })
}

fn expected_costs() -> serde_json::Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/tests/workspace/directories/work.json");
    serde_json::from_slice(&std::fs::read(path).expect("checked-in cost fixture"))
        .expect("cost fixture JSON")
}

fn assert_work(actual: &serde_json::Value, expected: &serde_json::Value) {
    assert_eq!(actual["work"], expected["work"], "exact production work");
}

fn assert_allocations(actual: &serde_json::Value, expected: &serde_json::Value) {
    assert_eq!(
        actual["allocation"], expected["allocation"],
        "exact physical allocations"
    );
}

#[test]
fn directory_work_and_physical_allocations_are_exact() {
    let mut measured = serde_json::Map::new();
    for (name, depth, existing, pages, components) in [
        ("existing4", 4, 4, [24, 9], [10, 4]),
        ("existing8", 8, 8, [80, 17], [36, 8]),
        ("missing4", 4, 0, [66, 42], [10, 10]),
        ("partial4", 4, 2, [60, 38], [10, 7]),
    ] {
        let case = Case {
            depth,
            existing,
            collision: 0,
            concurrent: false,
            maximum_paths: 64,
        };
        let samples = compare(case);
        for index in [0, 1] {
            assert_eq!(
                samples[index].0.page_reads, pages[index],
                "authenticated frontier reads: {name}"
            );
            assert_eq!(
                samples[index].0.source_path_components, components[index],
                "parsed component copies: {name}"
            );
        }
        measured.insert(name.into(), serde_json::json!(samples.map(costs)));
    }
    eprintln!(
        "DIRECTORY_PROFILE {}",
        serde_json::Value::Object(measured.clone())
    );
    let expected = expected_costs();
    for (name, pair) in &measured {
        for index in [0, 1] {
            assert_work(&pair[index], &expected[name][index]);
            assert_allocations(&pair[index], &expected[name][index]);
        }
    }
}

#[test]
fn restored_prefix_walks_fail_the_same_work_and_allocation_assertions() {
    let case = Case {
        depth: 4,
        existing: 4,
        collision: 0,
        concurrent: false,
        maximum_paths: 64,
    };
    let [old, _] = compare(case).map(costs);
    let expected = expected_costs();
    for (check, message) in [
        (assert_work as fn(&_, &_), "exact production work"),
        (
            assert_allocations as fn(&_, &_),
            "exact physical allocations",
        ),
    ] {
        let failure = std::panic::catch_unwind(|| check(&old, &expected["existing4"][1]))
            .expect_err("restoring the original production traversal must be detected");
        let text = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(text.contains(message), "wrong failure: {text}");
    }
}

#[test]
fn directory_frontiers_preserve_each_filesystem_name_profile() {
    for profile in [
        FilesystemProfile::Portable,
        FilesystemProfile::Browser,
        FilesystemProfile::Posix,
        FilesystemProfile::Windows,
    ] {
        for (existing, collision) in [(4, 0), (2, 0), (2, 1), (0, 0)] {
            let case = Case {
                depth: 4,
                existing,
                collision,
                concurrent: false,
                maximum_paths: 64,
            };
            compare_profile(case, &path(case.depth), false, profile);
        }
    }
}
