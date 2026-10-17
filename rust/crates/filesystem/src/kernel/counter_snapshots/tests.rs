use super::*;
use crate::model::{Lifecycle, VolumeConfig};
use crate::workspace::{IdempotencyKey, TransactionCommit};
use crate::{CancellationToken, Fs, OperationId, OperationReceipt, WorkBudget, WorkspaceError};
use acyclic_test_support::{
    AllocationInfo,
    differential::{pair, record},
    measure as allocations,
};
use proptest::prelude::*;

async fn fixture(
    payload: &[u8],
) -> crate::workspace::Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    fs.create_workspace_with_operation::<_, (), WorkspaceError>(
        "snapshot-copies",
        VolumeConfig::portable(Lifecycle::Ephemeral),
        OperationId::from_bytes([6; 16]),
        WorkBudget::UNBOUNDED,
        &cancellation,
        None,
    )
    .await
    .unwrap();
    let workspace = fs
        .open_workspace("snapshot-copies", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    let mut transaction = workspace
        .begin_transaction(
            IdempotencyKey::from_bytes([7; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap()
        .value;
    transaction
        .write(
            "/file",
            bytes::Bytes::copy_from_slice(payload),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap();
    let result = transaction
        .commit::<WorkspaceError, _, _>(
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| {
                std::future::ready(Ok(OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .unwrap();
    assert!(matches!(result.value, TransactionCommit::Committed(_)));
    workspace
}

fn run(
    payload: &[u8],
    missing: bool,
    cancelled: bool,
    candidate_restore: u8,
) -> [(u64, AllocationInfo); 2] {
    run_mode(payload, missing, cancelled, candidate_restore, false)
}

fn run_mode(
    payload: &[u8],
    missing: bool,
    cancelled: bool,
    candidate_restore: u8,
    native: bool,
) -> [(u64, AllocationInfo); 2] {
    let path = crate::PortablePath::parse(
        if missing { "/absent" } else { "/file" },
        VolumeConfig::portable(Lifecycle::Ephemeral).limits,
    )
    .unwrap();
    let path = crate::kernel::NamespacePath::from_portable(
        &path,
        VolumeConfig::portable(Lifecycle::Ephemeral).limits,
    )
    .unwrap();
    let result = pair::<_, _, String>(
        || {
            let workspace = futures::executor::block_on(fixture(payload));
            let cancellation = CancellationToken::new();
            let original = native.then(|| {
                crate::NativeMaterializationWork::new(
                    crate::work_budget(1_000_000, 64 * 1024 * 1024),
                    &cancellation,
                )
                .map_err(|(error, _)| error)
                .unwrap()
            });
            let key = IdempotencyKey::from_bytes([8; 16]);
            let transaction = futures::executor::block_on(async {
                if let Some(original) = &original {
                    workspace
                        .begin_transaction(key, original, &cancellation)
                        .await
                        .unwrap()
                } else {
                    workspace
                        .begin_transaction(key, WorkBudget::UNBOUNDED, &cancellation)
                        .await
                        .unwrap()
                        .value
                }
            });
            (transaction.checkout, cancellation, original)
        },
        |(mut checkout, cancellation, original), old| {
            if cancelled {
                cancellation.cancel();
            }
            let mut output = None;
            record("dispatch no-follow".to_owned());
            let allocation = allocations(|| {
                output = Some(measure(if old { 3 } else { candidate_restore }, || {
                    futures::executor::block_on(async {
                        if let Some(original) = &original {
                            checkout
                                .lookup_no_follow(&path, original, &cancellation)
                                .await
                                .map(|value| (value, original.receipt()))
                                .map_err(|error| format!("{error:?}"))
                        } else {
                            checkout
                                .lookup_no_follow(&path, WorkBudget::UNBOUNDED, &cancellation)
                                .await
                                .map(|receipt| (receipt.value, receipt.work))
                                .map_err(|failure| format!("{failure:?}"))
                        }
                    })
                }))
            });
            let (output, copied) = output.unwrap();
            record(format!("result: {output:?}"));
            (copied, allocation)
        },
    );
    assert_eq!(
        result[0].1, result[1].1,
        "complete sparse lookup output, errors and trace"
    );
    assert_eq!(result[0].0.1, result[1].0.1, "exact physical allocations");
    result.map(|(sample, _)| sample)
}

fn assert_copies(bytes: u64, expected: u64) {
    assert_eq!(
        bytes, expected,
        "exact bytes returned by accounting snapshots"
    );
}

#[test]
fn sparse_production_lookup_does_not_read_discarded_accounting_snapshots() {
    for missing in [false, true] {
        let [before, after] = run(b"complete bytes", missing, false, 0);
        eprintln!(
            "SNAPSHOT_COPIES missing={missing} before={} after={} allocations={:?}",
            before.0, after.0, after.1
        );
        assert_eq!(
            before.0 - after.0,
            2 * std::mem::size_of::<WorkCounters>() as u64
        );
    }
    let [before, after] = run(b"cancelled", false, true, 0);
    assert_eq!(before, after);
}

#[test]
fn restored_discarded_copy_fails_the_same_copy_assertion() {
    let [_, candidate] = run(b"complete bytes", false, false, 0);
    for restored in [1, 2, 3] {
        let [_, broken] = run(b"complete bytes", false, false, restored);
        let failure = std::panic::catch_unwind(|| assert_copies(broken.0, candidate.0))
            .expect_err("restoring the real snapshot must fail");
        let text = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            text.contains("exact bytes returned by accounting snapshots"),
            "wrong failure: {text}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn discarded_snapshot_reads_never_change_sparse_lookup_behavior(
        payload in prop::collection::vec(any::<u8>(),0..32), missing in any::<bool>(), cancelled in any::<bool>(),
    ) {
        let [before,after] = run(&payload,missing,cancelled,0);
        prop_assert_eq!(before.0 - after.0, if cancelled {0} else {2 * std::mem::size_of::<WorkCounters>() as u64});
    }
}

#[test]
fn native_sparse_lookup_preserves_work_and_discards_the_same_two_snapshots() {
    for missing in [false, true] {
        let [before, after] = run_mode(b"complete bytes", missing, false, 0, true);
        eprintln!(
            "NATIVE_SNAPSHOT_COPIES missing={missing} before={} after={} allocations={:?}",
            before.0, after.0, after.1
        );
        assert_eq!(
            before.0 - after.0,
            2 * std::mem::size_of::<WorkCounters>() as u64
        );
    }
    let [before, after] = run_mode(b"cancelled", false, true, 0, true);
    assert_eq!(before, after);
}
