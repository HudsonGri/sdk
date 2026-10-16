use acyclic_fs::kernel::FileKind;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Fs, IdempotencyKey, Transaction,
    TransactionCommit, WorkBudget, WorkspaceError,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

fn path(depth: u8) -> String {
    if depth == 0 {
        "/".into()
    } else {
        (0..depth).map(|i| format!("/node-{i}")).collect()
    }
}

async fn commit<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut Transaction<A, O>,
) -> bool {
    let result = transaction
        .commit::<WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await;
    let result = match result {
        Ok(receipt) => receipt.value,
        Err(failure) => {
            assert!(matches!(
                failure.error,
                WorkspaceError::Filesystem(acyclic_fs::FsError::NoPendingMutations)
            ));
            return false;
        }
    };
    assert!(matches!(result, TransactionCommit::Committed(_)));
    true
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    AcceptObstacle,
    RejectDirectory,
    SkipCreation,
    LoseExisting,
}

async fn check(depth: u8, present: u8, obstacle: u8, fault: Fault) -> TestCaseResult {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let workspace = fs
        .create_workspace("directory-property", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    let mut fixture = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    for prefix in 1..=present {
        fixture
            .create_directory(&path(prefix), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap();
    }
    if obstacle == 1 {
        fixture
            .write(
                &path(present + 1),
                &b"file"[..],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .unwrap();
    } else if obstacle == 2 {
        fixture
            .create_symbolic_link(
                &path(present + 1),
                &b"/"[..],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .unwrap();
    }
    assert_eq!(commit(&mut fixture).await, present > 0 || obstacle != 0);
    let existing = present + u8::from(obstacle != 0);
    let mut identities = Vec::new();
    for prefix in 1..=existing {
        identities.push(
            workspace
                .stat(&path(prefix), WorkBudget::UNBOUNDED, &cancellation)
                .await
                .unwrap()
                .value
                .file_id,
        );
    }
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    let result = transaction
        .create_dir_all(&path(depth), WorkBudget::UNBOUNDED, &cancellation)
        .await;
    let expected_ok = obstacle == 0 || depth <= present;
    let mut accepted = result.is_ok();
    let correct_error = result.is_ok()
        || matches!(&result, Err(error) if matches!(&error.error, WorkspaceError::NotDirectory));
    let changed = commit(&mut transaction).await;
    let mut kinds = Vec::new();
    let mut preserved = true;
    for prefix in 1..=existing.max(depth) {
        let stat = workspace
            .stat(&path(prefix), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .ok()
            .map(|value| value.value);
        if prefix <= existing {
            preserved &= stat
                .as_ref()
                .is_some_and(|stat| stat.file_id == identities[usize::from(prefix - 1)]);
        }
        kinds.push(stat.map(|stat| stat.kind));
    }
    match fault {
        Fault::AcceptObstacle => accepted = true,
        Fault::RejectDirectory => accepted = false,
        Fault::SkipCreation => *kinds.last_mut().unwrap() = None,
        Fault::LoseExisting => preserved = false,
        Fault::None => {}
    }
    let expected = (1..=existing.max(depth))
        .map(|prefix| {
            if prefix <= present || (expected_ok && prefix <= depth) {
                Some(FileKind::Directory)
            } else if prefix == present + 1 {
                match obstacle {
                    1 => Some(FileKind::Regular),
                    2 => Some(FileKind::SymbolicLink),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    prop_assert_eq!(
        (accepted, correct_error, kinds, preserved, changed),
        (
            expected_ok,
            true,
            expected,
            true,
            expected_ok && depth > present
        ),
        "recursive directory creation differs from input tree"
    );
    Ok(())
}

#[test]
fn recursive_directory_creation_preserves_existing_nodes_and_rejects_links() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest!(ProptestConfig::with_cases(64), |(depth in 0..6_u8, present in 0..6_u8, obstacle in 0..3_u8)| {
        runtime.block_on(check(depth, present, obstacle, Fault::None))?;
    });
}

#[tokio::test]
async fn directory_creation_negative_controls() {
    for (depth, present, obstacle, fault) in [
        (4, 2, 1, Fault::AcceptObstacle),
        (4, 2, 2, Fault::AcceptObstacle),
        (4, 4, 0, Fault::RejectDirectory),
        (4, 2, 0, Fault::SkipCreation),
        (4, 4, 0, Fault::LoseExisting),
    ] {
        check(depth, present, obstacle, Fault::None).await.unwrap();
        expect_fault(check(depth, present, obstacle, fault).await);
    }
}

fn expect_fault(result: TestCaseResult) {
    let failure = result.unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("recursive directory creation differs from input tree"),
        "{failure}"
    );
}

async fn check_replacement(depth: u8, replaced: u8, observe: bool) -> TestCaseResult {
    let cancellation = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "directory-replacement",
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap()
        .value;
    let mut fixture = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    for prefix in 1..=depth {
        fixture
            .create_directory(&path(prefix), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap();
    }
    assert!(commit(&mut fixture).await);
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    if observe {
        transaction
            .create_dir_all(&path(depth), WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap();
    }
    transaction
        .write("/own", &b"own"[..], WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap();
    let mut peer = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    if replaced > 0 {
        for prefix in (replaced..=depth).rev() {
            peer.remove(&path(prefix), WorkBudget::UNBOUNDED, &cancellation)
                .await
                .unwrap();
        }
        for prefix in replaced..=depth {
            peer.create_directory(&path(prefix), WorkBudget::UNBOUNDED, &cancellation)
                .await
                .unwrap();
        }
    } else {
        peer.write("/peer", &b"peer"[..], WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap();
    }
    assert!(commit(&mut peer).await);
    let result = transaction
        .rebase(16, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    let conflicted = matches!(result, acyclic_fs::TransactionRebase::Conflicted { .. });
    prop_assert_eq!(
        conflicted,
        replaced > 0,
        "directory existence observation lost before rebase"
    );
    Ok(())
}

#[tokio::test]
async fn directory_existence_observes_each_replaced_ancestor() {
    for depth in 1..=3 {
        for replaced in 0..=depth {
            check_replacement(depth, replaced, true).await.unwrap();
        }
    }
}

#[tokio::test]
async fn missing_directory_observation_negative_control() {
    let failure = check_replacement(3, 1, false).await.unwrap_err();
    assert!(
        failure
            .to_string()
            .contains("directory existence observation lost before rebase"),
        "{failure}"
    );
}
