use super::*;
use crate::WorkCounters;

type MemoryWorkspace = Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;

fn nested(depth: usize) -> String {
    (0..depth).map(|index| format!("/d{index}")).collect()
}

async fn prepared(depth: usize) -> Result<(MemoryWorkspace, String), Box<dyn Error>> {
    let workspace = unbounded!(Fs::memory(), create_workspace("existing-directories"))?;
    let path = nested(depth);
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_dir_all(&path))?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    Ok((workspace, path))
}

async fn measured(
    workspace: &MemoryWorkspace,
    path: &str,
    repeated_prefixes: bool,
) -> Result<WorkCounters, Box<dyn Error>> {
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let root = transaction.checkout.root().file_table;
    let cancellation = CancellationToken::new();
    let mut work = WorkCounters::UNCHARGED;
    let ends: Vec<_> = if repeated_prefixes {
        // The former algorithm: look up each complete ancestor again.
        path.match_indices('/')
            .skip(1)
            .map(|(end, _)| end)
            .chain([path.len()])
            .collect()
    } else {
        vec![path.len()]
    };
    for end in ends {
        let receipt = transaction
            .create_dir_all(&path[..end], WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        work = work.checked_add(receipt.work)?;
    }
    assert_eq!(transaction.checkout.root().file_table, root);
    assert!(!transaction.checkout.has_pending_mutations());
    Ok(work)
}

fn check_linear_reads(work: WorkCounters, depth: usize) {
    assert!(
        work.page_reads <= 8 * depth as u64 + 16
            && work.backend_write_operations == 0
            && work.authority_records_appended == 0,
        "directory check exceeded linear read-only work: depth {depth}: {work:?}"
    );
}

fn check_shared_writes(candidate: WorkCounters, separate: WorkCounters) {
    assert!(
        candidate.page_writes < separate.page_writes,
        "shared generation was rewritten per prefix: {candidate:?}; {separate:?}"
    );
}

async fn missing(
    depth: usize,
    separate: bool,
    maximum_mutations: u32,
) -> Result<WorkCounters, Box<dyn Error>> {
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.limits.maximum_mutations_per_batch = maximum_mutations;
    let workspace = unbounded!(
        Fs::memory(),
        create_workspace_with_config("missing", config)
    )?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let path = nested(depth);
    let mut work = WorkCounters::UNCHARGED;
    for length in if separate { 1..=depth } else { depth..=depth } {
        let receipt = transaction
            .create_dir_all(
                &nested(length),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        work = work.checked_add(receipt.work)?;
    }
    commit(&mut transaction).await?;
    for end in path
        .match_indices('/')
        .skip(1)
        .map(|(end, _)| end)
        .chain([path.len()])
    {
        assert_eq!(
            unbounded!(workspace, stat(&path[..end]))?.kind,
            FileKind::Directory
        );
    }
    Ok(work)
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn existing_directory_reads_grow_linearly_without_mutations(depth in 2usize..17) {
        let result: Result<(), Box<dyn Error>> = futures::executor::block_on(async {
            let (workspace, path) = prepared(depth).await?;
            let work = measured(&workspace, &path, false).await?;
            check_linear_reads(work, depth);
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn missing_descendants_share_generation_work(depth in 2usize..17) {
        let result: Result<(), Box<dyn Error>> = futures::executor::block_on(async {
            let together = missing(depth, false, 2048).await?;
            let separate = missing(depth, true, 2048).await?;
            check_shared_writes(together, separate);
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[tokio::test]
async fn one_mutation_volumes_still_create_every_missing_descendant() -> Result<(), Box<dyn Error>>
{
    missing(8, false, 1).await?;
    Ok(())
}

#[tokio::test]
async fn missing_suffix_preserves_ancestors_and_honors_path_limits() -> Result<(), Box<dyn Error>> {
    for maximum in [1, 2, 2048] {
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.limits.maximum_paths_per_batch = maximum;
        let workspace = unbounded!(Fs::memory(), create_workspace_with_config("suffix", config))?;
        let mut initial = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(initial, create_dir_all(&nested(3)))?;
        commit(&mut initial).await?;
        let ancestor = unbounded!(workspace, stat(&nested(3)))?.file_id;
        let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(transaction, create_dir_all(&nested(8)))?;
        commit(&mut transaction).await?;
        assert_eq!(unbounded!(workspace, stat(&nested(3)))?.file_id, ancestor);
        assert_eq!(
            unbounded!(workspace, stat(&nested(8)))?.kind,
            FileKind::Directory
        );
    }
    Ok(())
}

#[tokio::test]
async fn per_prefix_rewrite_control_is_rejected_by_the_same_work_check()
-> Result<(), Box<dyn Error>> {
    let together = missing(16, false, 2048).await?;
    let separate = missing(16, true, 2048).await?;
    eprintln!("missing depth 16: shared {together:?}; per-prefix {separate:?}");
    check_shared_writes(together, separate);
    assert!(
        std::panic::catch_unwind(|| check_shared_writes(separate, separate)).is_err(),
        "per-prefix control escaped the same work assertion"
    );
    Ok(())
}

#[tokio::test]
async fn repeated_prefix_control_exceeds_the_same_read_bound() -> Result<(), Box<dyn Error>> {
    let depth = 16;
    let (workspace, path) = prepared(depth).await?;
    let direct = measured(&workspace, &path, false).await?;
    let former = measured(&workspace, &path, true).await?;
    eprintln!("existing directory depth {depth}: direct {direct:?}; repeated prefixes {former:?}");
    check_linear_reads(direct, depth);
    assert!(
        std::panic::catch_unwind(|| check_linear_reads(former, depth)).is_err(),
        "quadratic control passed the same linear read assertion"
    );
    Ok(())
}

#[tokio::test]
async fn existing_directory_checks_every_ancestor_and_retires_failed_paths()
-> Result<(), Box<dyn Error>> {
    let (workspace, _) = prepared(2).await?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    transaction.write_text("/file", "unchanged").await?;
    unbounded!(
        transaction,
        create_symbolic_link("/link", OwnedBytes::from_static(b"/d0"))
    )?;
    for path in ["/file", "/file/child", "/link", "/link/d1"] {
        assert!(
            matches!(
                unbounded!(transaction, create_dir_all(path)),
                Err(WorkspaceError::NotDirectory)
            ),
            "{path}"
        );
    }
    unbounded!(transaction, create_dir_all("/d0/d1/new/deeper"))?;
    transaction
        .write_text("/d0/d1/new/deeper/value", "created")
        .await?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file", 32))?.as_ref(),
        b"unchanged"
    );
    assert_eq!(
        unbounded!(workspace, read("/d0/d1/new/deeper/value", 32))?.as_ref(),
        b"created"
    );
    Ok(())
}

#[tokio::test]
async fn existing_directory_observations_survive_a_competing_ancestor_replacement()
-> Result<(), Box<dyn Error>> {
    let (workspace, path) = prepared(3).await?;
    let mut reader = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(reader, create_dir_all(&path))?;
    reader.write_text("/stamp", "observed").await?;
    let mut writer = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(writer, rename("/d0", "/old", false))?;
    unbounded!(writer, create_dir_all(&path))?;
    commit(&mut writer).await?;
    assert!(matches!(
        commit(&mut reader).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(reader, rebase(16))?,
        TransactionRebase::Conflicted { .. }
    ));
    assert!(matches!(
        unbounded!(workspace, read("/stamp", 32)),
        Err(WorkspaceError::NotFound)
    ));
    Ok(())
}
