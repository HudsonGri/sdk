use super::*;
use crate::facade::FsWork as _;

fn conflict(number: u128) -> MergeConflict {
    MergeConflict::File(FileId::from_bytes(number.to_be_bytes()))
}

#[test]
fn map_and_slice_resolution_adapters_share_bounded_logarithmic_probes() {
    let cancellation = CancellationToken::new();
    let resolutions: BTreeMap<_, _> = (0..100_000)
        .map(|number| {
            (
                conflict(number),
                MergeConflictResolution::Select(MergeConflictSide::Ours),
            )
        })
        .collect();
    let budget = crate::performance::work_budget(100_064, 2 * 1024 * 1024);
    let mut accounting = budget.accounting(&cancellation);
    let indexed = index_resolutions(resolutions.iter(), &mut accounting).unwrap();
    let built = accounting.spent().items_examined;
    assert_eq!(built, 100_000);
    let view = ResolutionView::Entries(&indexed.value);
    assert!(
        view.file(
            FileId::from_bytes(99_999u128.to_be_bytes()),
            &mut accounting
        )
        .unwrap()
        .is_some()
    );
    assert!(
        view.file(
            FileId::from_bytes(100_000u128.to_be_bytes()),
            &mut accounting
        )
        .unwrap()
        .is_none()
    );
    assert!(accounting.spent().items_examined - built <= 34);
    indexed.retire(&mut accounting).unwrap();

    // The actual former map lookup restarts at the first key for every region.
    // Its final-key probe alone exhausts the same remaining allowance.
    let mut old = budget.accounting(&cancellation);
    let indexed = index_resolutions(resolutions.iter(), &mut old).unwrap();
    let denied = resolutions.keys().try_for_each(|key| {
        old.charge_items(1)?;
        assert!(key <= &conflict(99_999));
        Ok::<_, FsError>(())
    });
    assert!(denied.is_err());
    indexed.retire(&mut old).unwrap();

    let slice: Vec<_> = resolutions.into_iter().take(64).collect();
    let mut accounting = budget.accounting(&cancellation);
    let indexed = index_resolutions(
        slice.iter().map(|(key, value)| (key, value)),
        &mut accounting,
    )
    .unwrap();
    assert!(
        ResolutionView::Entries(&indexed.value)
            .file(FileId::from_bytes(63u128.to_be_bytes()), &mut accounting)
            .unwrap()
            .is_some()
    );
    indexed.retire(&mut accounting).unwrap();
    let mut duplicate = slice;
    duplicate[1].0 = duplicate[0].0.clone();
    assert!(matches!(
        index_resolutions(
            duplicate.iter().map(|(key, value)| (key, value)),
            &mut accounting
        ),
        Err(FsError::InvalidDiff)
    ));
}
