use super::*;
use proptest::prelude::*;

/// Host observations of a directory and a regular file, to stand for every
/// observed state of those kinds.
struct Kinds {
    _source: tempfile::TempDir,
    directory: HostStat,
    file: HostStat,
    meter: NativeMaterializationWork,
}

fn kinds() -> Kinds {
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("f"), b"body").unwrap();
    let meter =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let root = HostRoot::open(source.path(), &mut meter.clone()).unwrap();
    let limits = crate::model::VolumeLimits::default();
    let stat = |text: &str| {
        let path = NamespacePath::from_portable(
            &crate::path::PortablePath::parse(text, limits).unwrap(),
            limits,
        )
        .unwrap();
        let host = namespace_to_host_path_measured(&path, &meter).unwrap();
        let stat = observe_host_path(&root, &host, &meter).unwrap().unwrap();
        capture_drop_host_path(host, &meter).unwrap();
        capture_drop_path(path, &meter).unwrap();
        stat
    };
    let (directory, file) = (stat("/"), stat("/f"));
    Kinds {
        _source: source,
        directory,
        file,
        meter,
    }
}

fn record() -> FileRecord {
    let object = |kind| crate::ObjectId {
        kind,
        digest: crate::Digest::from_bytes([3; 32]),
    };
    FileRecord {
        file_id: crate::FileId::from_bytes([2; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: object(crate::ObjectKind::Metadata),
        payload: crate::kernel::FilePayload::InlineRegular(
            crate::kernel::InlineFileData::new(b"x").unwrap(),
        ),
    }
}

fn states(kinds: &Kinds, entries: &[(String, u8)]) -> Vec<CapturePathState> {
    let limits = crate::model::VolumeLimits::default();
    entries
        .iter()
        .map(|(text, rank)| {
            let path = NamespacePath::from_portable(
                &crate::path::PortablePath::parse(text, limits).unwrap(),
                limits,
            )
            .unwrap();
            match rank % 4 {
                0 => (None, path, None),
                1 => (Some(kinds.directory.clone()), path, None),
                2 => (Some(kinds.file.clone()), path, Some(record())),
                _ => (Some(kinds.file.clone()), path, None),
            }
        })
        .collect()
}

type Order = Vec<(String, bool, bool, bool)>;

fn order(states: &[CapturePathState]) -> Order {
    states
        .iter()
        .map(|(observation, path, current)| {
            (
                format!("{path:?}"),
                observation.is_some(),
                observation.as_ref().is_some_and(HostStat::is_dir),
                current.is_some(),
            )
        })
        .collect()
}

/// Sorts with `sort`, returning the order and the items it charged.
fn charged(
    kinds: &Kinds,
    mut states: Vec<CapturePathState>,
    sort: fn(&mut [CapturePathState], &NativeMaterializationWork) -> Result<(), CaptureError>,
) -> (Order, u64) {
    let before = kinds.meter.receipt().items_examined;
    sort(&mut states, &kinds.meter).unwrap();
    (
        order(&states),
        kinds.meter.receipt().items_examined - before,
    )
}

fn comparison_sort(
    states: &mut [CapturePathState],
    meter: &NativeMaterializationWork,
) -> Result<(), CaptureError> {
    capture_sort_by(states, capture_state_order, meter)
}

fn paths() -> impl Strategy<Value = Vec<(String, u8)>> {
    proptest::collection::btree_map(
        proptest::collection::vec(prop_oneof![Just("a"), Just("b"), Just("c")], 1..6)
            .prop_map(|parts| format!("/{}", parts.join("/"))),
        0u8..4,
        0..200,
    )
    .prop_map(|map| map.into_iter().collect())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    /// The counting pass over path-ordered states is the comparison order, at
    /// a charge linear in the states and their depth classes.
    #[test]
    fn path_ordered_states_bucket_into_capture_order(entries in paths()) {
        let kinds = kinds();
        let mut ordered = entries.clone();
        ordered.sort_by(|left, right| {
            states(&kinds, std::slice::from_ref(left))[0]
                .1
                .cmp(&states(&kinds, std::slice::from_ref(right))[0].1)
        });
        let input = states(&kinds, &ordered);
        let count = input.len() as u64;
        let (expected, _) = charged(&kinds, states(&kinds, &ordered), comparison_sort);
        let (actual, items) = charged(&kinds, input, sort_capture_states);
        prop_assert_eq!(&actual, &expected);
        let bucketed = if count < 2 { 0 } else { count };
        prop_assert_eq!(items, 2 * count.saturating_sub(1) + bucketed);
        // Negative control: reversed input fails the path check and takes the
        // comparison sort, charging every comparison it makes.
        if count > 1 {
            let mut reversed = ordered.clone();
            reversed.reverse();
            let (fallback, fallback_items) =
                charged(&kinds, states(&kinds, &reversed), sort_capture_states);
            let (_, comparison_items) =
                charged(&kinds, states(&kinds, &reversed), comparison_sort);
            prop_assert_eq!(&fallback, &expected);
            prop_assert_eq!(fallback_items, 2 + comparison_items);
        }
    }
}

/// The path check is what makes the counting pass sound: without it, one
/// class out of path order stays out of order.
#[test]
fn unordered_states_need_the_comparison_sort() {
    let kinds = kinds();
    let entries = [("/b".to_owned(), 3), ("/a".to_owned(), 3)];
    let (expected, _) = charged(&kinds, states(&kinds, &entries), comparison_sort);
    let (checked, _) = charged(&kinds, states(&kinds, &entries), sort_capture_states);
    let (unchecked, _) = charged(&kinds, states(&kinds, &entries), bucket_path_ordered_states);
    assert_eq!(checked, expected);
    assert_ne!(unchecked, expected);
}

/// Deep chains need more depth classes than the stack holds; the measured
/// buffer gives the same order and is released.
#[test]
fn deep_paths_take_a_measured_class_buffer() {
    let kinds = kinds();
    let mut entries = Vec::new();
    for depth in 1..=24 {
        let chain = vec!["d"; depth].join("/");
        for (leaf, rank) in [("a", depth as u8), ("z", depth as u8 + 1)] {
            entries.push((format!("/{chain}/{leaf}"), rank));
        }
        entries.push((format!("/{chain}"), 1));
    }
    entries.sort_by(|left, right| {
        states(&kinds, std::slice::from_ref(left))[0]
            .1
            .cmp(&states(&kinds, std::slice::from_ref(right))[0].1)
    });
    let live = kinds.meter.receipt().peak_allocation_bytes;
    let (expected, _) = charged(&kinds, states(&kinds, &entries), comparison_sort);
    let (actual, items) = charged(&kinds, states(&kinds, &entries), sort_capture_states);
    let count = entries.len() as u64;
    assert_eq!(actual, expected);
    assert_eq!(items, 2 * (count - 1) + count);
    assert!(kinds.meter.receipt().peak_allocation_bytes > live);
}
