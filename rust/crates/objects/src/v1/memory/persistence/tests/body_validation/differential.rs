use super::*;
use acyclic_test_support::{AllocationInfo, differential, measure};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn pending_validation_matches_the_independent_scan_without_allocating(
        limit in 1u64..17,
        prefix in prop::collection::vec(0usize..17, 1..33),
        pending in prop::collection::vec(any::<u8>(), 0..25),
    ) {
        // Each side opens and seeds its own store; no state, decoder, or journal
        // from the candidate is reused by the baseline.
        let [(before, old_trace), (after, new_trace)] = differential::pair::<_, _, (&str, u64, u64)>(
            || {
                let root = tempfile::tempdir().unwrap();
                let core = core(root.path(), limit);
                let requests = prefix.iter().enumerate().map(|(index, &size)| (
                    put(&format!("prefix-{index}")), OwnedBytes::from(vec![7; size.min(limit as usize)]),
                )).collect();
                assert!(batch(&core, requests, false).iter().all(Result::is_ok));
                (root, core)
            },
            |(_root, core), old| {
                let body = StoredBody::memory(OwnedBytes::from(pending.clone()));
                let journal = core.journal.as_ref().unwrap();
                let state = core.lock_state().unwrap();
                let mut result = Ok(());
                let (allocation, work) = count_body_lengths(|| measure(|| {
                    result = if old {
                        previous_validation(journal, &state, Some(&body))
                    } else {
                        journal.validate_body(&body)
                    };
                }));
                let bytes = result.map(|()| match body {
                    StoredBody::Memory(bytes) => bytes.as_slice().to_vec(),
                    _ => unreachable!("pending memory body"),
                });
                (bytes, allocation, work)
            },
        );
        prop_assert_eq!(&before.0, &after.0);
        prop_assert_eq!(old_trace, new_trace);
        // imbl's pinned 16-entry leaves need one cursor frame at this bound.
        // The removed scan allocates that frame; the candidate has no cursor.
        let frames = u64::from(prefix.len() > 16);
        let bytes = frames * std::mem::size_of::<(usize, usize)>() as u64;
        prop_assert_eq!(before.1, AllocationInfo {
            count_total: frames, count_max: frames,
            bytes_total: bytes, bytes_max: bytes,
            ..AllocationInfo::default()
        });
        prop_assert_eq!(after.1, AllocationInfo::default());
        prop_assert_eq!(after.2, 1);
        prop_assert_eq!(before.2, prefix.len() + 1);
        prop_assert_ne!(before.2, after.2, "restoring the full scan fails the same one-read bound");
    }
}
