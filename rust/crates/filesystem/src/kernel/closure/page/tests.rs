use super::*;
use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue,
};
use crate::kernel::file_table_mutation::FileTableFormat;
use crate::kernel::persistent_btree::Page;
use crate::kernel::{FileKind, encode_file_table_page};
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

struct Store(
    DecodedCacheKey,
    std::sync::OnceLock<DecodedCacheValue>,
    bool,
    bool,
);

impl AsyncObjectStore for Store {
    fn supports_decoded_cache(&self) -> bool {
        !self.3
    }

    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        acyclic_test_support::differential::record(key);
        Ok(self.1.get().filter(|_| !self.2 && key == self.0).cloned())
    }

    fn decoded_cache_admit(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, ObjectStoreError> {
        assert_eq!(key, self.0);
        if self.3 {
            return Ok(DecodedCacheAdmission::Uncached(value));
        }
        Ok(DecodedCacheAdmission::Shared(
            self.1.get_or_init(|| value).clone(),
        ))
    }

    async fn read(
        &self,
        _: ObjectId,
        _: u64,
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::storage::ObjectRead, ObjectStoreError> {
        unreachable!("the caller has already authenticated the object bytes")
    }

    async fn contains(
        &self,
        _: ObjectId,
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        unreachable!("decoding does not probe objects")
    }

    async fn read_many(
        &self,
        _: &[crate::ObjectReadRequest],
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        unreachable!("the caller has already authenticated the object bytes")
    }

    async fn put(
        &self,
        _: ObjectId,
        _: acyclic_stream::OwnedBytes,
        _: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        unreachable!("decoding does not write objects")
    }
}

fn records(count: usize) -> Vec<FileRecord> {
    (0..count)
        .map(|index| FileRecord {
            file_id: FileId::from_bytes((index as u128).to_be_bytes()),
            kind: FileKind::Fifo,
            link_count: 1,
            metadata: ObjectId {
                kind: ObjectKind::Metadata,
                digest: crate::Digest::ZERO,
            },
            payload: FilePayload::Empty,
        })
        .collect()
}

fn check(count: usize, cold: bool, native: bool) -> TestCaseResult {
    let limits = DecodeLimits::default();
    let expected = records(count);
    let bytes = encode_file_table_page(
        &crate::kernel::FileTablePage::Leaf(expected.clone()),
        1024,
        |_| Ok::<_, crate::FsError>(()),
        |_| crate::FsError::HandleAllocationFailed,
    )
    .unwrap();
    let id = ObjectId {
        kind: ObjectKind::FileTablePage,
        digest: crate::storage::object_digest(ObjectKind::FileTablePage, &bytes),
    };
    let key = DecodedCacheKey::new::<Page<FileTableFormat>>(id, limits);
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let initial_live = original.live_allocation_bytes();
    let meter = if native {
        ProofMeter::Native {
            original: &original,
            cancellation: &cancellation,
        }
    } else {
        ProofMeter::standalone(WorkBudget::UNBOUNDED, &cancellation).unwrap()
    };
    let owned = (count * size_of::<FileRecord>()
        + size_of::<Page<FileTableFormat>>()
        + size_of::<crate::NativeAllocation>()
        + 4 * size_of::<usize>()) as u64;
    let store = Store(
        key,
        (!cold)
            .then(|| DecodedCacheValue {
                value: Arc::new(Page::<FileTableFormat>::Leaf(records(count))),
                logical_bytes: (count * size_of::<FileRecord>()) as u64,
                native_custody: Some(Arc::new(
                    original.claim(owned, 3, WorkCounters::UNCHARGED).unwrap(),
                )),
            })
            .map(std::sync::OnceLock::from)
            .unwrap_or_default(),
        cold,
        false,
    );
    let before = meter.work();
    let mut result = None;
    let (heap, trace): (_, Vec<DecodedCacheKey>) = acyclic_test_support::differential::observe(
        || (),
        |_| {
            // Reserve the trace's first slot outside allocation measurement.
            acyclic_test_support::differential::record(key);
            acyclic_test_support::measure(|| {
                result = Some(persistent::<_, FileTableFormat>(
                    &store, id, limits, &meter, &bytes,
                ))
            })
        },
    );
    let result = result.unwrap().unwrap();
    drop(store);
    let Page::Leaf(actual) = result.page() else {
        panic!("expected leaf")
    };
    prop_assert_eq!(actual, &expected);
    // The baseline is independently decoded from canonical bytes with the old
    // decoder, not from the candidate's cached representation.
    let baseline =
        crate::kernel::decode_file_table_page(&bytes, limits, &mut |_| Ok::<_, crate::FsError>(()))
            .unwrap();
    let crate::kernel::FileTablePage::Leaf(baseline) = baseline else {
        panic!("expected leaf")
    };
    prop_assert_eq!(actual, &baseline);
    prop_assert_eq!(trace, vec![key, key]);
    prop_assert_eq!(meter.work(), before, "borrowing does no decoder work");
    prop_assert_eq!(heap.count_total, 0);
    prop_assert_eq!(heap.bytes_total, 0);
    prop_assert_eq!(original.live_allocation_bytes(), initial_live + owned);
    drop(result);
    prop_assert_eq!(original.live_allocation_bytes(), initial_live);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn authenticated_pages_are_borrowed_at_n_and_2n(n in 1usize..65) {
        for native in [false, true] {
            check(n, false, native)?;
            check(2 * n, false, native)?;
        }
    }
}

#[test]
fn restoring_the_canonical_decode_fails_the_same_work_assertion() {
    for count in [32, 64] {
        for native in [false, true] {
            let error = check(count, true, native).unwrap_err();
            assert!(
                error.to_string().contains("borrowing does no decoder work"),
                "{error}"
            );
        }
    }
}

fn fill_then_borrow(
    count: usize,
    native: bool,
    bounded: bool,
    no_admission: bool,
) -> TestCaseResult {
    let limits = DecodeLimits::default();
    let expected = records(count);
    let bytes = encode_file_table_page(
        &crate::kernel::FileTablePage::Leaf(expected.clone()),
        1024,
        |_| Ok::<_, crate::FsError>(()),
        |_| crate::FsError::HandleAllocationFailed,
    )
    .unwrap();
    let id = ObjectId {
        kind: ObjectKind::FileTablePage,
        digest: crate::storage::object_digest(ObjectKind::FileTablePage, &bytes),
    };
    let key = DecodedCacheKey::new::<Page<FileTableFormat>>(id, limits);
    let cancellation = CancellationToken::new();
    let probe =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let budget = if bounded {
        WorkBudget {
            allocation_operations: if native {
                probe.receipt().allocation_operations + 1
            } else {
                2
            },
            ..WorkBudget::UNBOUNDED
        }
    } else {
        WorkBudget::UNBOUNDED
    };
    drop(probe);
    let original = crate::NativeMaterializationWork::new(
        if native {
            budget
        } else {
            WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .unwrap();
    let initial_live = original.live_allocation_bytes();
    let meter = if native {
        ProofMeter::Native {
            original: &original,
            cancellation: &cancellation,
        }
    } else {
        ProofMeter::standalone(budget, &cancellation).unwrap()
    };
    let store = Store(key, Default::default(), false, no_admission);
    let controls = meter
        .with_accounting(|accounting| {
            crate::kernel::file_read::decoded_cache_controls::<Page<FileTableFormat>>(accounting)
        })
        .unwrap()
        .0;
    let mut first = None;
    let cold = acyclic_test_support::measure(|| {
        first = Some(persistent::<_, FileTableFormat>(
            &store, id, limits, &meter, &bytes,
        ))
    });
    let first = first.unwrap().unwrap();
    let Page::Leaf(actual) = first.page() else {
        panic!("expected leaf")
    };
    let baseline =
        crate::kernel::decode_file_table_page(&bytes, limits, &mut |_| Ok::<_, crate::FsError>(()))
            .unwrap();
    let crate::kernel::FileTablePage::Leaf(baseline) = baseline else {
        panic!("expected leaf")
    };
    prop_assert_eq!(actual, &baseline);
    if bounded {
        prop_assert!(matches!(&first.value, PageValue::Owned(_)));
        prop_assert!(store.1.get().is_none());
        prop_assert_eq!(cold.count_total, 1);
        prop_assert_eq!(cold.bytes_total, count * size_of::<FileRecord>());
        drop(first);
        prop_assert_eq!(original.live_allocation_bytes(), initial_live);
        return Ok(());
    }
    drop(first);
    let before = meter.work();
    let mut second = None;
    let warm = acyclic_test_support::measure(|| {
        second = Some(persistent::<_, FileTableFormat>(
            &store, id, limits, &meter, &bytes,
        ))
    });
    let second = second.unwrap().unwrap();
    prop_assert_eq!(meter.work(), before, "warm page repeated decoder work");
    prop_assert_eq!(warm.count_total, 0);
    prop_assert_eq!(warm.bytes_total, 0);
    prop_assert_eq!(cold.count_total, if native { 3 } else { 2 });
    prop_assert_eq!(cold.bytes_total, count * size_of::<FileRecord>() + controls);
    prop_assert!(store.1.get().is_some());
    drop(store);
    let Page::Leaf(actual) = second.page() else {
        panic!("expected leaf")
    };
    prop_assert_eq!(actual, &baseline);
    drop(second);
    prop_assert_eq!(original.live_allocation_bytes(), initial_live);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn cold_proofs_populate_the_same_cache_and_preserve_tight_bounds(n in 1usize..65) {
        for native in [false, true] {
            for count in [n, 2 * n] {
                fill_then_borrow(count, native, false, false)?;
                fill_then_borrow(count, native, true, false)?;
            }
        }
    }
}

#[test]
fn removing_admission_restores_repeated_decoding() {
    for native in [false, true] {
        for count in [32, 64] {
            let error = fill_then_borrow(count, native, false, true).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("warm page repeated decoder work"),
                "{error}"
            );
        }
    }
}
