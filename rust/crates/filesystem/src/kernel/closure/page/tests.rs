use super::*;
use crate::async_storage::{AsyncObjectStore, DecodedCacheKey, DecodedCacheValue};
use crate::kernel::file_table_mutation::FileTableFormat;
use crate::kernel::persistent_btree::{Format, Page};
use crate::kernel::{FileKind, encode_file_table_page};
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

struct Store(DecodedCacheKey, Option<DecodedCacheValue>);

impl AsyncObjectStore for Store {
    fn decoded_cache_get(
        &self,
        key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, ObjectStoreError> {
        acyclic_test_support::differential::record(key);
        Ok(self.1.as_ref().filter(|_| key == self.0).cloned())
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
        (!cold).then(|| DecodedCacheValue {
            value: Arc::new(Page::<FileTableFormat>::Leaf(records(count))),
            logical_bytes: owned,
            native_custody: Some(Arc::new(
                original.claim(owned, 3, WorkCounters::UNCHARGED).unwrap(),
            )),
        }),
    );
    let before = meter.work();
    let mut result = None;
    let (heap, trace): (_, Vec<DecodedCacheKey>) = acyclic_test_support::differential::observe(
        || (),
        |_| {
            // Reserve the trace's first slot outside allocation measurement.
            acyclic_test_support::differential::record(key);
            acyclic_test_support::measure(|| {
                result = Some(decode::<_, Page<FileTableFormat>>(
                    &store,
                    id,
                    limits,
                    &meter,
                    |observe| FileTableFormat::decode(&bytes, limits, &mut |step| observe(step)),
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
            assert!(check(count, true, native).is_err());
        }
    }
}
