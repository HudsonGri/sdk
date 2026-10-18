use super::*;
use crate::foundation::{Digest, FileId};
use crate::kernel::attribute_mutation::{AttributeFormat, AttributeMutation};
use crate::kernel::file_table_mutation::{FileTableFormat, FileTableMutation};
use crate::kernel::persistent_btree::{self, Page};
use crate::kernel::persistent_point::retire_allocation;
use crate::kernel::tree_mutation::{TreeFormat, TreeMutation};
use crate::kernel::{
    AttributeClass, AttributeEntry, AttributeName, FileKind, FilePayload, FileRecord,
    InlineFileData, LogicalName, NameEncoding, TreeEntry,
};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectKind;
use crate::{CachedObjectStore, ObjectCacheOptions, WorkBudget};
use acyclic_test_support::differential;
use proptest::prelude::*;

thread_local! {
    static OMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(super) fn omit_written_cache() -> bool {
    OMIT.get()
}

fn cached() -> CachedObjectStore<MemoryObjectStore> {
    CachedObjectStore::new(
        MemoryObjectStore::default(),
        ObjectCacheOptions {
            maximum_entries: 4096,
            maximum_bytes: 16 * 1024 * 1024,
            maximum_in_flight: 2,
            maximum_waiters_per_object: 2,
        },
    )
    .unwrap()
}

fn encoded<F: Format>(page: PageRef<'_, F>) -> Vec<u8> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let (value, allocation) = F::encode_with_accounting(&page, 128, &mut accounting).unwrap();
    let bytes = usize_to_u64(value.capacity());
    let output = value.clone();
    drop(value);
    retire_allocation(allocation, bytes, &mut accounting).unwrap();
    output
}

async fn seed<S: AsyncObjectStore, F: Format>(store: &S, values: &[F::Value]) -> ObjectId {
    let object = crate::storage::HashedObject::new(
        F::kind(),
        encoded::<F>(PageRef::Leaf(values.into())).into(),
    );
    let root = object.object_id();
    let cancellation = CancellationToken::new();
    let mut authority = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    store.put_hashed(object, &mut authority).await.unwrap();
    root
}

async fn canonical_tree<S: AsyncObjectStore, F: Format>(
    store: &S,
    root: ObjectId,
) -> Vec<(ObjectId, Vec<u8>)> {
    let cancellation = CancellationToken::new();
    let mut authority = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let mut pending = vec![root];
    let mut output = Vec::new();
    while let Some(object) = pending.pop() {
        let read = store
            .read(object, 1024 * 1024, &mut authority)
            .await
            .unwrap();
        let bytes = read.bytes.to_vec();
        if let Page::Internal(children) = F::decode(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, CanonicalDecodeError>(())
        })
        .unwrap()
        {
            pending.extend(children.into_iter().rev().map(|child| child.page));
        }
        output.push((object, bytes));
    }
    output
}

type Trace = Vec<(Result<ObjectId, String>, Vec<(ObjectId, Vec<u8>)>)>;

async fn history<S, F, M>(
    store: S,
    initial: Vec<F::Value>,
    operations: Vec<M>,
    native: bool,
) -> Trace
where
    S: AsyncObjectStore,
    F: Format,
    M: persistent_btree::Mutation<F>,
{
    let mut root = seed::<_, F>(&store, &initial).await;
    let mut output = Vec::new();
    let mut originals = Vec::new();
    for (index, mutation) in operations.into_iter().enumerate() {
        let cancellation = CancellationToken::new();
        let original =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let baseline = original.live_allocation_bytes();
        let mut accounting = if native {
            KernelAccounting::native(&original, &cancellation)
        } else {
            KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation)
        };
        let (retained, custody) = read_page_with_accounting::<_, F>(
            &store,
            root,
            DecodeLimits::default(),
            &mut accounting,
            &cancellation,
            None,
        )
        .await
        .unwrap();
        let before = encoded::<F>(retained.page.view());
        let result = persistent_btree::apply_with_accounting::<_, F, M>(
            &store,
            root,
            std::slice::from_ref(&mutation),
            1,
            DecodeLimits::default(),
            &mut accounting,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|error| error.to_string());
        if let Ok(next) = &result {
            root = *next;
        }
        assert_eq!(
            encoded::<F>(retained.page.view()),
            before,
            "mutation changed a retained page"
        );
        let bytes = retained.logical_bytes;
        drop(retained);
        retire_allocation(custody, bytes, &mut accounting).unwrap();
        differential::record((index, root));
        output.push((result, canonical_tree::<_, F>(&store, root).await));
        drop(accounting);
        originals.push((original, baseline));
    }
    drop(store);
    for (original, baseline) in originals {
        assert_eq!(
            original.live_allocation_bytes(),
            baseline,
            "store retained original funding after drop"
        );
    }
    output
}

fn paired<F, M>(fixture: impl Fn() -> (Vec<F::Value>, Vec<M>))
where
    F: Format,
    M: persistent_btree::Mutation<F>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for native in [false, true] {
        let [(old, old_events), (new, new_events)] = differential::pair::<_, _, (usize, ObjectId)>(
            &fixture,
            |(initial, operations), baseline| {
                runtime.block_on(async {
                    // The retained owned implementation has its own backend, decoded
                    // pages and mutation state. Only the unchanged canonical wire codec
                    // is common; no cache, decoder instance or journal is shared.
                    if baseline {
                        history::<_, F, M>(
                            MemoryObjectStore::default(),
                            initial,
                            operations,
                            native,
                        )
                        .await
                    } else {
                        history::<_, F, M>(cached(), initial, operations, native).await
                    }
                })
            },
        );
        assert_eq!(new, old);
        assert_eq!(new_events, old_events);
    }
}

fn tree(key: u8, value: u8) -> TreeEntry {
    TreeEntry {
        name: LogicalName::new(NameEncoding::PosixBytes, vec![key + 1; 64], 255).unwrap(),
        file_id: FileId::from_bytes([value; 16]),
        kind: FileKind::Regular,
    }
}
fn record(key: u8, value: u8) -> FileRecord {
    FileRecord {
        file_id: FileId::from_bytes([key; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([value; 32]),
        },
        payload: FilePayload::InlineRegular(InlineFileData::new(&[value; 64]).unwrap()),
    }
}
fn attribute(key: u8, value: u8) -> AttributeEntry {
    AttributeEntry {
        name: AttributeName::new(AttributeClass::PosixXattr, vec![key + 1; 64], 255).unwrap(),
        value_bytes: u64::from(value),
        value: ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([value; 32]),
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn cached_histories_preserve_all_canonical_pages_errors_and_events(
        count in prop_oneof![Just(8_u8), Just(24_u8)],
        operations in prop::collection::vec((0_u8..3, 0_u8..32, 1_u8..4), 0..17),
    ) {
        paired::<TreeFormat, TreeMutation>(|| {
            let mut changes = vec![TreeMutation::Replace { entry: tree(0, 2), expected_file_id: tree(0, 1).file_id }];
            changes.extend(operations.iter().map(|&(op, key, value)| match op {
                0 => TreeMutation::Insert(tree(key, value)),
                1 => TreeMutation::Replace { entry: tree(key, value), expected_file_id: tree(key, 1).file_id },
                _ => TreeMutation::Remove { name: tree(key, 1).name, expected_file_id: None },
            }));
            ((0..count).map(|key| tree(key, 1)).collect(), changes)
        });
        paired::<FileTableFormat, FileTableMutation>(|| {
            let mut changes = vec![FileTableMutation::Replace { expected: record(0, 1), replacement: record(0, 2) }];
            changes.extend(operations.iter().map(|&(op, key, value)| match op {
                0 => FileTableMutation::Insert(record(key, value)),
                1 => FileTableMutation::Replace { expected: record(key, 1), replacement: record(key, value) },
                _ => FileTableMutation::Remove { file_id: record(key, 1).file_id, expected: None },
            }));
            ((0..count).map(|key| record(key, 1)).collect(), changes)
        });
        paired::<AttributeFormat, AttributeMutation>(|| {
            let mut changes = vec![AttributeMutation::Replace { expected: attribute(0, 1), replacement: attribute(0, 2) }];
            changes.extend(operations.iter().map(|&(op, key, value)| match op {
                0 => AttributeMutation::Insert(attribute(key, value)),
                1 => AttributeMutation::Replace { expected: attribute(key, 1), replacement: attribute(key, value) },
                _ => AttributeMutation::Remove { name: attribute(key, 1).name, expected: None },
            }));
            ((0..count).map(|key| attribute(key, 1)).collect(), changes)
        });
    }
}

fn written_identity(omit: bool) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            OMIT.set(false);
        }
    }
    OMIT.set(omit);
    let _reset = Reset;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let store = cached();
            let values: Vec<_> = (0..8).map(|key| tree(key, 1)).collect();
            let root = seed::<_, TreeFormat>(&store, &values).await;
            let cancellation = CancellationToken::new();
            let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            let (old, old_custody) = read_page_with_accounting::<_, TreeFormat>(
                &store,
                root,
                DecodeLimits::default(),
                &mut accounting,
                &cancellation,
                None,
            )
            .await
            .unwrap();
            let result = persistent_btree::apply_with_accounting::<_, TreeFormat, TreeMutation>(
                &store,
                root,
                &[TreeMutation::Replace {
                    entry: tree(3, 2),
                    expected_file_id: tree(3, 1).file_id,
                }],
                1,
                DecodeLimits::default(),
                &mut accounting,
                &cancellation,
            )
            .await
            .unwrap();
            let (new, new_custody) = read_page_with_accounting::<_, TreeFormat>(
                &store,
                result.value,
                DecodeLimits::default(),
                &mut accounting,
                &cancellation,
                None,
            )
            .await
            .unwrap();
            let (
                PageLease::Shared { page: old_page, .. },
                PageLease::Shared { page: new_page, .. },
            ) = (&old.page, &new.page)
            else {
                panic!("expected shared leaves")
            };
            let (
                SharedPage::Leaf {
                    entries: old_entries,
                    ..
                },
                SharedPage::Leaf {
                    entries: new_entries,
                    ..
                },
            ) = (old_page.as_ref(), new_page.as_ref())
            else {
                panic!("expected shared leaves")
            };
            for (index, (old, new)) in old_entries.iter().zip(new_entries).enumerate() {
                assert_eq!(
                    Arc::ptr_eq(old, new),
                    index != 3,
                    "written leaf lost unchanged entry identity"
                );
            }
            let old_bytes = old.logical_bytes;
            let new_bytes = new.logical_bytes;
            drop((old, new));
            retire_allocation(old_custody, old_bytes, &mut accounting).unwrap();
            retire_allocation(new_custody, new_bytes, &mut accounting).unwrap();
        });
}

#[test]
fn authenticated_written_leaf_keeps_unchanged_entry_identity() {
    written_identity(false);
}

#[test]
#[should_panic(expected = "written leaf lost unchanged entry identity")]
fn omitted_written_cache_fails_the_same_identity_oracle() {
    written_identity(true);
}
