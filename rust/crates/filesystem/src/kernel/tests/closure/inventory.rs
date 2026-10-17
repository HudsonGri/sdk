use super::*;
use crate::kernel::closure::objects::tests::old_path;
use std::cell::RefCell;

thread_local! {
    static READS: RefCell<Vec<(ObjectId, Result<Vec<u8>, String>)>> = const { RefCell::new(Vec::new()) };
}

struct Traced(MemoryObjectStore);

impl AsyncObjectStore for Traced {
    async fn put(
        &self,
        id: ObjectId,
        bytes: acyclic_stream::OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.0.put(id, bytes, work).await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, ObjectStoreError> {
        let result = self.0.read(id, maximum, work).await;
        READS.with_borrow_mut(|trace| {
            trace.push((
                id,
                result
                    .as_ref()
                    .map(|read| read.bytes.to_vec())
                    .map_err(ToString::to_string),
            ));
        });
        result
    }

    async fn read_many(
        &self,
        requests: &[crate::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        crate::async_storage::read_many_sequential(self, requests, work).await
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.0.contains(id, work).await
    }
}

async fn fixture(count: usize) -> Result<(Traced, ObjectId), Box<dyn std::error::Error>> {
    // Baseline and candidate construct independent canonical objects and stores.
    let store = MemoryObjectStore::default();
    let mut attributes = Vec::new();
    for index in 0..count {
        let chunk = put(&store, ObjectKind::BlobChunk, vec![u8::try_from(index)?]).await?;
        let blob = put(
            &store,
            ObjectKind::Blob,
            encode_blob_page(
                &BlobPage {
                    first_offset: 0,
                    end_offset: 1,
                    node: BlobNode::Leaf(vec![BlobChunkRef {
                        first_offset: 0,
                        end_offset: 1,
                        chunk,
                    }]),
                },
                8,
                |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
            )?,
        )
        .await?;
        attributes.push(AttributeEntry {
            name: attribute_name(&format!("attr-{index:03}"))?,
            value_bytes: 1,
            value: blob,
        });
    }
    let attributes = put(
        &store,
        ObjectKind::AttributePage,
        encode_attribute_page(
            &AttributePage::Leaf(attributes),
            128,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let metadata = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(
            FileMetadata {
                named_attributes: MetadataField::Value(attributes),
                ..FileMetadata::default()
            },
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &store,
        root,
        vec![directory_record(root, 1, metadata, tree)],
    )
    .await?;
    Ok((Traced(store), generation))
}

#[tokio::test]
async fn independent_old_and_indexed_provers_match_complete_objects_errors_and_read_trace()
-> Result<(), Box<dyn std::error::Error>> {
    for count in [32, 64] {
        for maximum in [0, 4, 32, 1_000] {
            let mut runs = Vec::new();
            for old in [true, false] {
                let (store, root) = fixture(count).await?;
                let limits = ClosureLimits {
                    maximum_objects: maximum,
                    ..limits()
                };
                READS.with_borrow_mut(Vec::clear);
                let cancel = CancellationToken::new();
                // This proof uses immediate in-process storage futures. Its
                // complete execution remains on the current test thread.
                let result = old_path(old, || {
                    futures::executor::block_on(prove_generation_closure(
                        &store,
                        root,
                        limits,
                        WorkBudget::UNBOUNDED,
                        &cancel,
                    ))
                });
                let result = result
                    .map(|proof| {
                        (
                            proof.root,
                            proof.generation_id,
                            proof.file_count,
                            proof.logical_file_bytes,
                            proof.objects,
                        )
                    })
                    .map_err(|failure| failure.error.to_string());
                let trace = READS.with_borrow_mut(std::mem::take);
                runs.push((result, trace));
            }
            assert_eq!(runs[0], runs[1], "count={count} maximum={maximum}");
            assert_eq!(runs[0].0.is_ok(), maximum == 1_000);
        }
    }
    Ok(())
}
