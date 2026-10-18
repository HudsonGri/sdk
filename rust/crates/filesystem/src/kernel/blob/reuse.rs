use super::*;

// Only authenticated index pages are read. Source chunks are hashed once and
// compared to their prior references, without rereading either chunk body.
pub(super) async fn matches_prior<S: AsyncObjectStore>(
    store: &S,
    (mut page_id, limits, length): (ObjectId, DecodeLimits, u64),
    object: ObjectId,
    range: (u64, u64),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let mut bounds = (0, length);
    for _ in 0..=limits.maximum_page_height {
        accounting.check()?;
        if object.kind == ObjectKind::Blob && range == bounds {
            return Ok(object == page_id);
        }
        accounting.charge(WorkCounters {
            page_reads: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let key = DecodedCacheKey::new::<BlobPage>(page_id, limits);
        let (cached, allocated) = if let Some(cached) = store.decoded_cache_get(key)? {
            (cached, 0)
        } else {
            let (read, retained) = read_object_with_accounting(
                store,
                page_id,
                limits.maximum_page_object_bytes(),
                accounting,
                cancellation,
            )
            .await?;
            let decoded = decode_blob_page_with_accounting(&read, limits, accounting);
            drop(read);
            crate::kernel::file_read::release_standalone_read_bytes(retained, accounting)?;
            let decoded = ReadOwned::decoded(decoded?);
            crate::kernel::file_read::cache_decoded_read(store, key, decoded, accounting)?
        };
        let decision: Result<Option<(ObjectId, (u64, u64))>, FsError> = (|| {
            let page = cached
                .value
                .downcast_ref::<BlobPage>()
                .ok_or(FsError::BlobRead(ObjectStoreError::Corrupt.into()))?;
            if (page.first_offset, page.end_offset) != bounds {
                return Err(BlobReadError::ChildBoundsMismatch.into());
            }
            if range.0 < bounds.0 || range.1 > bounds.1 {
                return Ok(None);
            }
            match &page.node {
                BlobNode::Leaf(chunks) => {
                    let (mut low, mut high) = (0, chunks.len());
                    while low < high {
                        accounting.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                        let middle = low + (high - low) / 2;
                        let chunk = chunks[middle];
                        if chunk.first_offset < range.0 {
                            low = middle + 1;
                        } else if chunk.first_offset > range.0 {
                            high = middle;
                        } else {
                            return Ok(Some((chunk.chunk, (chunk.first_offset, chunk.end_offset))));
                        }
                    }
                    Ok(None)
                }
                BlobNode::Internal(children) => {
                    let (mut low, mut high) = (0, children.len());
                    while low < high {
                        accounting.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                        let middle = low + (high - low) / 2;
                        let child = children[middle];
                        if child.end_offset <= range.0 {
                            low = middle + 1;
                        } else if child.first_offset > range.0 {
                            high = middle;
                        } else {
                            return Ok((range.1 <= child.end_offset)
                                .then_some((child.page, (child.first_offset, child.end_offset))));
                        }
                    }
                    Ok(None)
                }
            }
        })();
        drop(cached);
        crate::kernel::file_read::release_standalone_read_bytes(allocated, accounting)?;
        match decision? {
            Some((prior, prior_range)) if prior.kind == ObjectKind::BlobChunk => {
                return Ok(object == prior && range == prior_range);
            }
            Some((prior, prior_range)) => {
                page_id = prior;
                bounds = prior_range;
            }
            None => return Ok(false),
        }
    }
    Err(BlobReadError::CycleOrHeight.into())
}
