struct ReuseSource<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl AsyncBlobSource for ReuseSource<'_> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        _budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        cancellation
            .check()
            .map_err(|_| std::io::ErrorKind::Interrupted)?;
        acyclic_test_support::differential::record("source-read");
        let count = destination.len().min(self.bytes.len() - self.offset);
        destination[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}

#[test]
fn reuse_matches_independent_staging_for_leaf_and_multilevel_changes() {
    use acyclic_test_support::differential;
    use proptest::prelude::*;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(64));
    runner
        .run(
            &(
                prop::collection::vec(any::<u8>(), 1..65),
                1u32..9,
                2u32..5,
                any::<usize>(),
            ),
            |(bytes, chunk_bytes, width, edit)| {
                let options = BlobBuildOptions {
                    chunk_bytes,
                    page_items: width,
                    page_bytes: 256,
                    maximum_blob_bytes: bytes.len() as u64,
                };
                let limits = DecodeLimits {
                    maximum_page_items: width,
                    maximum_page_bytes: 256,
                    ..DecodeLimits::default()
                };
                let mut changed = bytes.clone();
                if edit % 2 == 0 {
                    let index = edit % changed.len();
                    changed[index] ^= 1;
                }
                let paired = differential::pair::<_, _, &'static str>(
                    || {
                        let store = MemoryObjectStore::default();
                        let cancellation = CancellationToken::new();
                        let prior = runtime
                            .block_on(build_blob(
                                &store,
                                &mut std::io::Cursor::new(&bytes),
                                options,
                                WorkBudget::UNBOUNDED,
                                &cancellation,
                            ))
                            .unwrap();
                        (store, cancellation, prior)
                    },
                    |(store, cancellation, prior), old| {
                        let mut accounting = crate::kernel::KernelAccounting::standalone(
                            WorkBudget {
                                source_bytes_read: bytes.len() as u64,
                                ..WorkBudget::UNBOUNDED
                            },
                            &cancellation,
                        );
                        let mut source = ReuseSource {
                            bytes: &changed,
                            offset: 0,
                        };
                        let built = if old {
                            runtime.block_on(build_blob_with_accounting(
                                &store,
                                &mut source,
                                options,
                                &mut accounting,
                                &cancellation,
                            ))
                        } else {
                            runtime.block_on(reuse_blob_with_accounting(
                                &store,
                                &mut source,
                                options,
                                (prior.root, limits),
                                &mut accounting,
                                &cancellation,
                            ))
                        }
                        .unwrap();
                        let work = accounting.spent();
                        let output = runtime
                            .block_on(read_blob_range(
                                &store,
                                built.root,
                                ByteRange {
                                    offset: 0,
                                    length: bytes.len() as u64,
                                },
                                limits,
                                WorkBudget::UNBOUNDED,
                                &cancellation,
                            ))
                            .unwrap();
                        (
                            built.root,
                            output.bytes.as_ref().to_vec(),
                            source.offset,
                            work.source_bytes_read,
                            work.object_bytes_written,
                        )
                    },
                );
                let [(old, old_trace), (new, trace)] = paired;
                prop_assert_eq!(
                    (&old.0, &old.1, old.2, old.3),
                    (&new.0, &new.1, new.2, new.3)
                );
                prop_assert_eq!(&new.1, &changed);
                prop_assert_eq!(new.2, bytes.len());
                prop_assert_eq!(new.3, bytes.len() as u64);
                prop_assert_eq!(trace, old_trace);
                if bytes == changed {
                    prop_assert_eq!(new.4, 0);
                } else {
                    prop_assert!(new.4 > 0);
                }
                // Restoring the second pass fails the same exact byte oracle.
                prop_assert_ne!(new.3 + bytes.len() as u64, bytes.len() as u64);
                Ok(())
            },
        )
        .unwrap();
}
