use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use futures::FutureExt as _;
use proptest::prelude::*;

fn delta_bound(delta: usize, copied: u64, allocated: u64, allocations: u64) -> bool {
    copied <= delta as u64 + 1024 && allocated <= 8 * delta as u64 + 32_768 && allocations <= 128
}

async fn publish_deltas(deltas: &[Vec<u8>], rewrite: bool) -> Result<bool> {
    let (_, volume) = memory().await?;
    let query = if rewrite {
        Query::File(FileKey::Goal)
    } else {
        Query::Chunks {
            task: Component::new("t")?,
            turn: 0,
        }
    };
    let snapshot = volume.snapshot(&query, 0).await?;
    let mut cursor = snapshot.cursor;
    let mut follow = volume.follow(query.clone(), cursor.clone()).await?;
    let mut generation = snapshot.generation;
    let mut accumulated = Vec::new();
    for (sequence, delta) in deltas.iter().enumerate() {
        let work = NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &CancellationToken::new(),
        )
        .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
        // Measure the actual payload allocation, packing, provider publication,
        // index update and follower delivery. Historical reads/flattening are
        // included if the implementation starts doing them in this path.
        let mut result = None;
        let allocation = acyclic_test_support::measure(|| {
            result = Some(
                async {
                    let mut file = Write::new(
                        if rewrite {
                            FileKey::Goal
                        } else {
                            FileKey::parse(&format!(".tasks/t/turns/0/chunks/{sequence}"))?
                        },
                        if rewrite {
                            // Negative control: the old accumulated-file strategy.
                            accumulated.extend_from_slice(delta);
                            accumulated.clone()
                        } else {
                            delta.clone()
                        },
                    );
                    if !rewrite {
                        file.channel = Some(if sequence % 2 == 0 {
                            Channel::Text
                        } else {
                            Channel::Reasoning
                        });
                    }
                    generation = volume
                        .append_if(generation, &file, vec![], None, Some(&work))
                        .await?;
                    let change = follow.next().await.ok_or("missing delta")??;
                    assert_eq!(change.file.as_ref(), Some(&file));
                    cursor = change.cursor;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                }
                .now_or_never(),
            );
        });
        result
            .flatten()
            .ok_or("in-memory publication or follow was pending")??;
        let within_bound = delta_bound(
            delta.len(),
            work.receipt().bytes_copied,
            allocation.bytes_total,
            allocation.count_total,
        );
        if !within_bound && !rewrite {
            return Err(format!(
                "delta {sequence}: length={}, copied={}, allocated={}, allocations={}",
                delta.len(),
                work.receipt().bytes_copied,
                allocation.bytes_total,
                allocation.count_total,
            )
            .into());
        }
        if !within_bound && rewrite {
            // The oracle has rejected the control. Continuing to grow its
            // file would test record-size limits instead of delta work.
            return Ok(false);
        }
        // Drop and resume the real changefeed after each delivery. Previously
        // accepted chunks must neither reappear nor change order.
        drop(follow);
        follow = volume.follow(query.clone(), cursor.clone()).await?;
        assert!(follow.next().now_or_never().is_none());
    }
    Ok(true)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn each_chunk_cost_depends_on_its_delta_not_the_retained_prefix(
        deltas in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..2048), 1..48),
    ) {
        let bounded = futures::executor::block_on(publish_deltas(&deltas, false))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert!(bounded);
    }

    #[test]
    fn rewriting_the_prefix_fails_the_same_delta_bound(count in 16usize..32, length in 2048usize..4096) {
        let deltas = vec![vec![b'x'; length]; count];
        prop_assert!(futures::executor::block_on(publish_deltas(&deltas, false))
            .map_err(|error| TestCaseError::fail(error.to_string()))?);
        let bounded = futures::executor::block_on(publish_deltas(&deltas, true))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert!(!bounded);
    }
}
