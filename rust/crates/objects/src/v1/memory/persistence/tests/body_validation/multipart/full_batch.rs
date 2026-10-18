use super::super::super::super::work::{self, Scan, Visits};
use super::*;

#[derive(Debug, PartialEq, Eq)]
struct BatchWork {
    visits: Visits,
    allocations: AllocationInfo,
}

fn history(prefix: u32, native: bool, scan: Scan) -> Vec<BatchWork> {
    let root = tempfile::tempdir().unwrap();
    let core = core(root.path(), 64);
    let upload = create_upload(&core);
    for number in 1..=prefix {
        part(&core, &upload.upload_id, number);
    }
    let mut samples = Vec::new();
    for turn in 0..4 {
        // Inputs and the result holder are outside measurement, but everything
        // from public batch admission through durable commit is inside it.
        let requests = vec![(
            put(&format!("turn-{turn}")),
            OwnedBytes::from_static(b"frame"),
        )];
        let mut result = None;
        let (allocations, visits) = work::measure(scan, || {
            measure(|| result = Some(batch(&core, requests, native)))
        });
        assert_eq!(result.unwrap(), vec![Ok(5)]);
        samples.push(BatchWork {
            visits,
            allocations,
        });
    }
    drop(core);
    let reopened = core_reopen(root.path());
    for turn in 0..4 {
        assert_eq!(
            futures::executor::block_on(reopened.get(get(bucket(), &format!("turn-{turn}")), 5))
                .unwrap()
                .into_body()
                .as_slice(),
            b"frame",
        );
    }
    // Reverted scans must preserve the exact persisted upload as well as writes.
    let state = reopened.lock_state().unwrap();
    let parts = &state.uploads[upload.upload_id.as_str()].parts;
    assert_eq!(parts.len(), prefix as usize);
    for (number, (receipt, body)) in parts {
        assert_eq!(receipt.part_number, *number);
        assert_eq!(receipt.size, 4);
        assert_eq!(body.len(), 4);
    }
    samples
}

fn same_work(expected: &[BatchWork], actual: &[BatchWork]) -> bool {
    expected == actual
}

#[test]
fn full_batch_encoding_relocation_and_allocations_are_independent_of_multipart_prefix() {
    for native in [false, true] {
        let n = history(16, native, Scan::None);
        let twice = history(32, native, Scan::None);
        eprintln!("full batch native={native}: N={n:?}, 2N={twice:?}");
        assert!(same_work(&n, &twice));
        for sample in &n {
            assert_eq!(
                sample.visits,
                Visits {
                    encoded: 1,
                    relocated: 1
                }
            );
        }
        for scan in [Scan::Encoding, Scan::Relocation] {
            for prefix in [16, 32] {
                let restored = history(prefix, native, scan);
                eprintln!("restored scan prefix={prefix}: {restored:?}");
                assert!(!same_work(&n, &restored));
                for (baseline, old) in n.iter().zip(&restored) {
                    let expected = match scan {
                        Scan::Encoding => Visits {
                            encoded: 1 + prefix as usize,
                            relocated: 1,
                        },
                        Scan::Relocation => Visits {
                            encoded: 1,
                            relocated: 1 + prefix as usize,
                        },
                        Scan::None => unreachable!(),
                    };
                    assert_eq!(old.visits, expected);
                    // Both restored scans must independently trip the heap gate,
                    // even if all visit-count assertions are removed.
                    assert!(old.allocations.count_total > baseline.allocations.count_total);
                    assert!(old.allocations.bytes_total > baseline.allocations.bytes_total);
                }
            }
        }
    }
}
