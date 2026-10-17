use super::*;
use crate::foundation::Digest;
use proptest::prelude::*;
use std::cell::Cell;

thread_local! {
    static OLD_INSERTIONS: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn old_insertions() -> bool {
    OLD_INSERTIONS.get()
}

struct Restore(bool);
impl Drop for Restore {
    fn drop(&mut self) {
        OLD_INSERTIONS.set(self.0);
    }
}

pub(in crate::kernel::closure) fn old_path<T>(old: bool, run: impl FnOnce() -> T) -> T {
    let _restore = Restore(OLD_INSERTIONS.replace(old));
    run()
}

fn live_bytes(meter: &ProofMeter<'_>) -> Result<u64, crate::FsError> {
    meter.with_accounting(|accounting| {
        accounting
            .standalone_parts()
            .map(|(allocations, ..)| allocations.live_bytes())
            .ok_or_else(|| ObjectStoreError::Corrupt.into())
    })
}

fn object(value: u64) -> ObjectId {
    let mut digest = [0; 32];
    digest[..8].copy_from_slice(&value.to_le_bytes());
    ObjectId {
        kind: ObjectKind::Metadata,
        digest: Digest::from_bytes(digest),
    }
}

fn build(
    input: &[ObjectId],
    maximum: u64,
    meter: &ProofMeter<'_>,
) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
    let mut objects = ProofObjects::new(meter, maximum);
    for &id in input {
        objects.insert(id)?;
        assert!(objects.contains(id)?);
    }
    objects.into_parts()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn indexed_membership_and_canonical_output_match_an_independent_set(
        input in prop::collection::vec(any::<u8>(), 0..180), maximum in 0u64..150,
    ) {
        let input = input.into_iter().map(|value| object(u64::from(value))).collect::<Vec<_>>();
        let expected = input.iter().copied().collect::<std::collections::BTreeSet<_>>();
        for old in [false, true] {
            let cancel = CancellationToken::new();
            let meter = ProofMeter::standalone(WorkBudget::UNBOUNDED, &cancel)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            let result = old_path(old, || build(&input, maximum, &meter));
            match result {
                Ok((actual, guard)) => {
                    prop_assert!(expected.len() as u64 <= maximum);
                    prop_assert_eq!(actual, expected.iter().copied().collect::<Vec<_>>());
                    drop(guard);
                }
                Err(crate::FsError::Closure(ClosureError::TooManyObjects { observed, maximum: limit })) => {
                    prop_assert!(expected.len() as u64 > maximum);
                    prop_assert_eq!((observed, limit), (maximum + 1, maximum));
                }
                Err(error) => return Err(TestCaseError::fail(error.to_string())),
            }
            prop_assert_eq!(
                live_bytes(&meter).map_err(|error| TestCaseError::fail(error.to_string()))?,
                super::super::memory::proof_standalone_control_bytes(),
            );
        }
    }
}

#[test]
fn canonical_object_inventory_copies_and_allocates_exactly_at_n_and_2n()
-> Result<(), crate::FsError> {
    for (count, copied_units) in [(128u64, 3_055u64), (256, 6_253)] {
        let input = (0..count).rev().map(object).collect::<Vec<_>>();
        for old in [false, true] {
            let cancel = CancellationToken::new();
            let meter = ProofMeter::standalone(WorkBudget::UNBOUNDED, &cancel)?;
            let mut output = None;
            let allocations = acyclic_test_support::measure(|| {
                output = Some(old_path(old, || build(&input, count, &meter)));
            });
            let (actual, guard) = output.ok_or(ObjectStoreError::Corrupt)??;
            assert_eq!(actual, (0..count).map(object).collect::<Vec<_>>());
            let log = u64::from(count.ilog2());
            let size = size_of::<ObjectId>() as u64;
            let copied = meter.work().bytes_copied;
            let expected_old = (count * (count + 1) / 2 + count - 1) * size;
            let expected = copied_units * size;
            assert_eq!(copied, if old { expected_old } else { expected });
            assert_eq!(
                allocations.count_total,
                if old { log + 1 } else { 2 * log - 1 }
            );
            assert_eq!(
                allocations.bytes_total,
                (2 * count - 1) * size
                    + if old {
                        0
                    } else {
                        (4 * count - 16) * size_of::<Option<ObjectId>>() as u64
                    },
            );
            assert_eq!(
                meter.work().allocation_operations,
                allocations.count_total + 1
            );
            // This fault executes the original sorted insertion at the actual
            // walker inventory choke point, with unchanged inputs and oracle.
            assert_eq!(copied == expected, !old);
            assert_eq!(
                std::panic::catch_unwind(|| assert_eq!(copied, expected)).is_ok(),
                !old
            );
            println!("objects={count} old={old} copies={copied} allocations={allocations:?}");
            drop((actual, guard));
        }
    }
    Ok(())
}

#[test]
fn failed_growth_sort_and_cancellation_release_the_index() -> Result<(), crate::FsError> {
    let input = (0..128).rev().map(object).collect::<Vec<_>>();
    for copied in [0, 20_000, 40_000, 100_000] {
        let cancel = CancellationToken::new();
        let meter = ProofMeter::standalone(
            WorkBudget {
                bytes_copied: copied,
                ..WorkBudget::UNBOUNDED
            },
            &cancel,
        )?;
        assert!(build(&input, 128, &meter).is_err());
        assert_eq!(
            live_bytes(&meter)?,
            super::super::memory::proof_standalone_control_bytes(),
        );
    }
    let cancel = CancellationToken::new();
    let meter = ProofMeter::standalone(WorkBudget::UNBOUNDED, &cancel)?;
    let mut objects = ProofObjects::new(&meter, 128);
    for &id in &input[..64] {
        objects.insert(id)?;
    }
    cancel.cancel();
    assert!(objects.insert(input[64]).is_err());
    drop(objects);
    assert_eq!(
        live_bytes(&meter)?,
        super::super::memory::proof_standalone_control_bytes(),
    );
    Ok(())
}

#[test]
fn native_inventory_keeps_original_credit_and_releases_it_after_failure()
-> Result<(), crate::FsError> {
    for (count, copied_units) in [(128u64, 3_055u64), (256, 6_253)] {
        let input = (0..count).rev().map(object).collect::<Vec<_>>();
        for maximum in [0, 50_000, u64::MAX] {
            let cancel = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(
                WorkBudget {
                    bytes_copied: maximum,
                    ..WorkBudget::UNBOUNDED
                },
                &cancel,
            )
            .map_err(|(cause, _)| crate::FsError::from(cause))?;
            let initial = original.live_allocation_bytes();
            let meter = ProofMeter::Native {
                original: &original,
                cancellation: &cancel,
            };
            let result = build(&input, count, &meter);
            assert_eq!(result.is_ok(), maximum == u64::MAX);
            if let Ok((actual, guard)) = result {
                assert_eq!(actual, (0..count).map(object).collect::<Vec<_>>());
                assert_eq!(
                    meter.work().bytes_copied,
                    copied_units * size_of::<ObjectId>() as u64
                );
                drop((actual, guard));
            }
            assert_eq!(original.live_allocation_bytes(), initial);
        }
    }
    Ok(())
}
