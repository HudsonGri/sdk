//! Input counters, independent of the production accounting implementation.
use super::*;
use crate::executor::json::JsonCodecCharge;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

#[derive(Clone, Copy)]
enum Fault {
    None,
    OmitCharge,
    DoubleCharge,
    MutateRejected,
}

fn field(work: &mut WorkCounters, index: usize) -> &mut u64 {
    match index {
        0 => &mut work.bytes_encoded,
        1 => &mut work.source_bytes_read,
        2 => &mut work.items_examined,
        3 => &mut work.bytes_copied,
        4 => &mut work.allocation_operations,
        5 => &mut work.bytes_hashed,
        6 => &mut work.backend_read_operations,
        7 => &mut work.backend_write_operations,
        8 => &mut work.output_bytes,
        9 => &mut work.items_returned,
        _ => unreachable!(),
    }
}

fn history(
    limits: [u64; 5],
    events: &[(u8, u64)],
    cancel_at: Option<usize>,
    native: bool,
    fault: Fault,
) -> TestCaseResult {
    let cancellation = CancellationToken::new();
    let mut budget = acyclic_fs::performance::work_budget(100_000, 16_000_000);
    for (index, limit) in limits.into_iter().enumerate() {
        *field(&mut budget, index) = limit;
    }
    for index in 5..10 {
        *field(&mut budget, index) = limits[(index - 5) % 5];
    }
    // Native construction funds one ledger allocation before codec work.
    budget.allocation_operations += u64::from(native);
    let original =
        native.then(|| acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap());
    let io = match &original {
        Some(original) => PreparationIo::from_native_work(original).unwrap(),
        None => PreparationIo::from_budget(budget, &cancellation).unwrap(),
    };
    let mut expected = io.receipt();
    let mut history = &io;
    let mut observed = Vec::new();
    let mut wanted = Vec::new();
    let mut cancelled = false;
    for (step, &(kind, amount)) in events.iter().enumerate() {
        if cancel_at == Some(step) {
            cancelled = true;
            cancellation.cancel();
        }
        let (charge, encode, index) = match kind {
            0 => (
                JsonCodecCharge::Work {
                    bytes: amount,
                    items: 1,
                },
                true,
                0,
            ),
            1 => (
                JsonCodecCharge::Work {
                    bytes: amount,
                    items: 1,
                },
                false,
                1,
            ),
            2 => (JsonCodecCharge::Copy { bytes: amount }, true, 3),
            3 => (JsonCodecCharge::Read { bytes: amount }, true, 1),
            4 => (JsonCodecCharge::Inspect { items: amount }, true, 2),
            5 => (
                JsonCodecCharge::AllocationOperations { operations: amount },
                true,
                4,
            ),
            6 => (JsonCodecCharge::Inspect { items: 0 }, true, 5),
            7 => (JsonCodecCharge::Inspect { items: 0 }, true, 6),
            8 => (JsonCodecCharge::Inspect { items: 0 }, true, 7),
            9 => (JsonCodecCharge::Inspect { items: 0 }, true, 3),
            _ => unreachable!(),
        };
        let mut next = expected;
        let value = field(&mut next, index);
        let mut accepted = !cancelled;
        let amount = if matches!(kind, 7 | 8) { 1 } else { amount };
        match value.checked_add(amount) {
            Some(sum) => *value = sum,
            None => accepted = false,
        }
        if kind <= 1 {
            match next.items_examined.checked_add(1) {
                Some(sum) => next.items_examined = sum,
                None => accepted = false,
            }
        }
        for index in 0..10 {
            accepted &= *field(&mut next, index) <= *field(&mut budget, index);
        }
        let mut apply = || match kind {
            6 => crate::store::HistoryIo::hashed_bytes(&mut history, amount),
            7 => crate::store::HistoryIo::begin_call(&mut history),
            8 => crate::store::HistoryIo::begin_write(&mut history),
            9 => crate::store::HistoryIo::copy_bytes(&mut history, amount),
            _ => io.codec_charge(charge, encode),
        };
        let result = if matches!(fault, Fault::OmitCharge) {
            Ok(())
        } else {
            apply()
        };
        if matches!(fault, Fault::DoubleCharge) && result.is_ok() {
            let _ = apply();
        }
        if matches!(fault, Fault::MutateRejected) && result.is_err() {
            let _ = io.spent(WorkCounters {
                bytes_copied: 1,
                ..WorkCounters::UNCHARGED
            });
        }
        if accepted {
            expected = next;
        }
        observed.push((result.is_ok(), io.receipt()));
        wanted.push((accepted, expected));
    }
    prop_assert_eq!(
        observed,
        wanted,
        "codec receipts differ from input counters"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn codec_charges_preserve_exact_receipts_and_rejection(
        limits in prop::array::uniform5(0u64..17),
        events in prop::collection::vec((0u8..10, prop_oneof![7 => 0u64..17, 1 => any::<u64>()]), 0..17),
        cancel_at in prop::option::of(0usize..17),
    ) {
        for native in [false, true] {
            // Force every event kind to succeed, then exceed a bound, even if
            // the generated limits are all zero or the history is empty.
            history([8; 5], &[(0,1),(1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(2,9),(0,u64::MAX)], None, native, Fault::None)?;
            history(limits, &events, cancel_at, native, Fault::None)?;
        }
    }
}

#[test]
fn codec_accounting_negative_controls() {
    for native in [false, true] {
        for (fault, limits, event) in [
            (Fault::OmitCharge, [8; 5], (0, 1)),
            (Fault::DoubleCharge, [8; 5], (0, 1)),
            (Fault::MutateRejected, [0; 5], (2, 1)),
            (Fault::OmitCharge, [8; 5], (6, 1)),
            (Fault::DoubleCharge, [8; 5], (7, 1)),
            (Fault::MutateRejected, [0; 5], (9, 1)),
        ] {
            let error = history(limits, &[event], None, native, fault).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("codec receipts differ from input counters"),
                "{error}"
            );
        }
    }
}

fn old_native_buffer(
    custody: &mut acyclic_fs::NativeAllocation,
    charge: JsonCodecCharge,
) -> Result<()> {
    use acyclic_fs::{NativeTreeMaterializationError, WorkError};
    if let JsonCodecCharge::Release { bytes } = charge {
        return custody.release(bytes).map_err(crate::Error::from);
    }
    use crate::store::HistoryIo;
    let work = custody.work();
    work.cancellation()
        .check()
        .map_err(|_| NativeTreeMaterializationError::Cancelled)?;
    match charge {
        JsonCodecCharge::Read { bytes } => work
            .charge(WorkCounters {
                source_bytes_read: bytes,
                ..WorkCounters::default()
            })
            .map_err(crate::Error::from),
        JsonCodecCharge::Admission {
            maximum_owned_bytes,
            ..
        } => {
            custody.resize(maximum_owned_bytes)?;
            Ok(())
        }
        JsonCodecCharge::Retain { bytes } => {
            let retained = custody
                .bytes()
                .checked_add(bytes)
                .ok_or(NativeTreeMaterializationError::Work(WorkError::Overflow))?;
            custody.resize(retained)?;
            Ok(())
        }
        JsonCodecCharge::Release { .. } => unreachable!(),
        charge => crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )
        .codec_charge(charge),
    }
}

#[test]
fn native_buffer_codec_matches_the_former_adapter_at_every_limit() {
    use JsonCodecCharge as Charge;
    let events = [
        Charge::Inspect { items: 1 },
        Charge::Work { bytes: 3, items: 2 },
        Charge::CopyAdmission { bytes: 4 },
        Charge::Copy { bytes: 4 },
        Charge::AllocationOperation { bytes: 8 },
        Charge::AllocationOperations { operations: 3 },
        Charge::Read { bytes: 5 },
        Charge::Retain { bytes: 8 },
        Charge::Release { bytes: 8 },
        Charge::Work {
            bytes: u64::MAX,
            items: u64::MAX,
        },
    ];
    for mode in 0..4 {
        for limit in [0, 3, 128, u64::MAX] {
            for cancel_at in [None, Some(0), Some(4), Some(8)] {
                let run = |old: bool| {
                    let cancellation = CancellationToken::new();
                    let mut budget = acyclic_fs::performance::work_budget(128, 16_000_000);
                    budget.bytes_encoded = limit;
                    budget.source_bytes_read = limit;
                    budget.items_examined = limit;
                    budget.bytes_copied = limit;
                    let work =
                        acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap();
                    let work = if mode & 1 != 0 {
                        work.native_volume_work(budget).unwrap()
                    } else {
                        work
                    };
                    let work = if mode & 2 != 0 {
                        work.invocation_window().unwrap()
                    } else {
                        work
                    };
                    let mut custody = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
                    let mut trace = Vec::new();
                    for (step, event) in events.into_iter().enumerate() {
                        if cancel_at == Some(step) {
                            cancellation.cancel();
                        }
                        let result = if old {
                            old_native_buffer(&mut custody, event)
                        } else {
                            PreparationIo::native_buffer_codec_charge(&mut custody, event)
                        };
                        trace.push((format!("{result:?}"), work.receipt(), custody.bytes()));
                    }
                    trace
                };
                assert_eq!(
                    run(false),
                    run(true),
                    "mode={mode} limit={limit} cancellation={cancel_at:?}"
                );
            }
        }
    }
}
