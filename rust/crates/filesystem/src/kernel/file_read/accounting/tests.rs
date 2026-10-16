use super::*;
use crate::foundation::{Digest, FileId, GenerationId, VolumeId};
use crate::kernel::{GenerationRoot, decode_generation_root, encode_generation_root};
use crate::storage::{ObjectId, ObjectKind};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Reserved but unperformed work is returned, including partial completion,
    // cancellation and all observer exit methods. Completed work stays charged.
    #[test]
    fn unused_credit_is_returned_without_losing_completed_work(
        reserved in 0_u64..64,
        completed in 0_u64..64,
        mode in 0_u8..3,
        exit in 0_u8..3,
        cancel in any::<bool>(),
    ) {
        let completed = completed.min(reserved);
        let cancellation = CancellationToken::new();
        let limit = WorkBudget { bytes_copied: reserved, ..WorkBudget::UNBOUNDED };
        let original = crate::NativeMaterializationWork::new(
            if mode == 0 { limit } else { WorkBudget::UNBOUNDED }, &cancellation,
        ).unwrap();
        let work = if mode == 0 { original.clone() } else {
            original.native_volume_work(limit).unwrap()
        };
        let recheck = work.recheck_admitted();
        let work = if mode == 2 { &recheck } else { &work };
        let mut accounting = KernelAccounting::native(work, &cancellation);
        let mut observed = OriginalReadAccounting::new(&mut accounting).unwrap();
        observed.observe(super::super::super::codec::ReadAllocationStep::BeforeCopy(reserved)).unwrap();
        if cancel { cancellation.cancel(); }
        let result = observed.observe(super::super::super::codec::ReadAllocationStep::Copied(completed));
        prop_assert_eq!(result.is_err(), cancel);
        match exit {
            0 => drop(observed),
            1 => drop(observed.into_allocation()),
            _ => drop(observed.into_parts()),
        }
        prop_assert_eq!(original.receipt().bytes_copied, completed);
        // Read remaining through the original work to include outstanding
        // root and resource reservations without creating another owner.
        if !cancel {
            let remaining = work.remaining().unwrap().bytes_copied;
            prop_assert_eq!(remaining, reserved - completed);
            work.charge(WorkCounters { bytes_copied: remaining, ..WorkCounters::UNCHARGED }).unwrap();
            prop_assert_eq!(original.receipt().bytes_copied, reserved);
        }
    }

    // The aggregate parent check is a preflight, followed by individually
    // reserved copies. An exact budget must not reserve the same bytes twice.
    #[test]
    fn generation_preflight_does_not_double_reserve(
        first in any::<[u8; 32]>(),
        parent_count in 0_usize..3,
        resource in any::<bool>(),
    ) {
        let mut second = first;
        second[0] ^= 1;
        let root = GenerationRoot {
            volume_id: VolumeId::from_bytes([1; 16]),
            root_file_id: FileId::from_bytes([2; 16]),
            file_table: ObjectId { kind: ObjectKind::FileTablePage, digest: Digest::from_bytes([3; 32]) },
            parents: [first, second].into_iter().take(parent_count)
                .map(|bytes| GenerationId::new(Digest::from_bytes(bytes))).collect(),
            required_features: 0,
        };
        let bytes = encode_generation_root(&root,
            |_| Ok::<_, super::super::super::CanonicalDecodeError>(()),
            |_| super::super::super::CanonicalDecodeError::AllocationFailed,
        ).unwrap();
        let maximum = (bytes.len() - 8) as u64;
        let limit = WorkBudget { bytes_copied: maximum, ..WorkBudget::UNBOUNDED };
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            if resource { WorkBudget::UNBOUNDED } else { limit }, &cancellation,
        ).unwrap();
        let work = if resource { original.native_volume_work(limit).unwrap() } else { original.clone() };
        let mut accounting = KernelAccounting::native(&work, &cancellation);
        let mut observed = OriginalReadAccounting::new(&mut accounting).unwrap();
        let decoded = decode_generation_root(&bytes, DecodeLimits::default(), |step| observed.observe(step)).unwrap();
        prop_assert_eq!(decoded, root);
        drop(observed.into_allocation());
        prop_assert_eq!(original.receipt().bytes_copied, maximum);
        prop_assert_eq!(work.remaining().unwrap().bytes_copied, 0);
    }
}
