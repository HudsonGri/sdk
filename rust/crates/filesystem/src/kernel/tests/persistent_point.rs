use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{Digest, FileId};
use crate::kernel::file_table_mutation::FileTableFormat;
use crate::kernel::persistent_btree::Child;
use crate::kernel::tree_mutation::TreeFormat;
use crate::kernel::{
    FileKind, FilePayload, FileRecord, FileTablePage, LogicalName, NameEncoding, TreeEntry,
    encode_file_table_page, file_table_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectKind;
use acyclic_stream::OwnedBytes;

#[test]
fn owned_string_copy_admits_bytes_before_allocation_and_retires_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    for limit in [0, 6, 7] {
        let mut accounting = KernelAccounting::standalone(
            WorkBudget {
                bytes_copied: limit,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        let result = Owned::<String>::copy("payload", &mut accounting);
        if limit < 7 {
            assert!(result.is_err());
            assert_eq!(accounting.spent().allocation_operations, 0);
            assert_eq!(accounting.spent().bytes_copied, 0);
        } else {
            let owned = result?;
            assert_eq!(owned.value, "payload");
            assert_eq!(accounting.spent().bytes_copied, 7);
            owned.retire(&mut accounting)?;
        }
        assert_eq!(
            accounting
                .standalone_parts()
                .ok_or("missing standalone ledger")?
                .0
                .live_bytes(),
            0
        );
    }
    #[cfg(not(target_arch = "wasm32"))]
    for allocation_limit in [1, 2] {
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: allocation_limit,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let result = Owned::<String>::copy("payload", &mut accounting);
        if allocation_limit == 1 {
            assert!(result.is_err());
        } else {
            result?.retire(&mut accounting)?;
        }
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(original.receipt().allocation_operations <= allocation_limit);
    }
    Ok(())
}

#[test]
fn owned_vector_clear_releases_capacity_and_original_quota()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut values = Owned::<Vec<u8>>::allocate(17, &mut accounting)?;
    values.value.extend_from_slice(b"payload");
    assert!(values.bytes >= 17);
    assert!(
        accounting
            .standalone_parts()
            .ok_or("missing ledger")?
            .0
            .live_bytes()
            >= 17
    );
    values.clear(&mut accounting)?;
    assert!(values.value.is_empty());
    assert_eq!(values.value.capacity(), 0);
    assert_eq!(values.bytes, 0);
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or("missing ledger")?
            .0
            .live_bytes(),
        0
    );
    values.clear(&mut accounting)?;
    values.retire(&mut accounting)?;
    Ok(())
}

#[test]
fn owned_box_admits_before_taking_its_slot_and_retires_the_exact_buffer()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    for budget in [
        WorkBudget {
            allocation_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            bytes_copied: 191,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget::UNBOUNDED,
    ] {
        let mut accounting = KernelAccounting::standalone(budget, &cancellation);
        let mut slot = Some([7_u8; 192]);
        if let Ok(owned) = Owned::take_boxed(&mut slot, &mut accounting) {
            assert_eq!(budget, WorkBudget::UNBOUNDED);
            assert!(slot.is_none());
            assert_eq!(owned.bytes, 192);
            assert_eq!(owned.unbox(&mut accounting)?, [7; 192]);
        } else {
            assert_ne!(budget, WorkBudget::UNBOUNDED);
            assert_eq!(slot, Some([7; 192]));
            assert_eq!(accounting.spent().bytes_copied, 0);
        }
        assert_eq!(
            accounting
                .standalone_parts()
                .ok_or("missing ledger")?
                .0
                .live_bytes(),
            0
        );
    }
    #[cfg(not(target_arch = "wasm32"))]
    for extra in [0, 1] {
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: 1 + extra,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&original, &cancellation);
        let mut slot = Some([7_u8; 192]);
        let result = Owned::take_boxed(&mut slot, &mut accounting);
        if extra == 0 {
            assert!(result.is_err());
            assert!(slot.is_some());
        } else {
            let owned = result?;
            assert!(
                matches!(&owned.allocation, KernelOwnedAllocation::NativeOriginal(allocation)
                if allocation.bytes() == 192 && allocation.work().shares_ledger(&original))
            );
            assert_eq!(original.live_allocation_bytes(), baseline + 192);
            owned.retire(&mut accounting)?;
            assert!(slot.is_none());
        }
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    Ok(())
}

fn record(byte: u8) -> FileRecord {
    FileRecord {
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([byte; 32]),
        },
        payload: FilePayload::Empty,
    }
}

async fn put(
    store: &MemoryObjectStore,
    page: &FileTablePage,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = file_table_page_id(page, 8)?;
    {
        let __object_args = (
            id,
            OwnedBytes::from(encode_file_table_page(
                page,
                8,
                |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
            )?),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result =
            AsyncObjectStore::put(store, __object_args.0, __object_args.1, &mut __object_work)
                .await;
        __object_work.receipt(__object_result)
    }?;
    Ok(id)
}

fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
        255,
    )?)
}

#[test]
fn authenticated_leaf_and_internal_bounds_are_total() -> Result<(), Box<dyn std::error::Error>> {
    let (first, second) = (record(1), record(2));
    assert!(leaf_within::<FileTableFormat>(
        &[first],
        Some(&first.file_id),
        Some(&second.file_id)
    ));
    assert!(!leaf_within::<FileTableFormat>(
        &[first],
        Some(&second.file_id),
        None
    ));
    assert!(!leaf_within::<FileTableFormat>(
        &[first],
        Some(&first.file_id),
        Some(&first.file_id)
    ));
    let child = Child {
        first: first.file_id,
        page: first.metadata,
    };
    assert!(children_within(
        std::slice::from_ref(&child),
        Some(&first.file_id),
        Some(&second.file_id)
    ));
    assert!(!children_within(
        std::slice::from_ref(&child),
        Some(&second.file_id),
        None
    ));
    assert!(!children_within(
        std::slice::from_ref(&child),
        None,
        Some(&first.file_id)
    ));
    let entries = [
        TreeEntry {
            name: name("b")?,
            file_id: first.file_id,
            kind: FileKind::Regular,
        },
        TreeEntry {
            name: name("c")?,
            file_id: second.file_id,
            kind: FileKind::Regular,
        },
    ];
    assert!(leaf_within::<TreeFormat>(
        &entries,
        Some(&name("b")?),
        Some(&name("d")?)
    ));
    assert!(!leaf_within::<TreeFormat>(
        &entries,
        None,
        Some(&name("c")?)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_point_uses_the_original_counters_and_retires_scratch()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(vec![record(1), record(2)])).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (value, allocation) = lookup_async_with_accounting::<_, FileTableFormat>(
        &store,
        root,
        &record(2).file_id,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
        None,
    )
    .await?;
    assert_eq!(value, Some(record(2)));
    assert_eq!(original.receipt().page_reads, 1);
    assert!(original.receipt().backend_read_operations > 0);
    drop(allocation);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_point_denies_scratch_before_provider_work() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let root = put(&store, &FileTablePage::Leaf(vec![record(1)])).await?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = lookup_async_with_accounting::<_, FileTableFormat>(
        &store,
        root,
        &record(1).file_id,
        DecodeLimits::default(),
        &mut accounting,
        &cancellation,
        None,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(original.receipt(), before);
    assert_eq!(original.receipt().backend_read_operations, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_owned_reserve_preserves_the_actual_allocator_error()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = crate::kernel::persistent_point::Owned::<Vec<u8>>::allocate(
        isize::MAX as usize + 1,
        &mut accounting,
    );
    assert!(matches!(
        result,
        Err(crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::AllocationFailed(_)
        ))
    ));
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn owned_push_respects_the_logical_limit_with_spare_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut values =
        crate::kernel::persistent_point::Owned::<Vec<u8>>::allocate(2, &mut accounting)?;
    values.value.push(1);
    assert!(values.ensure_for_push(1, &mut accounting).is_err());
    values.retire(&mut accounting)?;
    Ok(())
}
