//! Charged boxes hold their bytes and attempt exactly as long as the box.
use super::*;
use crate::filesystem::PreparationIo;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

type Value = [u64; 16];
const BYTES: u64 = std::mem::size_of::<Value>() as u64;

fn standalone(allocation_operations: u64) -> crate::Result<PreparationIo> {
    let mut budget = acyclic_fs::performance::work_budget(1_000, 4_096);
    budget.allocation_operations = allocation_operations;
    PreparationIo::from_budget(budget, &CancellationToken::new())
}

#[test]
fn standalone_box_holds_its_bytes_and_attempt_until_freed() -> crate::Result<()> {
    let io = standalone(8)?;
    let boxed = ChargedBox::new(Some(&io), [7u64; 16])?;
    assert_eq!(io.owned_bytes()?, BYTES);
    assert_eq!(io.receipt().allocation_operations, 1);
    assert_eq!(io.receipt().peak_allocation_bytes, BYTES);
    assert_eq!(boxed.into_inner(), [7; 16]);
    assert_eq!(io.owned_bytes()?, 0);
    let reserved = ChargedBox::<Value>::reserve(Some(&io))?;
    assert_eq!(io.owned_bytes()?, BYTES);
    drop(reserved);
    assert_eq!(io.owned_bytes()?, 0);
    // A charge may outlive the attempt that made it: it shares the ledger.
    let held = ChargedBox::new(Some(&io), [1u64; 16])?;
    drop(io);
    drop(held);
    Ok(())
}

#[test]
fn refused_box_is_never_made_and_holds_nothing() -> crate::Result<()> {
    let io = standalone(0)?;
    assert!(ChargedBox::new(Some(&io), [7u64; 16]).is_err());
    assert_eq!(io.owned_bytes()?, 0);
    let small = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(1_000, BYTES - 1),
        &CancellationToken::new(),
    )?;
    assert!(ChargedBox::new(Some(&small), [7u64; 16]).is_err());
    assert_eq!(small.owned_bytes()?, 0);
    Ok(())
}

#[test]
fn native_box_holds_its_claim_until_freed() -> crate::Result<()> {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000, 1 << 20),
        &cancellation,
    )
    .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let peak = work.remaining()?.peak_allocation_bytes;
    let attempts = work.remaining()?.allocation_operations;
    let boxed = ChargedBox::new(Some(&io), [7u64; 16])?;
    assert_eq!(work.remaining()?.peak_allocation_bytes, peak - BYTES);
    assert_eq!(work.remaining()?.allocation_operations, attempts - 1);
    drop(boxed);
    assert_eq!(work.remaining()?.peak_allocation_bytes, peak);
    let on_work = BoxCharge::on_native(Some(&work), BYTES)?;
    assert_eq!(work.remaining()?.peak_allocation_bytes, peak - BYTES);
    drop(on_work);
    assert_eq!(work.remaining()?.peak_allocation_bytes, peak);
    Ok(())
}

/// Files on paths that hold an allocation budget where this branch moved
/// values or frames to the heap.
#[test]
fn budget_paths_make_no_unpaid_box() {
    acyclic_test_support::boxes::assert_all_paid(&[
        (
            "attempt.rs",
            include_str!("filesystem/fork_preparer/attempt.rs"),
        ),
        (
            "authentication.rs",
            include_str!("filesystem/fork_preparer/created_access/authentication.rs"),
        ),
        (
            "provider.rs",
            include_str!("filesystem/fork_preparer/created_access/provider.rs"),
        ),
        (
            "journal.rs",
            include_str!("filesystem/fork_preparer/journal.rs"),
        ),
        (
            "preparation.rs",
            include_str!("filesystem/fork_preparer/preparation.rs"),
        ),
        (
            "execution.rs",
            include_str!("filesystem/task_runtime/execution.rs"),
        ),
        (
            "journals.rs",
            include_str!("filesystem/task_runtime/journals.rs"),
        ),
        ("history/mod.rs", include_str!("store/history/mod.rs")),
        ("materialize.rs", include_str!("tools/git/materialize.rs")),
        ("lifecycle.rs", include_str!("durable_host/lifecycle.rs")),
    ]);
}
