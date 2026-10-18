use super::*;
use acyclic_fs::{CancellationToken, WorkBudget};

const LINE: usize = 1 << 20;

fn budget(peak: u64, copied: u64) -> WorkBudget {
    WorkBudget {
        bytes_copied: copied,
        allocation_operations: 64,
        peak_allocation_bytes: peak,
        items_examined: 4096,
        ..WorkBudget::default()
    }
}

fn ledger(budget: WorkBudget) -> Result<PreparationIo> {
    PreparationIo::from_budget(budget, &CancellationToken::new())
}

#[test]
fn growth_is_claimed_before_any_allocation_or_copy() -> Result<()> {
    let line = vec![b'a'; LINE];
    // A memory allowance one byte short denies before allocating or copying.
    let short = budget(LINE as u64 - 1, LINE as u64);
    let io = ledger(short)?;
    let mut retained = Retained::default();
    assert!(retained.append(&line, 0, &io).is_err());
    assert_eq!(retained.bytes.capacity(), 0);
    assert_eq!(io.owned_bytes()?, 0);
    let left = io.remaining()?;
    assert_eq!(left.allocation_operations, short.allocation_operations);
    assert_eq!(left.bytes_copied, short.bytes_copied);
    // A copy allowance one byte short also denies before allocating.
    let uncopyable = budget(LINE as u64, LINE as u64 - 1);
    let io = ledger(uncopyable)?;
    assert!(retained.append(&line, 0, &io).is_err());
    assert_eq!(retained.bytes.capacity(), 0);
    assert_eq!(io.owned_bytes()?, 0);
    assert_eq!(
        io.remaining()?.allocation_operations,
        uncopyable.allocation_operations
    );
    // Control: the exact allowance admits the line, claims every byte held
    // and charges one allocation and the copy.
    let exact = budget(LINE as u64, LINE as u64);
    let io = ledger(exact)?;
    retained.append(&line, 0, &io)?;
    assert_eq!(retained.bytes, line);
    assert_eq!(io.owned_bytes()?, retained.bytes.capacity() as u64);
    let left = io.remaining()?;
    assert_eq!(left.allocation_operations, exact.allocation_operations - 1);
    assert_eq!(left.bytes_copied, 0);
    Ok(())
}

#[test]
fn growth_is_amortized_and_never_claims_past_the_file() -> Result<()> {
    let generous = budget(1 << 24, 1 << 24);
    let io = ledger(generous)?;
    let mut retained = Retained::default();
    let chunk = [b'x'; 64];
    let total = 4096 * chunk.len();
    for index in 0..4096 {
        let remaining = (total - (index + 1) * chunk.len()) as u64;
        retained.append(&chunk, remaining, &io)?;
    }
    assert_eq!(retained.bytes.len(), total);
    // Doubling growth: logarithmically many allocations, not one per chunk.
    let allocations = generous.allocation_operations - io.remaining()?.allocation_operations;
    assert!(allocations <= 13, "{allocations} allocations");
    // The file bounds growth: nothing beyond its length is ever claimed.
    assert_eq!(retained.bytes.capacity(), total);
    assert_eq!(io.owned_bytes()?, total as u64);
    Ok(())
}
