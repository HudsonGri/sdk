//! Owned outputs share one ledger through both preparation observer adapters.
use super::*;
use crate::store::history::{HistoryIo, HistoryWork};

#[test]
fn output_custody_outlives_both_observers_and_releases_the_shared_ledger() -> Result<()> {
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    for measured in [false, true] {
        let cancellation = CancellationToken::new();
        let io = PreparationIo::from_budget(budget, &cancellation)?;
        let attempt = io.scoped()?;
        attempt.retain_owned(8)?;
        assert!(attempt.take_owned_custody(9).is_err());
        assert_eq!(attempt.owned_bytes()?, 8);
        let output = if measured {
            let mut observer = HistoryWork::new(attempt);
            HistoryIo::take_owned_custody(&mut observer, 8)?
        } else {
            let output = HistoryIo::take_owned_custody(&mut attempt.history_io(), 8)?;
            assert_eq!(attempt.owned_bytes()?, 0);
            output
        }
        .expect("preparation observers supply owned custody");
        assert_eq!(io.owned_bytes()?, 0);
        assert_eq!(
            io.remaining()?.peak_allocation_bytes,
            budget.peak_allocation_bytes - 8
        );
        cancellation.cancel();
        drop(output);
        // Retirement must still work after cancellation and observer destruction.
        assert_eq!(
            io.remaining()?.peak_allocation_bytes,
            budget.peak_allocation_bytes
        );
    }
    Ok(())
}

#[test]
fn cancelled_custody_transfer_preserves_the_current_owner() -> Result<()> {
    let cancellation = CancellationToken::new();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )?;
    io.retain_owned(8)?;
    cancellation.cancel();
    assert!(io.take_owned_custody(8).is_err());
    assert_eq!(io.owned_bytes()?, 8);
    Ok(())
}
