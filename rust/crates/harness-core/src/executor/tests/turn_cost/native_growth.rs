use super::*;

#[test]
fn relocating_index_storage_requires_peak_capacity_before_allocation() -> Result<()> {
    for fund_peak in [true, false] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let base = acyclic_fs::NativeMaterializationWork::accounting_state_bytes();
        // The final eight u64 slots fit; old plus new storage does not.
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(128, base + 64),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        {
            let io = PreparationIo::from_native_work(&work)?;
            let mut values = Vec::<u64>::new();
            stock_vec_reserve(&mut values, 4, Some(&io))?;
            values.extend(0..4);
            assert_eq!(values.capacity(), 4);
            // Mirror retained cache custody, separate from temporary outputs.
            let retained = io.guard_owned(32)?;
            let result = if fund_peak {
                stock_vec_reserve(&mut values, 8, Some(&io))
            } else {
                // Negative control: the original delta-only reserve admits an
                // allocation whose temporary physical peak exceeds the budget.
                with_history_io(Some(&io), |work| {
                    let work = work
                        .as_deref_mut()
                        .ok_or_else(|| Error::Invalid("missing allocation test ledger".into()))?;
                    crate::store::history::reserve(&mut values, 8, work)
                })
            };
            assert_eq!(result.is_err(), fund_peak);
            assert_eq!(values.capacity(), if fund_peak { 4 } else { 8 });
            let output = io.guard_owned(io.owned_bytes()?)?;
            drop(values);
            drop(output);
            drop(retained);
        }
        assert_eq!(work.live_allocation_bytes(), base);
    }
    Ok(())
}
