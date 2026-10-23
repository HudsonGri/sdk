use super::*;

#[tokio::test]
async fn a_thousand_records_retain_live_metadata_without_decoder_scratch() -> Result<()> {
    let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let journal = storage.journal();
    let operation = OperationId::new();
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    for step in 0..1000 {
        journal
            .append(
                operation,
                &format!("failed:{step}"),
                &ExecutionEvent::ToolFailed {
                    step,
                    call_id: "call".into(),
                    reason: ToolFailureKind::Unavailable,
                    cause: ToolFailureCause {
                        class: ToolErrorClass::Invalid,
                        message: "retained diagnostic".into(),
                    },
                },
                None,
            )?
            .await?;
    }
    let executor = lookup_executor()?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    for native in [false, true] {
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(
                acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024),
                &cancellation,
            )?
        };
        check_index_storage(
            &executor,
            journal.as_ref(),
            operation,
            &io,
            native.then_some(&work),
        )
        .await?;
        assert_eq!(
            io.owned_bytes()?,
            0,
            "driver drop releases all index storage"
        );
    }
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

async fn check_index_storage(
    executor: &StockExecutor,
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    io: &PreparationIo,
    work: Option<&acyclic_fs::NativeMaterializationWork>,
) -> Result<()> {
    let live = || work.map_or_else(|| io.owned_bytes(), |work| Ok(work.live_allocation_bytes()));
    let baseline = live()?;
    let execution = StockExecution {
        io: Some(io),
        ..StockExecution::ordinary(executor)
    };
    let result = execution.prefix_records(journal, operation, 999).await;
    assert!(
        result.is_ok(),
        "index retained decoder scratch after a page: {result:?}"
    );
    let records = result?;
    assert_eq!(records.len(), 2);
    let physical = if work.is_some() {
        std::mem::size_of_val(&records) as u64
    } else {
        0
    } + (records.capacity() * std::mem::size_of::<ExecutionRecord>()) as u64
        + records
            .iter()
            .map(|record| {
                record.idempotency_key.capacity() as u64
                    + match &record.event {
                        ExecutionEvent::ToolFailed { call_id, cause, .. } => {
                            (call_id.capacity() + cause.message.capacity()) as u64
                        }
                        _ => 0,
                    }
            })
            .sum::<u64>();
    if work.is_some() {
        assert_eq!(
            io.owned_bytes()?,
            physical,
            "returned records need their full live storage, including diagnostics"
        );
    }
    let output = io.guard_owned(physical)?;
    drop(records);
    drop(output);
    let retained = live()?;
    for _ in 0..10 {
        let records = execution.prefix_records(journal, operation, 999).await?;
        let output = io.guard_owned(physical)?;
        drop(records);
        drop(output);
        assert_eq!(live()?, retained, "empty tail retained scratch");
    }
    eprintln!(
        "1,001-record index retained {} bytes (native: {})",
        retained - baseline,
        work.is_some()
    );
    Ok(())
}

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
