use super::*;
use proptest::prelude::*;
use serde_json::json;

proptest! {
    // Property: simultaneously live capture quotas fit one aggregate byte
    // budget for every command count, without a synchronization primitive.
    #[test]
    fn capture_partitions_bound_aggregate_storage(maximum in 256u64..=65_536, count in 2usize..512) {
        let quota = capture_quota(maximum, count);
        prop_assert!((quota as u64) * count as u64 <= maximum);
        // The former independent per-command limit violates the same detector.
        let broken = maximum.min(65_536);
        prop_assert!(broken * count as u64 > maximum);
    }

    // Property: incremental accounting equals the complete encoded outcome,
    // including escaping, replaced input and first-wins decisions. A rejection
    // occurs exactly when the resulting outcome exceeds the configured bound.
    #[test]
    fn merging_accounts_exact_encoded_bytes(
        steps in prop::collection::vec((".{0,48}", any::<bool>(), any::<bool>(), 0u8..4), 1..12),
        maximum in 256u64..4096,
    ) {
        let mut output = HookOutput::default();
        let mut budget = Budget::new(maximum)?;
        for (text, deny, replace, mode) in steps {
            let value = json!({"decision":if deny { "block" } else { "approve" }, "reason":text,
                "hookSpecificOutput": {"additionalContext":text,
                    "permissionDecision":if mode == 0 { "ask" } else { "allow" },
                    "permissionDecisionReason":text}});
            let mut value = value;
            if replace { value["hookSpecificOutput"]["updatedInput"] = json!({"text":text}); }
            let result = match mode {
                1 => Err(Error::Process { kind:"timeout", exit_code:None, message:text }),
                2 => Ok((Some(3), vec![], text.into_bytes())),
                _ => Ok((Some(0), serde_json::to_vec(&value)?, vec![])),
            };
            let admitted = budget.apply(&mut output, HookEvent::PreToolUse, HookFailure::Block, result);
            let exact = serde_json::to_vec(&output)?.len() as u64;
            prop_assert_eq!(admitted.is_ok(), exact <= maximum);
            if admitted.is_err() { break; }
            prop_assert_eq!(budget.used, exact);
        }
    }
}

#[test]
fn accounting_detector_rejects_a_budget_reset_between_hooks() -> Result<()> {
    fn observed(reset: bool) -> Result<bool> {
        let mut output = HookOutput::default();
        let mut budget = Budget::new(512)?;
        for _ in 0..2 {
            if reset {
                budget = Budget::new(512)?;
            }
            budget.apply(
                &mut output,
                HookEvent::SessionStart,
                HookFailure::Report,
                Ok((Some(0), b"retained context".to_vec(), vec![])),
            )?;
        }
        Ok(budget.used == serde_json::to_vec(&output).unwrap().len() as u64)
    }
    assert!(observed(false)?);
    assert!(
        !observed(true)?,
        "reset budget escaped the same exact-size detector"
    );
    Ok(())
}
