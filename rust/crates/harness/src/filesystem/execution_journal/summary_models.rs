use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

fn scenario(broken: bool) {
    assert!(
        cfg!(acyclic_fs_loom),
        "model requires the real native ledger locks"
    );
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)
    .unwrap();
    let baseline = work.live_allocation_bytes();
    // The validated prefix is fixed. Both writers settle a distinct admitted
    // tool through the real cache mutex and reducer, on the original ledger.
    let mut initial = ExecutionSummary {
        tail: 1,
        ..ExecutionSummary::default()
    };
    for call in ["a", "b"] {
        initial
            .tools
            .insert((0, *blake3::hash(call.as_bytes()).as_bytes()), false);
    }
    let summary = Arc::new(crate::sync::Mutex::new(initial));
    let _: &acyclic_stream::query_sync::Mutex<ExecutionSummary> = &summary;
    let writers: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|call| {
            let work = work.clone();
            let summary = summary.clone();
            loom::thread::spawn(move || {
                LOSE_NATIVE_CUSTODY.with(|fault| fault.set(broken));
                let io = PreparationIo::from_native_work(&work).unwrap();
                let event = ExecutionEvent::ToolFailed {
                    step: 0,
                    call_id: call.into(),
                    reason: crate::executor::ToolFailureKind::ExecutorRejected,
                };
                loom::future::block_on(async {
                    let mut summary = summary.lock().await;
                    let next = summary.tail + 1;
                    summary
                        .accept_fields((next, call, &event), None, Limits::default(), Some(&io))
                        .unwrap();
                });
                drop(io);
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    loom::future::block_on(async {
        let summary = summary.lock().await;
        assert_eq!(summary.tail, 3);
        assert_eq!(summary.retries.len(), 2);
        assert!(summary.tools.values().all(|settled| *settled));
    });
    assert_eq!(
        work.live_allocation_bytes(),
        baseline + 2 * std::mem::size_of::<([u8; 32], (u64, [u8; 32]))>() as u64,
        "cached summary lost its native custody"
    );
    drop(summary);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[test]
fn concurrent_cached_settlement_retains_original_custody_with_negative_control() {
    loom::model(|| scenario(false));
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true)))
        .expect_err("unfunded cached summary was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("cached summary lost its native custody"),
        "{message}"
    );
}
