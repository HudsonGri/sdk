//! Independent, current-thread differential runs over the real filesystem paths.
use crate::filesystem::PreparationIo;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
use std::cell::RefCell;

thread_local! {
    static TRACE: RefCell<Option<Vec<(&'static str, u64)>>> = const { RefCell::new(None) };
}

pub(crate) fn record(operation: &'static str, amount: u64) {
    TRACE.with_borrow_mut(|trace| {
        if let Some(trace) = trace {
            trace.push((operation, amount));
        }
    });
}

pub(crate) fn pair<S, R>(
    mut initialize: impl FnMut() -> S,
    mut run: impl FnMut(S, bool) -> R,
) -> [(R, Vec<(&'static str, u64)>); 2] {
    [true, false].map(|old| {
        let state = initialize();
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                TRACE.with_borrow_mut(|trace| *trace = None);
            }
        }
        TRACE.with_borrow_mut(|trace| {
            assert!(trace.is_none());
            *trace = Some(Vec::new());
        });
        let _reset = Reset;
        let result = run(state, old);
        (result, TRACE.with_borrow_mut(|trace| trace.take().unwrap()))
    })
}

// Include the original native ledger and live custody on both sides of IO drop;
// copying returned bytes in `run` releases their producer custody before sampling.
pub(crate) fn accounted<R>(
    native: bool,
    maximum: u64,
    cancelled: bool,
    run: impl FnOnce(&PreparationIo) -> R,
) -> Result<(R, WorkCounters, u64, WorkCounters, u64, u64), (String, WorkCounters)> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, maximum);
    let work = if native {
        Some(
            NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, receipt)| (cause.to_string(), *receipt))?,
        )
    } else {
        None
    };
    let io = match &work {
        Some(work) => PreparationIo::from_native_work(work),
        None => PreparationIo::from_budget(budget, &cancellation),
    }
    .unwrap();
    if cancelled {
        cancellation.cancel();
    }
    let result = run(&io);
    let receipt = io.receipt();
    let owned = io.owned_bytes().unwrap();
    let held = work
        .as_ref()
        .map_or(0, NativeMaterializationWork::live_allocation_bytes);
    drop(io);
    Ok((
        result,
        receipt,
        owned,
        work.as_ref()
            .map_or(WorkCounters::default(), NativeMaterializationWork::receipt),
        held,
        work.as_ref()
            .map_or(0, NativeMaterializationWork::live_allocation_bytes),
    ))
}

thread_local! {
    static CHUNK_SCAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn chunk_scan_enabled() -> bool {
    CHUNK_SCAN.get()
}

pub(crate) fn with_chunk_scan<R>(enabled: bool, run: impl FnOnce() -> R) -> R {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            CHUNK_SCAN.set(self.0);
        }
    }
    let _reset = Reset(CHUNK_SCAN.replace(enabled));
    run()
}

// Measure the real projection future, including every poll's SDK work. The
// transcript and canonical admission are intentionally outside this scope.
pub(crate) async fn observe_chunk<F: std::future::Future>(
    future: F,
    io: Option<&PreparationIo>,
) -> F::Output {
    if !TRACE.with_borrow(|trace| trace.is_some()) {
        return future.await;
    }
    let before = io.map(PreparationIo::receipt).unwrap_or_default();
    let mut future = std::pin::pin!(future);
    let output = futures::future::poll_fn(|cx| {
        // Observer storage must never contaminate the allocator measurement.
        let capacity = TRACE.with_borrow_mut(|trace| {
            let trace = trace.as_mut().unwrap();
            trace.reserve(64);
            trace.capacity()
        });
        let mut result = None;
        let (_, items) = super::measure_task_volume_work(|| {
            let allocations =
                acyclic_test_support::measure(|| result = Some(future.as_mut().poll(cx)));
            assert_eq!(
                TRACE.with_borrow(|trace| trace.as_ref().unwrap().capacity()),
                capacity
            );
            record("chunk.allocations", allocations.count_total);
            record("chunk.allocated-bytes", allocations.bytes_total);
        });
        record("chunk.items", items);
        result.unwrap()
    })
    .await;
    let after = io.map(PreparationIo::receipt).unwrap_or_default();
    record(
        "chunk.native-allocations",
        after.allocation_operations - before.allocation_operations,
    );
    record(
        "chunk.native-copy",
        after.bytes_copied - before.bytes_copied,
    );
    record(
        "chunk.native-encoding",
        after.bytes_encoded - before.bytes_encoded,
    );
    output
}
