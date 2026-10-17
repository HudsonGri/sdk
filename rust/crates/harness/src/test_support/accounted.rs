//! Native ledger and custody observations for differential runs.
use crate::filesystem::PreparationIo;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};

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
    let output =
        futures::future::poll_fn(|cx| observe_chunk_poll(|| future.as_mut().poll(cx))).await;
    record_chunk_native(before, io.map(PreparationIo::receipt).unwrap_or_default());
    output
}

pub(crate) fn observe_chunk_sync<R>(run: impl FnOnce() -> R, io: Option<&PreparationIo>) -> R {
    if !TRACE.with_borrow(|trace| trace.is_some()) {
        return run();
    }
    let before = io.map(PreparationIo::receipt).unwrap_or_default();
    let output = observe_chunk_poll(run);
    record_chunk_native(before, io.map(PreparationIo::receipt).unwrap_or_default());
    output
}

fn observe_chunk_poll<R>(run: impl FnOnce() -> R) -> R {
    let capacity = TRACE.with_borrow_mut(|trace| {
        let trace = trace.as_mut().unwrap();
        trace.reserve(64);
        trace.capacity()
    });
    let mut output = None;
    let (_, items) = super::measure_task_volume_work(|| {
        let allocations = acyclic_test_support::measure(|| output = Some(run()));
        assert_eq!(
            TRACE.with_borrow(|trace| trace.as_ref().unwrap().capacity()),
            capacity
        );
        record("chunk.allocations", allocations.count_total);
        record("chunk.allocated-bytes", allocations.bytes_total);
    });
    record("chunk.items", items);
    output.unwrap()
}

fn record_chunk_native(before: WorkCounters, after: WorkCounters) {
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
    record(
        "chunk.native-hash",
        after.bytes_hashed - before.bytes_hashed,
    );
}

thread_local! {
    static CHUNK_COPY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(crate) fn chunk_copy_enabled() -> bool {
    CHUNK_COPY.get()
}
pub(crate) fn with_chunk_copy<R>(enabled: bool, run: impl FnOnce() -> R) -> R {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            CHUNK_COPY.set(self.0);
        }
    }
    let _reset = Reset(CHUNK_COPY.replace(enabled));
    run()
}
