//! Optional execution-thread allocation totals for real LocalHarness turns.
use std::{future::poll_fn, task::Poll, time::Instant};

pub(super) fn install() -> super::TestResult {
    if std::env::var_os("E2E_PROFILE_TRACE").is_some() {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_writer(std::io::sink)
                .finish(),
        )?;
    }
    Ok(())
}

pub(super) async fn turn<T>(future: impl Future<Output = T>) -> T {
    if std::env::var_os("E2E_ALLOCATIONS").is_none() {
        return future.await;
    }
    let mut future = std::pin::pin!(future);
    let (mut allocations, mut bytes) = (0, 0);
    let started = Instant::now();
    let result = poll_fn(|cx| {
        let mut result = Poll::Pending;
        let measured = acyclic_test_support::measure(|| result = future.as_mut().poll(cx));
        allocations += measured.count_total;
        bytes += measured.bytes_total;
        result
    })
    .await;
    eprintln!(
        "TURN elapsed_ns={} allocations={allocations} bytes={bytes}",
        started.elapsed().as_nanos()
    );
    result
}
