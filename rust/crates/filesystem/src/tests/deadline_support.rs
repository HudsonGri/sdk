use acyclic_native_runtime::deadline::simulation::Clock;
use std::{
    fmt::Debug,
    future::Future,
    time::{Duration, Instant},
};

pub(crate) async fn paused_clock_oracle<T: Debug + Eq, F: Future<Output = T>>(
    control: bool,
    expected: T,
    mut run: impl FnMut(Clock) -> F,
) {
    let host = Instant::now();
    for offset in [1_000_000, 100_000] {
        // Virtual time remains paused while the actor runs. A restored host
        // read is already past every original bound, without a slow OS sleep.
        let clock = Clock::at(host - Duration::from_secs(offset));
        if control {
            clock.restore_host_clock_after(1);
        }
        assert_eq!(
            run(clock).await,
            expected,
            "host deadline escaped virtual time"
        );
    }
}
