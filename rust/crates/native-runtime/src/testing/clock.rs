//! Wall-clock jumps are independent of monotonic timeout advancement.
use super::*;
use std::time::Duration;

/// A provider retains its seed's clock when callbacks move to driver threads.
/// The runtime handle is essential: Tokio's paused clock belongs to the runtime,
/// not to whichever thread happens to invoke the callback.
#[derive(Clone)]
pub struct CapturedClock {
    schedule: Schedule,
    runtime: tokio::runtime::Handle,
    origin: std::time::Instant,
}

impl CapturedClock {
    pub fn capture() -> Option<Self> {
        let context = Context::simulation()?;
        context.schedule.0.state().wall.as_ref()?;
        Some(Self {
            schedule: context.schedule,
            runtime: tokio::runtime::Handle::current(),
            origin: tokio::time::Instant::now().into_std(),
        })
    }

    pub fn unix_millis(&self) -> u64 {
        let _runtime = self.runtime.enter();
        self.schedule
            .0
            .state()
            .wall
            .as_mut()
            .expect("seeded clock")
            .now()
    }

    pub fn instant(&self) -> std::time::Instant {
        let _runtime = self.runtime.enter();
        tokio::time::Instant::now().into_std()
    }

    pub fn elapsed(&self) -> Duration {
        self.instant().duration_since(self.origin)
    }
}

pub(super) struct Wall {
    millis: u64,
    anchor: Option<tokio::time::Instant>,
    #[cfg(test)]
    lose_elapsed: bool,
}

impl Wall {
    pub(super) fn new() -> Self {
        Self {
            millis: 1_700_000_000_000,
            anchor: None,
            #[cfg(test)]
            lose_elapsed: false,
        }
    }

    fn now(&mut self) -> u64 {
        let now = tokio::time::Instant::now();
        #[cfg(test)]
        if self.lose_elapsed {
            self.anchor = Some(now);
        }
        let anchor = self.anchor.get_or_insert(now);
        self.millis.saturating_add(
            u64::try_from(now.duration_since(*anchor).as_millis()).unwrap_or(u64::MAX),
        )
    }
}

/// The active simulation's wall clock, or `None` during ordinary execution.
pub fn unix_millis() -> Option<u64> {
    Context::simulation().and_then(|context| {
        let mut state = context.schedule.0.state();
        state.wall.as_mut().map(Wall::now)
    })
}

impl Schedule {
    /// Changes wall time without changing elapsed time or expiring a timer.
    pub fn set_unix_millis(&self, millis: u64) {
        let mut state = self.0.state();
        let wall = state.wall.as_mut().expect("clock requires a seeded scope");
        wall.millis = millis;
        wall.anchor = Some(tokio::time::Instant::now());
    }

    /// Advances both simulated elapsed time and wall time. The driver must use
    /// Tokio's paused current-thread clock; production timers remain unchanged.
    pub async fn advance(&self, duration: Duration) {
        {
            let mut state = self.0.state();
            // Establish the wall's anchor before any monotonic advancement,
            // including Tokio's own automatic advancement while the driver idles.
            state.wall.as_mut().expect("seeded clock").now();
        }
        tokio::time::advance(duration).await;
    }
}

#[cfg(all(test, not(acyclic_native_runtime_loom)))]
mod tests {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(32))]
        #[test]
        fn wall_jumps_do_not_expire_monotonic_waits(before in 0u64..u64::MAX / 2, after in 0u64..u64::MAX / 2) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
            let (schedule, _) = Schedule::seeded(71);
            runtime.block_on(async {
                tokio::time::pause();
                schedule.set_unix_millis(before);
                schedule.scope(async {
                    assert_eq!(unix_millis(), Some(before));
                    let start = tokio::time::Instant::now();
                    let timer = tokio::time::sleep(Duration::from_millis(10));
                    let mut timer = std::pin::pin!(timer);
                    std::future::poll_fn(|cx| {
                        assert!(timer.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    }).await;
                    schedule.set_unix_millis(after);
                    assert_eq!(unix_millis(), Some(after));
                    assert_eq!(tokio::time::Instant::now(), start);
                    std::future::poll_fn(|cx| {
                        assert!(timer.as_mut().poll(cx).is_pending(), "wall jump expired a timeout");
                        Poll::Ready(())
                    }).await;
                    schedule.advance(Duration::from_millis(100)).await;
                    timer.await;
                    assert_eq!(tokio::time::Instant::now() - start, Duration::from_millis(100));
                    assert_eq!(unix_millis(), Some(after + 100));
                }).await;
                assert_eq!(unix_millis(), None, "clock escaped its scope");
            });
        }
    }

    fn automatic_timeout(lose_elapsed: bool) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (schedule, _) = Schedule::seeded(71);
        schedule.0.state().wall.as_mut().unwrap().lose_elapsed = lose_elapsed;
        runtime.block_on(async {
            tokio::time::pause();
            schedule
                .scope(async {
                    schedule.set_unix_millis(123);
                    let start = tokio::time::Instant::now();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let elapsed = u64::try_from(start.elapsed().as_millis()).unwrap();
                    assert!(elapsed >= 10, "timer fired before its deadline");
                    assert_eq!(unix_millis(), Some(123 + elapsed), "wall clock diverged");
                })
                .await;
        });
    }

    #[test]
    fn automatic_timeout_advancement_cannot_leave_wall_time_behind() {
        automatic_timeout(false);
    }

    #[test]
    #[should_panic(expected = "wall clock diverged")]
    fn discarding_elapsed_time_negative_control() {
        automatic_timeout(true);
    }
}
