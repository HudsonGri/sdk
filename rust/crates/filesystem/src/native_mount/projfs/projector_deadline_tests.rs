use super::*;
use acyclic_native_runtime::deadline::simulation::Clock;
use std::time::{Duration, Instant};

#[test]
fn held_placeholder_settlement_expires_on_its_captured_clock_after_scope_exit() {
    let origin = Instant::now();
    let mut traces = Vec::new();
    for skew in [Duration::ZERO, Duration::from_secs(1_000_000)] {
        let clock = Clock::at(origin + skew);
        let projector = clock.enter(Projector::new);
        lock_recover(&projector.state).pending.insert(
            MountPath::root().child(b"held".to_vec()),
            WrittenEntry {
                owned: None,
                facts: None,
                basis: None,
            },
        );
        let projector = std::thread::spawn(move || {
            let error = projector.settle().unwrap_err();
            assert!(matches!(error, NativeMountError::Driver(ref cause)
                if cause.contains("superseded placeholder is held open")));
            assert!(projector.state.try_lock().is_ok());
            projector
        })
        .join()
        .unwrap();
        assert_eq!(lock_recover(&projector.state).pending.len(), 1);
        traces.push(clock.advances());
    }
    assert_eq!(traces[0], [HELD_SETTLE_LIMIT]);
    assert_eq!(traces[0], traces[1]);
}
