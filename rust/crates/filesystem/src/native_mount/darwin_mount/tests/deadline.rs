use super::*;
use acyclic_native_runtime::deadline::simulation::Clock;

#[test]
fn revalidation_expires_on_its_captured_clock_after_scope_exit() -> TestResult {
    let origin = Instant::now();
    // SAFETY: the bridge reports the fixed kernel attribute-cache timeout.
    let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
    let mut traces = Vec::new();
    for skew in [Duration::ZERO, Duration::from_secs(1_000_000)] {
        let clock = Clock::at(origin + skew);
        let (_source, context, work) =
            clock.enter(|| checkout_context(MountPublication::Manual))?;
        context.forget_view();
        let (changed, unchanged) = std::thread::spawn(move || {
            let changed = context.revalidate(&work).unwrap();
            let unchanged = context.revalidate(&work).unwrap();
            (changed, unchanged)
        })
        .join()
        .unwrap();
        assert!(changed);
        assert!(!unchanged);
        assert_eq!(clock.elapsed(), Duration::from_secs(timeout));
        traces.push(clock.advances());
    }
    assert_eq!(traces[0], traces[1]);
    Ok(())
}
