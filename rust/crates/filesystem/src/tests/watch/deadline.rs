use super::*;
use acyclic_native_runtime::deadline::simulation::Clock;

#[tokio::test(flavor = "current_thread")]
async fn cookie_fence_timeout_uses_virtual_time_without_holding_the_watch_state() {
    let directory = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let mut watch = NativeWatch::open(
        directory.path(),
        NativeWatchOptions::new(VolumeLimits::default()),
    )
    .unwrap();
    watch.begin_rescan().unwrap();
    watch.finish_rescan().unwrap();
    // No producer watches this path, so the original cookie wait must reach its
    // deadline. Other watcher activity cannot manufacture a cookie receipt.
    let cookie = external.path().join("unobserved-cookie");
    let clock = Clock::new();
    assert!(
        !clock
            .scope(async { watch.await_cookie(&cookie, Duration::from_secs(60)) })
            .await
            .unwrap()
    );
    assert_eq!(clock.advances(), [Duration::from_secs(60)]);
    assert!(!cookie.exists());
    assert!(watch.shared.try_lock().is_ok());
}
