use super::*;
use crate::deadline_test_support::paused_clock_oracle_sync;

fn revalidation_oracle(control: bool) {
    // SAFETY: the bridge reports the fixed kernel attribute-cache timeout.
    let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
    let expected = (true, false, Duration::from_secs(timeout));
    paused_clock_oracle_sync(control, expected, |clock| {
        let (_source, context, work) = clock
            .enter(|| checkout_context(MountPublication::Manual))
            .unwrap();
        context.forget_view();
        let (changed, unchanged) = std::thread::spawn(move || {
            let changed = context.revalidate(&work).unwrap();
            let unchanged = context.revalidate(&work).unwrap();
            (changed, unchanged)
        })
        .join()
        .unwrap();
        (
            changed,
            unchanged,
            clock.advances().last().copied().unwrap_or_default(),
        )
    });
}

#[test]
fn revalidation_expires_on_its_captured_clock_after_scope_exit() {
    revalidation_oracle(false);
}

#[test]
#[should_panic(expected = "host deadline escaped virtual time")]
fn revalidation_restored_host_deadline_negative_control() {
    revalidation_oracle(true);
}
