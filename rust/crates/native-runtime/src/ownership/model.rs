use super::*;
use crate::sync::Mutex;
use loom::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Cleanup(Arc<AtomicBool>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
        loom::thread::yield_now();
    }
}

fn check(reversed: bool) {
    loom::model(move || {
        // One lock: reopening must wait until the former owner's cleanup ends.
        let fence = Arc::new(Mutex::new(()));
        let exists = Arc::new(AtomicBool::new(true));
        let guard = fence.lock().unwrap();
        let owner = OwnershipAnchor::new(Cleanup(exists.clone()));
        let other_fence = fence.clone();
        let other_exists = exists.clone();
        let opener = loom::thread::spawn(move || {
            let _guard = other_fence.lock().unwrap();
            other_exists.store(true, Ordering::SeqCst);
        });
        if reversed {
            // Former tuple order: a waiting opener can publish before cleanup.
            drop((guard, owner));
        } else {
            // The exact production drop implementation, with the real shim lock.
            drop(FencedOwner {
                _owner: owner,
                _fence: guard,
            });
        }
        opener.join().unwrap();
        assert!(
            exists.load(Ordering::SeqCst),
            "cleanup deleted a reopened store"
        );
    });
}

#[test]
fn cleanup_precedes_reopen_under_every_interleaving() {
    check(false);
}

#[test]
#[should_panic(expected = "cleanup deleted a reopened store")]
fn reversed_release_order_is_detected() {
    check(true);
}
